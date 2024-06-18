use crate::bazel_remote_exec::command::EnvironmentVariable;
use crate::bazel_remote_exec::{ActionResult, Digest, ExecutedActionMetadata, OutputFile};
use crate::cache::{BlobDigest, Cache, MessageDigest};
use crate::config::{select_cache_dir, select_sandbox_dir};
use crate::executors::{
    ExecutionResult, ExecutionStatus, Executor, HttpRemoteExecConfig, HttpRemoteExecDomain,
    HttpRemoteExecState, WasiExecutor,
};
use crate::metadata::{write_graphs_html, LogFile, Measurements, Profile, Report, Tag};
use crate::tui::TUI;
use crate::{
    bazel_remote_exec, config, create_cgroup, force_remove_file, is_file_executable,
    write_gitignore, Arena, BoxedSandbox, CGroup, Command, CommandBuilder, CommandId, File, FileId,
    FileType, Scheduler, TmpDirSandbox, WasiSandbox, GITIGNORE_FILENAME,
};
use anyhow::{anyhow, bail, Context};
use itertools::{chain, Itertools};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, fs};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use url::Url;
use which::which;

#[derive(Debug, PartialEq, Eq)]
pub enum ScheduleState {
    New,
    /// Command can not yet be executed because dependencies are still missing
    Waiting,
    /// Command is ready for being executed
    Ready,
    /// Command execution finished successfully
    Succeeded,
    /// Command execution failed
    Failed,
    /// Command could not be started because it depends on a failed condition
    Skipped,
}

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub exec: SchedulerExecStats,
    pub cache_hits: usize,
    pub preparation_duration: Duration,
    pub execution_duration: Duration,
}

#[derive(Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SchedulerExecStats {
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
    pub not_run: usize,
}

impl SchedulerExecStats {
    pub fn finished_successfully(&self) -> bool {
        self.failed == 0 && self.not_run == 0
    }
}

type ExecutionResultChannel = (CommandId, ExecutionResult, Vec<OutputFile>, bool);

pub struct Razel {
    pub read_cache: bool,
    worker_threads: usize,
    /// absolute directory to resolve relative paths of input/output files
    workspace_dir: PathBuf,
    /// current working directory, read-only, used to execute commands
    current_dir: PathBuf,
    /// directory of output files - relative to current_dir
    out_dir: PathBuf,
    cache: Option<Cache>,
    /// directory to use as PWD for executing commands
    ///
    /// Should be but on same device as local cache dir to quickly move outfile file to cache.
    /// Ideally outside the workspace dir to help IDE indexer.
    sandbox_dir: Option<PathBuf>,
    files: Arena<File>,
    /// maps paths relative to current_dir (without out_dir prefix) to <File>s
    path_to_file_id: HashMap<PathBuf, FileId>,
    which_to_file_id: HashMap<String, FileId>,
    /// razel executable - used in Action::input_root_digest for versioning tasks
    self_file_id: Option<FileId>,
    commands: Arena<Command>,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    http_remote_exec_state: HttpRemoteExecState,
    waiting: HashSet<CommandId>,
    scheduler: Scheduler,
    succeeded: Vec<CommandId>,
    failed: Vec<CommandId>,
    skipped: Vec<CommandId>,
    cache_hits: usize,
    tui: TUI,
    tui_dirty: bool,
    measurements: Measurements,
    profile: Profile,
    log_file: LogFile,
}

impl Razel {
    pub fn new() -> Razel {
        let worker_threads = num_cpus::get();
        assert!(worker_threads > 0);
        let current_dir = env::current_dir().unwrap();
        let workspace_dir = current_dir.clone();
        let out_dir = PathBuf::from(config::OUT_DIR);
        Razel {
            read_cache: true,
            worker_threads,
            workspace_dir,
            current_dir,
            out_dir,
            cache: None,
            sandbox_dir: None,
            files: Default::default(),
            path_to_file_id: Default::default(),
            which_to_file_id: Default::default(),
            self_file_id: None,
            commands: Default::default(),
            cgroup: None,
            http_remote_exec_state: Default::default(),
            waiting: Default::default(),
            scheduler: Scheduler::new(worker_threads),
            succeeded: vec![],
            failed: vec![],
            skipped: vec![],
            cache_hits: 0,
            tui: TUI::new(),
            tui_dirty: false,
            measurements: Measurements::new(),
            profile: Profile::new(),
            log_file: Default::default(),
        }
    }

    /// Remove the binary directory
    pub fn clean(&self) {
        fs::remove_dir_all(&self.out_dir).ok();
    }

    /// Set the directory to resolve relative paths of input/output files
    pub fn set_workspace_dir(&mut self, workspace: &Path) -> Result<(), anyhow::Error> {
        if workspace.is_absolute() {
            self.workspace_dir = workspace.into();
        } else {
            self.workspace_dir = self.current_dir.join(workspace);
        }
        Ok(())
    }

    pub fn set_http_remote_exec_config(&mut self, config: &HttpRemoteExecConfig) {
        self.http_remote_exec_state = HttpRemoteExecState::new(config);
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_custom_command(
        &mut self,
        name: String,
        executable: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        inputs: Vec<String>,
        outputs: Vec<String>,
        stdout: Option<String>,
        stderr: Option<String>,
        deps: Vec<String>,
        tags: Vec<Tag>,
    ) -> Result<CommandId, anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args, tags);
        builder.inputs(&inputs, self)?;
        builder.outputs(&outputs, self)?;
        if let Some(x) = stdout {
            builder.stdout(&x, self)?;
        }
        if let Some(x) = stderr {
            builder.stderr(&x, self)?;
        }
        for dep in &deps {
            builder.dep(dep, self)?;
        }
        if executable.ends_with(".wasm") {
            builder.wasi_executor(executable, env, self)?;
        } else {
            builder.custom_command_executor(executable, env, self)?;
        }
        self.push(builder)
    }

    pub fn push(&mut self, builder: CommandBuilder) -> Result<CommandId, anyhow::Error> {
        // TODO check if name is unique
        let id = self.commands.alloc_with_id(|id| builder.build(id));
        let command = &mut self.commands[id];
        Self::check_tags(command)?;
        if !matches!(&command.executor, Executor::CustomCommand(_)) {
            // add razel executable to command hash
            // TODO set digest to razel version once stable
            let self_file_id = self.lazy_self_file_id()?;
            self.commands[id].executables.push(self_file_id);
        }
        // patch outputs.creating_command
        for output_id in &self.commands[id].outputs {
            let output = &mut self.files[*output_id];
            assert!(output.creating_command.is_none());
            output.creating_command = Some(id);
        }
        Ok(id)
    }

    fn check_tags(command: &mut Command) -> Result<(), anyhow::Error> {
        match &command.executor {
            Executor::CustomCommand(_) => {
                if command.tags.contains(&Tag::NoSandbox) && !command.tags.contains(&Tag::NoCache) {
                    // executing a command without sandbox is not reliable, therefore don't cache it
                    command.tags.push(Tag::NoCache);
                }
            }
            Executor::Wasi(_) => {
                if command.tags.contains(&Tag::NoSandbox) {
                    bail!(
                        "Tag is not supported for WASI executor: {}",
                        serde_json::to_string(&Tag::NoSandbox).unwrap()
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn lazy_self_file_id(&mut self) -> Result<FileId, anyhow::Error> {
        if let Some(x) = self.self_file_id {
            Ok(x)
        } else {
            let path = Path::new(&env::args().next().unwrap())
                .canonicalize()
                .ok()
                .filter(|x| x.is_file());
            let file_id = if let Some(x) = &path {
                self.input_file_for_rel_path(
                    config::EXECUTABLE.into(),
                    FileType::RazelExecutable,
                    x.clone(),
                )
                .with_context(|| anyhow!("Failed to find razel executable for {x:?}"))?
                .id
            } else {
                self.executable_which(config::EXECUTABLE.into(), FileType::RazelExecutable)
                    .with_context(|| anyhow!("Failed to find razel executable"))?
                    .id
            };
            self.self_file_id = Some(file_id);
            Ok(file_id)
        }
    }

    #[cfg(test)]
    pub fn get_command(&self, id: CommandId) -> Option<&Command> {
        self.commands.get(id)
    }

    pub fn get_command_by_name(&self, command_name: &String) -> Option<&Command> {
        self.commands.iter().find(|x| &x.name == command_name)
    }

    pub fn add_tag_for_command(&mut self, name: &str, tag: Tag) -> Result<(), anyhow::Error> {
        match self.commands.iter_mut().find(|x| x.name == name) {
            Some(x) => {
                x.tags.push(tag);
                Ok(())
            }
            _ => bail!("Command not found: {name}"),
        }
    }

    pub fn list_commands(&mut self) {
        self.create_dependency_graph();
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            let command = &mut self.commands[id];
            println!("# {}", command.name);
            println!(
                "{}",
                self.tui.format_command_line(
                    &command
                        .executor
                        .command_line_with_redirects(&self.tui.razel_executable)
                )
            );
            command.schedule_state = ScheduleState::Succeeded;
            self.scheduler
                .set_finished_and_get_retry_flag(command, false);
            for rdep_id in command.reverse_deps.clone() {
                let rdep = &mut self.commands[rdep_id];
                assert_eq!(rdep.schedule_state, ScheduleState::Waiting);
                assert!(!rdep.unfinished_deps.is_empty());
                rdep.unfinished_deps
                    .swap_remove(rdep.unfinished_deps.iter().position(|x| *x == id).unwrap());
                if rdep.unfinished_deps.is_empty() {
                    rdep.schedule_state = ScheduleState::Ready;
                    self.waiting.remove(&rdep_id);
                    self.scheduler.push_ready(rdep);
                }
            }
        }
    }

    pub fn show_info(&self, cache_dir: Option<PathBuf>) -> Result<(), anyhow::Error> {
        let output_directory = self.current_dir.join(&self.out_dir);
        println!("workspace dir:     {:?}", self.workspace_dir);
        println!("output directory:  {:?}", output_directory);
        let cache_dir = match cache_dir {
            Some(x) => x,
            _ => select_cache_dir(&self.workspace_dir)?,
        };
        println!("cache directory:   {:?}", cache_dir);
        println!("sandbox directory: {:?}", select_sandbox_dir(&cache_dir)?);
        println!("worker threads:    {}", self.worker_threads);
        Ok(())
    }

    async fn prepare_run(
        &mut self,
        cache_dir: Option<PathBuf>,
        remote_cache: Vec<String>,
        remote_cache_threshold: Option<u32>,
    ) -> Result<(), anyhow::Error> {
        let output_directory = self.current_dir.join(&self.out_dir);
        debug!("workspace dir:     {:?}", self.workspace_dir);
        debug!("output directory:  {:?}", output_directory);
        let cache_dir = match cache_dir {
            Some(x) => x,
            _ => select_cache_dir(&self.workspace_dir)?,
        };
        debug!("cache directory:   {:?}", cache_dir);
        let sandbox_dir = select_sandbox_dir(&cache_dir)?;
        let mut cache = Cache::new(cache_dir, self.out_dir.clone())?;
        debug!("sandbox directory: {:?}", sandbox_dir);
        debug!("worker threads:    {}", self.worker_threads);
        if !remote_cache.is_empty() {
            cache
                .connect_remote_cache(&remote_cache, remote_cache_threshold)
                .await?;
        }
        TmpDirSandbox::cleanup(&sandbox_dir);
        self.cache = Some(cache);
        self.sandbox_dir = Some(sandbox_dir);
        match create_cgroup() {
            Ok(x) => self.cgroup = x,
            Err(e) => debug!("create_cgroup(): {e}"),
        };
        self.create_dependency_graph();
        self.remove_unknown_files_from_out_dir(&self.out_dir).ok();
        self.digest_input_files().await?;
        self.create_output_dirs()?;
        self.create_wasi_modules()?;
        Ok(())
    }

    pub async fn run(
        &mut self,
        keep_going: bool,
        verbose: bool,
        group_by_tag: &str,
        cache_dir: Option<PathBuf>,
        remote_cache: Vec<String>,
        remote_cache_threshold: Option<u32>,
    ) -> Result<SchedulerStats, anyhow::Error> {
        let preparation_start = Instant::now();
        if self.commands.is_empty() {
            bail!("No commands added");
        }
        self.tui.verbose = verbose;
        self.prepare_run(cache_dir, remote_cache, remote_cache_threshold)
            .await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut interval = tokio::time::interval(self.tui.get_update_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let execution_start = Instant::now();
        self.start_ready_commands(&tx);
        let mut start_more_commands = true;
        while self.scheduler.running() != 0 {
            tokio::select! {
                Some((id, execution_result, output_files, output_files_cached)) = rx.recv() => {
                    self.on_command_finished(id, &execution_result, output_files, output_files_cached);
                    if execution_result.status == ExecutionStatus::SystemError
                        || (!self.failed.is_empty() && !keep_going)
                    {
                        start_more_commands = false;
                    }
                    if start_more_commands {
                        self.start_ready_commands(&tx);
                    }
                },
                _ = interval.tick() => self.update_status(),
            }
        }
        self.remove_outputs_of_not_run_actions_from_out_dir();
        TmpDirSandbox::cleanup(self.sandbox_dir.as_ref().unwrap());
        self.push_logs_for_not_started_commands();
        self.write_metadata(group_by_tag)
            .context("Failed to write metadata")?;
        let stats = SchedulerStats {
            exec: SchedulerExecStats {
                succeeded: self.succeeded.len(),
                failed: self.failed.len(),
                skipped: self.skipped.len(),
                not_run: self.waiting.len() + self.scheduler.ready(),
            },
            cache_hits: self.cache_hits,
            preparation_duration: execution_start.duration_since(preparation_start),
            execution_duration: execution_start.elapsed(),
        };
        self.tui.finished(&stats);
        Ok(stats)
    }

    pub(crate) fn get_file_path(&self, id: FileId) -> &PathBuf {
        &self.files[id].path
    }

    /// Register an executable file
    pub fn executable(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        let path = Path::new(&arg);
        if path.is_relative() {
            let abs = self.workspace_dir.join(path);
            let cwd_path = abs.strip_prefix(&self.current_dir).unwrap().to_path_buf();
            if let Some(id) = self.path_to_file_id.get(&cwd_path) {
                return Ok(&self.files[*id]);
            }
        }
        let (file_type, abs_path) = FileType::from_executable_arg(&arg, &self.workspace_dir)?;
        match file_type {
            FileType::ExecutableInWorkspace
            | FileType::ExecutableOutsideWorkspace
            | FileType::WasiModule => {
                let cwd_path = self.rel_path(&abs_path.unwrap().to_str().unwrap().into())?;
                self.input_file_for_rel_path(arg, file_type, cwd_path)
            }
            FileType::SystemExecutable => self.executable_which(arg, file_type),
            FileType::RazelExecutable => self.lazy_self_file_id().map(|x| &self.files[x]),
            _ => unreachable!(),
        }
    }

    fn executable_which(
        &mut self,
        arg: String,
        file_type: FileType,
    ) -> Result<&File, anyhow::Error> {
        if let Some(x) = self.which_to_file_id.get(&arg) {
            Ok(&self.files[*x])
        } else {
            let path =
                which(&arg).with_context(|| format!("executable not found: {:?}", arg.clone()))?;
            debug!("which({}) => {:?}", arg, path);
            let id = self
                .input_file_for_rel_path(arg.clone(), file_type, path.to_str().unwrap().into())?
                .id;
            self.which_to_file_id.insert(arg, id);
            Ok(&self.files[id])
        }
    }

    pub fn input_file(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        let rel_path = self.rel_path(&arg)?;
        self.input_file_for_rel_path(arg, FileType::DataFile, rel_path)
    }

    fn input_file_for_rel_path(
        &mut self,
        arg: String,
        file_type: FileType,
        rel_path: PathBuf,
    ) -> Result<&File, anyhow::Error> {
        let id = self
            .path_to_file_id
            .get(&rel_path)
            .cloned()
            .unwrap_or_else(|| {
                let id = self
                    .files
                    .alloc_with_id(|id| File::new(id, arg, file_type, rel_path.clone()));
                self.path_to_file_id.insert(rel_path, id);
                id
            });
        Ok(&self.files[id])
    }

    pub fn output_file(
        &mut self,
        arg: &String,
        file_type: FileType,
    ) -> Result<&File, anyhow::Error> {
        let rel_path = self.rel_path(arg)?;
        if let Some(file) = self.path_to_file_id.get(&rel_path).map(|x| &self.files[*x]) {
            if let Some(creating_command) = file.creating_command {
                bail!(
                    "File {} cannot be output of multiple commands, already output of {}",
                    arg,
                    self.commands[creating_command].name
                );
            } else {
                bail!(
                    "File {} cannot be output because it's already used as data",
                    arg,
                );
            }
        }
        let id = self.files.alloc_with_id(|id| {
            File::new(id, arg.clone(), file_type, self.out_dir.join(&rel_path))
        });
        self.path_to_file_id.insert(rel_path, id);
        Ok(&self.files[id])
    }

    pub fn http_remote_exec(&self, url: &Url) -> Option<Arc<HttpRemoteExecDomain>> {
        self.http_remote_exec_state.for_url(url)
    }

    pub fn wasi_module(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        let rel_path = self.rel_path(&arg)?;
        self.input_file_for_rel_path(arg, FileType::WasiModule, rel_path)
    }

    /// Maps a relative path from workspace dir to cwd, allow absolute path
    fn rel_path(&self, arg: &String) -> Result<PathBuf, anyhow::Error> {
        let path = Path::new(arg);
        if path.is_absolute() {
            Ok(PathBuf::from(
                path.strip_prefix(&self.current_dir).unwrap_or(path),
            ))
        } else {
            self.workspace_dir
                .join(path)
                .strip_prefix(&self.current_dir)
                .map(PathBuf::from)
                .with_context(|| {
                    format!(
                        "File is not within cwd ({:?}): {:?}",
                        self.current_dir, path
                    )
                })
        }
    }

    fn create_dependency_graph(&mut self) {
        self.waiting.reserve(self.commands.len());
        self.succeeded.reserve(self.commands.len());
        let mut rdeps = vec![];
        for command in self.commands.iter_mut() {
            assert_eq!(command.schedule_state, ScheduleState::New);
            command.unfinished_deps.reserve(command.deps.len());
            for input_id in chain(command.executables.iter(), command.inputs.iter()) {
                if let Some(dep) = self.files[*input_id].creating_command {
                    command.unfinished_deps.push(dep);
                    rdeps.push((dep, command.id));
                }
            }
            for dep in &command.deps {
                command.unfinished_deps.push(*dep);
                rdeps.push((*dep, command.id));
            }
            if command.unfinished_deps.is_empty() {
                command.schedule_state = ScheduleState::Ready;
                self.scheduler.push_ready(command);
            } else {
                command.schedule_state = ScheduleState::Waiting;
                self.waiting.insert(command.id);
            }
        }
        for (id, rdep) in rdeps {
            self.commands[id].reverse_deps.push(rdep);
        }
        self.check_for_circular_dependencies();
        assert_ne!(!self.scheduler.len(), 0);
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
    }

    fn remove_unknown_files_from_out_dir(&self, dir: &Path) -> Result<(), anyhow::Error> {
        for entry in fs::read_dir(dir)? {
            if let Ok(path) = entry.map(|x| x.path()) {
                if path.is_dir() {
                    // TODO remove whole dir if not known
                    self.remove_unknown_files_from_out_dir(&path).ok();
                } else {
                    let path_wo_prefix = path.strip_prefix(&self.out_dir).unwrap();
                    if self
                        .path_to_file_id
                        .get(path_wo_prefix)
                        .filter(|x| self.files[**x].path == path)
                        .is_none()
                        && path_wo_prefix.to_string_lossy() != GITIGNORE_FILENAME
                    {
                        fs::remove_file(path).ok();
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_outputs_of_not_run_actions_from_out_dir(&self) {
        for command_id in self.waiting.iter().chain(self.scheduler.ready_ids().iter()) {
            for file_id in &self.commands[*command_id].outputs {
                fs::remove_file(&self.files[*file_id].path).ok();
            }
        }
    }

    async fn digest_input_files(&mut self) -> Result<(), anyhow::Error> {
        let concurrent = self.worker_threads;
        let (tx, mut rx) = mpsc::channel(concurrent);
        let mut tx_option = Some(tx);
        let mut next_file_id = self.files.first_id();
        for _ in 0..concurrent {
            self.spawn_digest_input_file(&mut next_file_id, &mut tx_option);
        }
        let mut missing_files = 0;
        while let Some((id, result)) = rx.recv().await {
            match result {
                Ok(digest) => {
                    self.files[id].digest = Some(digest);
                }
                Err(x) => {
                    warn!("{}", x);
                    missing_files += 1;
                }
            };
            self.spawn_digest_input_file(&mut next_file_id, &mut tx_option);
        }
        if missing_files != 0 {
            bail!("{missing_files} input files not found!");
        }
        Ok(())
    }

    fn spawn_digest_input_file(
        &self,
        next_id: &mut FileId,
        tx_option: &mut Option<Sender<(FileId, Result<BlobDigest, anyhow::Error>)>>,
    ) {
        if tx_option.is_none() {
            return;
        }
        while let Some(file) = self.files.get_and_inc_id(next_id) {
            if file.creating_command.is_none() {
                let id = file.id;
                let path = file.path.clone();
                let tx = tx_option.clone().unwrap();
                tokio::spawn(async move {
                    tx.send((id, Digest::for_path(path).await)).await.ok();
                });
                return;
            }
        }
        tx_option.take();
    }

    fn create_output_dirs(&self) -> Result<(), anyhow::Error> {
        let dirs = self
            .files
            .iter()
            .map(|x| x.path.parent().unwrap())
            .sorted_unstable()
            .dedup();
        for x in dirs {
            fs::create_dir_all(x)
                .with_context(|| format!("Failed to create output directory: {x:?}"))?;
        }
        write_gitignore(&self.out_dir);
        Ok(())
    }

    fn create_wasi_modules(&mut self) -> Result<(), anyhow::Error> {
        let mut engine = None;
        let mut modules: HashMap<FileId, wasmtime::Module> = HashMap::new();
        for command in self.commands.iter_mut() {
            if let Executor::Wasi(executor) = &mut command.executor {
                let file_id = executor.module_file_id.unwrap();
                let module = match modules.entry(file_id) {
                    hash_map::Entry::Occupied(x) => x.get().clone(),
                    hash_map::Entry::Vacant(x) => {
                        if engine.is_none() {
                            engine = Some(WasiExecutor::create_engine()?);
                        }
                        let module = WasiExecutor::create_module(
                            engine.as_ref().unwrap(),
                            &executor.executable,
                        )?;
                        x.insert(module.clone());
                        module
                    }
                };
                executor.module = Some(module);
            }
        }
        Ok(())
    }

    fn start_ready_commands(&mut self, tx: &UnboundedSender<ExecutionResultChannel>) {
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            self.start_next_command(id, tx.clone());
            self.tui_dirty = true;
        }
    }

    fn update_status(&mut self) {
        if !self.tui_dirty {
            return;
        }
        self.tui.status(
            self.succeeded.len(),
            self.cache_hits,
            self.failed.len(),
            self.scheduler.running(),
            self.waiting.len() + self.scheduler.ready(),
        );
        self.tui_dirty = false;
    }

    fn new_sandbox(&self, command: &Command) -> BoxedSandbox {
        match command.executor {
            Executor::Wasi(_) => self.new_wasi_sandbox(command),
            _ => self.new_tmp_dir_sandbox(command),
        }
    }

    fn new_tmp_dir_sandbox(&self, command: &Command) -> BoxedSandbox {
        let command_executables = command.executables.iter().filter(|&&x| {
            if let Some(self_file_id) = self.self_file_id {
                // razel never calls itself
                x != self_file_id
            } else {
                true
            }
        });
        let inputs = chain(command_executables, command.inputs.iter())
            .map(|x| self.files[*x].path.clone())
            .filter(|x| x.is_relative())
            .collect();
        Box::new(TmpDirSandbox::new(
            self.sandbox_dir.as_ref().unwrap(),
            &command.id.to_string(),
            inputs,
        ))
    }

    fn new_wasi_sandbox(&self, command: &Command) -> BoxedSandbox {
        let cache = self.cache.as_ref().unwrap();
        let inputs = command
            .inputs
            .iter()
            .map(|x| &self.files[*x])
            .filter(|x| x.file_type == FileType::OutputFile)
            .map(|x| {
                (
                    x.path.clone(),
                    x.locally_cached
                        .then_some(cache.cas_path(x.digest.as_ref().unwrap())),
                )
            })
            .collect();
        Box::new(WasiSandbox::new(
            self.sandbox_dir.as_ref().unwrap(),
            &command.id.to_string(),
            inputs,
        ))
    }

    fn collect_output_file_paths_for_command(&self, command: &Command) -> Vec<PathBuf> {
        command
            .outputs
            .iter()
            .map(|x| self.files[*x].path.clone())
            .collect()
    }

    /// Execute a command in a worker thread with caching.
    ///
    /// If the executed command failed, action_result will be None and the action will not be cached.
    fn start_next_command(&mut self, id: CommandId, tx: UnboundedSender<ExecutionResultChannel>) {
        let total_duration_start = Instant::now();
        let command = &self.commands[id];
        assert_eq!(command.schedule_state, ScheduleState::Ready);
        assert_eq!(command.unfinished_deps.len(), 0);
        let (bzl_command, bzl_input_root) = self.get_bzl_action_for_command(command);
        let no_cache_tag = command.tags.contains(&Tag::NoCache);
        let no_sandbox_tag = command.tags.contains(&Tag::NoSandbox);
        let cache = (!no_cache_tag).then(|| self.cache.as_ref().unwrap().clone());
        let read_cache = self.read_cache && !no_sandbox_tag;
        let use_remote_cache = cache.is_some() && !command.tags.contains(&Tag::NoRemoteCache);
        let executor = command.executor.clone();
        let sandbox =
            (!no_sandbox_tag && executor.use_sandbox()).then(|| self.new_sandbox(command));
        let output_paths = self.collect_output_file_paths_for_command(command);
        let cgroup = self.cgroup.clone();
        let cwd = self.current_dir.clone();
        let out_dir = self.out_dir.clone();
        tokio::task::spawn(async move {
            let use_cache = cache.is_some();
            let action = bazel_remote_exec::Action {
                command_digest: Some(Digest::for_message(&bzl_command)),
                input_root_digest: Some(Digest::for_message(&bzl_input_root)),
                ..Default::default()
            };
            let action_digest = Digest::for_message(&action);
            let (mut execution_result, output_files) = Self::exec_action(
                &action_digest,
                cache,
                read_cache,
                use_remote_cache,
                &executor,
                &output_paths,
                sandbox,
                cgroup,
                &cwd,
                &out_dir,
            )
            .await
            .unwrap_or_else(|e| {
                (
                    ExecutionResult {
                        status: ExecutionStatus::SystemError,
                        error: Some(e),
                        ..Default::default()
                    },
                    Default::default(),
                )
            });
            execution_result.total_duration = Some(total_duration_start.elapsed());
            let output_files_cached = use_cache && execution_result.success();
            // ignore SendError - channel might be closed if a previous command failed
            tx.send((id, execution_result, output_files, output_files_cached))
                .ok();
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_action(
        action_digest: &MessageDigest,
        mut cache: Option<Cache>,
        read_cache: bool,
        use_remote_cache: bool,
        executor: &Executor,
        output_paths: &Vec<PathBuf>,
        sandbox: Option<BoxedSandbox>,
        cgroup: Option<CGroup>,
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>), anyhow::Error> {
        let (execution_result, output_files) = if let Some(x) =
            Self::get_action_from_cache(action_digest, cache.as_mut(), read_cache, use_remote_cache)
                .await
        {
            x
        } else if let Some(sandbox) = sandbox {
            Self::exec_action_with_sandbox(
                action_digest,
                cache.as_mut(),
                use_remote_cache,
                executor,
                sandbox,
                output_paths,
                cgroup,
                cwd,
                out_dir,
            )
            .await
            .context("exec_action_with_sandbox()")?
        } else {
            Self::exec_action_without_sandbox(
                action_digest,
                cache.as_mut(),
                use_remote_cache,
                executor,
                output_paths,
                cgroup,
                cwd,
                out_dir,
            )
            .await
            .context("exec_action_without_sandbox()")?
        };
        if let Some(cache) = cache.as_ref().filter(|_| execution_result.success()) {
            cache
                .link_output_files_into_out_dir(&output_files)
                .await
                .context("symlink_output_files_into_out_dir()")?;
        }
        Ok((execution_result, output_files))
    }

    async fn get_action_from_cache(
        action_digest: &MessageDigest,
        cache: Option<&mut Cache>,
        read_cache: bool,
        use_remote_cache: bool,
    ) -> Option<(ExecutionResult, Vec<OutputFile>)> {
        let cache = cache.filter(|_| read_cache)?;
        if let Some((action_result, cache_hit)) = cache
            .get_action_result(action_digest, use_remote_cache)
            .await
        {
            let exit_code = Some(action_result.exit_code);
            let metadata = action_result.execution_metadata.as_ref();
            let execution_result = ExecutionResult {
                status: ExecutionStatus::Success,
                exit_code,
                error: None,
                cache_hit: Some(cache_hit),
                stdout: action_result.stdout_raw,
                stderr: action_result.stderr_raw,
                exec_duration: metadata
                    .and_then(|x| x.virtual_execution_duration.as_ref())
                    .map(|x| Duration::new(x.seconds as u64, x.nanos as u32)),
                total_duration: None,
            };
            return Some((execution_result, action_result.output_files));
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_action_with_sandbox(
        action_digest: &MessageDigest,
        cache: Option<&mut Cache>,
        use_remote_cache: bool,
        executor: &Executor,
        sandbox: BoxedSandbox,
        output_paths: &Vec<PathBuf>,
        cgroup: Option<CGroup>,
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>), anyhow::Error> {
        sandbox
            .create(output_paths)
            .await
            .context("Sandbox::create()")?;
        let execution_result = executor
            .exec(cwd, Some(sandbox.dir().clone()), cgroup)
            .await;
        let output_files = if execution_result.success() {
            Self::new_output_files_with_digest(Some(sandbox.dir()), out_dir, output_paths).await?
        } else {
            Default::default()
        };
        if execution_result.success() {
            if let Some(cache) = cache {
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_files.clone(),
                    Some(sandbox.dir()),
                    cache,
                    use_remote_cache,
                )
                .await
                .with_context(|| "cache_action_result()")?;
            } else {
                sandbox.move_output_files_into_out_dir(output_paths).await?;
            }
        }
        sandbox
            .destroy()
            .await
            .with_context(|| "Sandbox::destroy()")?;
        Ok((execution_result, output_files))
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_action_without_sandbox(
        action_digest: &MessageDigest,
        cache: Option<&mut Cache>,
        use_remote_cache: bool,
        executor: &Executor,
        output_paths: &Vec<PathBuf>,
        cgroup: Option<CGroup>,
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>), anyhow::Error> {
        // remove expected output files, because symlinks will not be overwritten
        for x in output_paths {
            force_remove_file(x).await?;
        }
        let execution_result = executor.exec(cwd, None, cgroup).await;
        let output_files = if execution_result.success() {
            Self::new_output_files_with_digest(None, out_dir, output_paths).await?
        } else {
            Default::default()
        };
        if execution_result.success() {
            if let Some(cache) = cache {
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_files.clone(),
                    None,
                    cache,
                    use_remote_cache,
                )
                .await
                .with_context(|| "cache_action_result()")?;
            }
        }
        Ok((execution_result, output_files))
    }

    async fn new_output_files_with_digest(
        sandbox_dir: Option<&PathBuf>,
        out_dir: &PathBuf,
        output_paths: &Vec<PathBuf>,
    ) -> Result<Vec<OutputFile>, anyhow::Error> {
        let mut output_files: Vec<OutputFile> = Vec::with_capacity(output_paths.len());
        for path in output_paths {
            let output_file = Self::new_output_file_with_digest(sandbox_dir, out_dir, path)
                .await
                .context("Handle expected output file")?;
            output_files.push(output_file);
        }
        Ok(output_files)
    }

    async fn new_output_file_with_digest(
        sandbox_dir: Option<&PathBuf>,
        out_dir: &PathBuf,
        exec_path: &PathBuf,
    ) -> Result<OutputFile, anyhow::Error> {
        let src = sandbox_dir
            .as_ref()
            .map_or(exec_path.clone(), |x| x.join(exec_path));
        if src.is_symlink() {
            bail!("Output file must not be a symlink: {:?}", src);
        }
        let file = tokio::fs::File::open(&src)
            .await
            .with_context(|| format!("Failed to open: {src:?}"))?;
        let is_executable = is_file_executable(&file)
            .await
            .with_context(|| format!("is_file_executable(): {src:?}"))?;
        let digest = Digest::for_file(file)
            .await
            .with_context(|| format!("Digest::for_file(): {src:?}"))?;
        let path = exec_path.strip_prefix(out_dir).unwrap_or(exec_path);
        if !path.is_relative() {
            bail!("Path should be relative: {:?}", path);
        }
        Ok(OutputFile {
            path: path.to_str().unwrap().into(),
            digest: Some(digest),
            is_executable,
            contents: vec![],
            node_properties: None,
        })
    }

    async fn cache_action_result(
        action_digest: &MessageDigest,
        execution_result: &ExecutionResult,
        output_files: Vec<OutputFile>,
        sandbox_dir: Option<&PathBuf>,
        cache: &mut Cache,
        use_remote_cache: bool,
    ) -> Result<Vec<OutputFile>, anyhow::Error> {
        assert!(execution_result.success());
        let mut action_result = ActionResult {
            output_files,
            exit_code: execution_result.exit_code.unwrap_or_default(),
            execution_metadata: Some(ExecutedActionMetadata {
                virtual_execution_duration: execution_result.exec_duration.map(|x| {
                    prost_types::Duration {
                        seconds: x.as_secs() as i64,
                        nanos: x.subsec_nanos() as i32,
                    }
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        // TODO add stdout/stderr files for non-small outputs
        action_result.stdout_raw = execution_result.stdout.clone();
        action_result.stderr_raw = execution_result.stderr.clone();
        cache
            .push(action_digest, &action_result, sandbox_dir, use_remote_cache)
            .await?;
        Ok(action_result.output_files)
    }

    fn on_command_finished(
        &mut self,
        id: CommandId,
        execution_result: &ExecutionResult,
        output_files: Vec<OutputFile>,
        output_files_cached: bool,
    ) {
        let retry = self.scheduler.set_finished_and_get_retry_flag(
            &self.commands[id],
            execution_result.status == ExecutionStatus::Killed,
        );
        if retry {
            self.on_command_retry(id, execution_result);
        } else {
            let measurements = self
                .measurements
                .collect(&self.commands[id].name, execution_result);
            self.profile.collect(&self.commands[id], execution_result);
            let output_size = output_files
                .iter()
                .map(|x| x.digest.as_ref().unwrap().size_bytes as u64)
                .sum::<u64>()
                + execution_result.stdout.len() as u64
                + execution_result.stderr.len() as u64;
            self.log_file.push(
                &self.commands[id],
                execution_result,
                Some(output_size),
                measurements,
            );
            if execution_result.success() {
                self.set_output_file_digests(output_files, output_files_cached);
                self.on_command_succeeded(id, execution_result);
            } else if self.commands[id].tags.contains(&Tag::Condition) {
                self.on_condition_failed(id, execution_result);
            } else {
                self.on_command_failed(id, execution_result);
            }
            self.tui_dirty = true;
        }
    }

    fn set_output_file_digests(
        &mut self,
        output_files: Vec<OutputFile>,
        output_files_cached: bool,
    ) {
        for output_file in output_files {
            assert!(output_file.digest.is_some());
            let path = PathBuf::from(output_file.path);
            let file = &mut self.files[self.path_to_file_id[&path]];
            assert!(file.digest.is_none());
            file.digest = output_file.digest;
            if output_files_cached {
                file.locally_cached = true;
            }
        }
    }

    /// Track state and check if reverse dependencies are ready
    fn on_command_succeeded(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        self.succeeded.push(id);
        if execution_result.cache_hit.is_some() {
            self.cache_hits += 1;
        }
        let command = &mut self.commands[id];
        command.schedule_state = ScheduleState::Succeeded;
        self.tui.command_succeeded(command, execution_result);
        for rdep_id in command.reverse_deps.clone() {
            let rdep = &mut self.commands[rdep_id];
            assert!(!rdep.unfinished_deps.is_empty());
            rdep.unfinished_deps
                .swap_remove(rdep.unfinished_deps.iter().position(|x| *x == id).unwrap());
            if rdep.unfinished_deps.is_empty() {
                assert_eq!(rdep.schedule_state, ScheduleState::Waiting);
                rdep.schedule_state = ScheduleState::Ready;
                self.waiting.remove(&rdep_id);
                self.scheduler.push_ready(rdep);
            }
        }
    }

    fn on_command_retry(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        let command = &self.commands[id];
        self.tui.command_retry(command, execution_result);
    }

    fn on_command_failed(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        self.failed.push(id);
        let command = &self.commands[id];
        self.tui.command_failed(command, execution_result);
    }

    fn on_condition_failed(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        let command = &self.commands[id];
        self.tui.command_failed(command, execution_result);
        let mut ids_to_skip = command.reverse_deps.clone();
        while let Some(id_to_skip) = ids_to_skip.pop() {
            let to_skip = &mut self.commands[id_to_skip];
            if to_skip.schedule_state == ScheduleState::Skipped {
                continue;
            }
            assert_eq!(to_skip.schedule_state, ScheduleState::Waiting);
            assert!(!to_skip.unfinished_deps.is_empty());
            to_skip.schedule_state = ScheduleState::Skipped;
            self.log_file
                .push_not_run(to_skip, ExecutionStatus::Skipped);
            self.waiting.remove(&id_to_skip);
            self.skipped.push(id_to_skip);
            ids_to_skip.extend(to_skip.reverse_deps.iter());
        }
    }

    fn get_bzl_action_for_command(
        &self,
        command: &Command,
    ) -> (bazel_remote_exec::Command, bazel_remote_exec::Directory) {
        let bzl_command = bazel_remote_exec::Command {
            arguments: command.executor.args_with_executable(),
            environment_variables: command
                .executor
                .env()
                .map(|x| {
                    x.clone()
                        .into_iter()
                        .map(|(name, value)| EnvironmentVariable { name, value })
                        .sorted_unstable_by(|a, b| Ord::cmp(&a.name, &b.name))
                        .collect()
                })
                .unwrap_or_default(),
            output_paths: command
                .outputs
                .iter()
                .map(|x| self.files[*x].path.to_str().unwrap())
                .sorted_unstable()
                .dedup()
                .map_into()
                .collect(),
            working_directory: "".to_string(),
            ..Default::default()
        };
        // TODO properly build bazel_remote_exec::Directory tree
        let bzl_input_root = bazel_remote_exec::Directory {
            files: chain(command.executables.iter(), command.inputs.iter())
                .map(|x| {
                    let file = &self.files[*x];
                    assert!(file.digest.is_some(), "digest missing for {:?}", file.path);
                    bazel_remote_exec::FileNode {
                        name: file.path.to_str().unwrap().into(),
                        digest: file.digest.clone(),
                        is_executable: false, // TODO bazel_remote_exec::FileNode::is_executable
                        node_properties: None,
                    }
                })
                .sorted_unstable_by(|a, b| Ord::cmp(&a.name, &b.name))
                .collect(),
            directories: vec![],
            symlinks: vec![],
            node_properties: None,
        };
        (bzl_command, bzl_input_root)
    }

    fn push_logs_for_not_started_commands(&mut self) {
        assert_eq!(self.scheduler.running(), 0);
        for id in self.waiting.iter().chain(self.scheduler.ready_ids().iter()) {
            self.log_file
                .push_not_run(&self.commands[*id], ExecutionStatus::NotStarted);
        }
    }

    fn write_metadata(&self, group_by_tag: &str) -> Result<(), anyhow::Error> {
        let dir = self.out_dir.join("razel-metadata");
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create metadata directory: {dir:?}"))?;
        write_graphs_html(&self.commands, &self.files, &dir.join("graphs.html"))?;
        self.measurements.write_csv(&dir.join("measurements.csv"))?;
        self.profile.write_json(&dir.join("execution_times.json"))?;
        self.log_file.write(&dir.join("log.json"))?;
        let report = Report::new(group_by_tag, &self.log_file.items);
        report.print();
        report.write(&dir.join("report.json"))?;
        Ok(())
    }
}

impl Default for Razel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;
    use serial_test::serial;

    use crate::{Razel, SchedulerExecStats};

    /// Test that commands are actually run in parallel limited by Scheduler::worker_threads
    #[tokio::test]
    #[serial]
    async fn parallel_real_time_test() {
        let mut razel = Razel::new();
        razel.read_cache = false;
        let threads = razel.worker_threads;
        let n = threads * 3;
        let sleep_duration = 0.5;
        for i in 0..n {
            razel
                .push_custom_command(
                    format!("{i}"),
                    "cmake".into(),
                    vec!["-E".into(), "sleep".into(), sleep_duration.to_string()],
                    Default::default(),
                    vec![],
                    vec![],
                    None,
                    None,
                    vec![],
                    vec![],
                )
                .unwrap();
        }
        assert_eq!(razel.len(), n);
        let stats = razel
            .run(false, true, "", None, vec![], None)
            .await
            .unwrap();
        assert_eq!(
            stats.exec,
            SchedulerExecStats {
                succeeded: n,
                ..Default::default()
            }
        );
        assert_abs_diff_eq!(
            stats.execution_duration.as_secs_f64(),
            (n as f64 / threads as f64).ceil() * sleep_duration,
            epsilon = sleep_duration * 0.5
        );
    }
}
