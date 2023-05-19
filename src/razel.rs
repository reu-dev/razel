use std::collections::{hash_map, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, fs};

use anyhow::{anyhow, bail, Context};
use itertools::{chain, Itertools};
use log::{debug, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use which::which;

use crate::bazel_remote_exec::command::EnvironmentVariable;
use crate::bazel_remote_exec::{ActionResult, Digest, ExecutedActionMetadata, OutputFile};
use crate::cache::{BlobDigest, Cache, MessageDigest};
use crate::executors::{ExecutionResult, ExecutionStatus, Executor, WasiExecutor};
use crate::metadata::{write_graphs_html, Measurements, Profile, Tag};
use crate::{
    bazel_remote_exec, config, force_remove_file, Arena, CGroup, Command, CommandBuilder,
    CommandId, File, FileId, FileType, Sandbox, Scheduler, TUI,
};

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
}

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub exec: SchedulerExecStats,
    pub cache_hits: usize,
    pub preparation_duration: Duration,
    pub execution_duration: Duration,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SchedulerExecStats {
    pub succeeded: usize,
    pub failed: usize,
    pub not_run: usize,
}

impl SchedulerExecStats {
    pub fn finished_successfully(&self) -> bool {
        self.failed == 0 && self.not_run == 0
    }
}

type ExecutionResultChannel = (CommandId, ExecutionResult, Option<ActionResult>);

pub struct Razel {
    pub read_cache: bool,
    worker_threads: usize,
    /// absolute directory to resolve relative paths of input/output files
    workspace_dir: PathBuf,
    /// current working directory, read-only, used to execute commands
    current_dir: PathBuf,
    /// directory of output files - relative to current_dir
    out_dir: PathBuf,
    cache: Cache,
    files: Arena<File>,
    /// maps paths relative to current_dir (without out_dir prefix) to <File>s
    path_to_file_id: HashMap<PathBuf, FileId>,
    which_to_file_id: HashMap<String, FileId>,
    /// razel executable - used in Action::input_root_digest for versioning tasks
    self_file_id: Option<FileId>,
    commands: Arena<Command>,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    waiting: HashSet<CommandId>,
    scheduler: Scheduler,
    succeeded: Vec<CommandId>,
    failed: Vec<CommandId>,
    cache_hits: usize,
    tui: TUI,
    measurements: Measurements,
    profile: Profile,
}

impl Razel {
    pub fn new() -> Razel {
        let worker_threads = num_cpus::get();
        assert!(worker_threads > 0);
        let current_dir = env::current_dir().unwrap();
        let workspace_dir = current_dir.clone();
        let out_dir = PathBuf::from(config::OUT_DIR);
        let cache = Cache::new(&workspace_dir).unwrap();
        debug!("workspace_dir: {:?}", workspace_dir);
        debug!("out_dir:       {:?}", current_dir.join(&out_dir));
        let cgroup = match Self::create_cgroup() {
            Ok(x) => x,
            Err(e) => {
                debug!("create_cgroup(): {:?}", e);
                None
            }
        };
        Razel {
            read_cache: true,
            worker_threads,
            workspace_dir,
            current_dir,
            out_dir,
            cache,
            files: Default::default(),
            path_to_file_id: Default::default(),
            which_to_file_id: Default::default(),
            self_file_id: None,
            commands: Default::default(),
            cgroup,
            waiting: Default::default(),
            scheduler: Scheduler::new(worker_threads),
            succeeded: vec![],
            failed: vec![],
            cache_hits: 0,
            tui: TUI::new(),
            measurements: Measurements::new(),
            profile: Profile::new(),
        }
    }

    #[cfg(target_os = "linux")]
    fn create_cgroup() -> Result<Option<CGroup>, anyhow::Error> {
        use crate::get_available_memory;
        let available = get_available_memory()?;
        let mut limit = available;
        let existing_limit = CGroup::new("".into()).read::<u64>("memory", "memory.limit_in_bytes");
        if let Ok(x) = existing_limit {
            limit = limit.min(x); // memory.limit_in_bytes will be infinite if not set
        }
        limit = (limit as f64 * 0.95) as u64;
        let cgroup = CGroup::new(config::EXECUTABLE.into());
        cgroup.create("memory")?;
        cgroup.write("memory", "memory.limit_in_bytes", limit)?;
        cgroup.write("memory", "memory.swappiness", 0)?;
        debug!(
            "create_cgroup(): available: {}MiB, limit: {:?}MiB -> set limit {}MiB",
            available / 1024 / 1024,
            existing_limit.ok().map(|x| x / 1024 / 1024),
            limit / 1024 / 1024
        );
        Ok(Some(cgroup))
    }

    #[cfg(not(target_os = "linux"))]
    fn create_cgroup() -> Result<Option<CGroup>, anyhow::Error> {
        // no error, just not supported
        Ok(None)
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
        debug!("workspace_dir: {:?}", self.workspace_dir);
        self.cache = Cache::new(&self.workspace_dir)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn show_info(&self) {
        println!(
            "output directory: {:?}",
            self.current_dir.join(&self.out_dir)
        );
        println!("cache directory:  {:?}", self.cache.local_cache.dir);
        println!("worker threads:   {}", self.worker_threads);
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
        if !matches!(&self.commands[id].executor, Executor::CustomCommand(_)) {
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

    pub fn list_commands(&mut self) {
        self.create_dependency_graph();
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            let command = &mut self.commands[id];
            println!("# {}", command.name);
            println!(
                "{}",
                TUI::format_command_line(
                    &command
                        .executor
                        .command_line_with_redirects(&self.tui.razel_executable)
                )
            );
            command.schedule_state = ScheduleState::Succeeded;
            self.scheduler.set_finished_and_get_retry_flag(id, false);
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

    pub async fn run(
        &mut self,
        keep_going: bool,
        verbose: bool,
    ) -> Result<SchedulerStats, anyhow::Error> {
        let preparation_start = Instant::now();
        if self.commands.is_empty() {
            bail!("no commands added");
        }
        self.tui.verbose = verbose;
        Sandbox::cleanup();
        self.create_dependency_graph();
        self.remove_unknown_files_from_out_dir(&self.out_dir).ok();
        self.digest_input_files().await?;
        self.create_output_dirs()?;
        self.create_wasi_modules()?;
        let (tx, mut rx) = mpsc::channel(32);
        let execution_start = Instant::now();
        self.start_ready_commands(&tx);
        let mut start_more_commands = true;
        while self.scheduler.running() != 0 {
            if let Some((id, execution_result, action_result)) = rx.recv().await {
                self.on_command_finished(id, &execution_result, action_result);
                if execution_result.status == ExecutionStatus::SystemError
                    || (!execution_result.success() && !keep_going)
                {
                    start_more_commands = false;
                }
                if start_more_commands {
                    self.start_ready_commands(&tx);
                }
            }
        }
        self.remove_outputs_of_not_run_actions_from_out_dir();
        Sandbox::cleanup();
        self.write_metadata()?;
        let stats = SchedulerStats {
            exec: SchedulerExecStats {
                succeeded: self.succeeded.len(),
                failed: self.failed.len(),
                not_run: self.waiting.len() + self.scheduler.ready(),
            },
            cache_hits: self.cache_hits,
            preparation_duration: execution_start.duration_since(preparation_start),
            execution_duration: execution_start.elapsed(),
        };
        self.tui.finished(&stats);
        Ok(stats)
    }

    /// Register an executable file
    pub fn executable(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        let path = Path::new(&arg);
        let (file_type, abs_path) = if let Some(x) = arg.strip_prefix("./") {
            (FileType::ExecutableInWorkspace, self.workspace_dir.join(x))
        } else if path.is_absolute() {
            match path.strip_prefix(&self.workspace_dir) {
                Ok(_) => (FileType::ExecutableInWorkspace, path.to_path_buf()),
                _ => (FileType::SystemExecutable, path.to_path_buf()),
            }
        } else if arg == config::EXECUTABLE {
            return self.lazy_self_file_id().map(|x| &self.files[x]);
        } else {
            // relative path or system binary name
            let abs = self.workspace_dir.join(path);
            let cwd_path = abs.strip_prefix(&self.current_dir).unwrap().to_path_buf();
            if let Some(id) = self.path_to_file_id.get(&cwd_path) {
                return Ok(&self.files[*id]);
            } else if arg.contains('/') || abs.is_file() {
                (FileType::ExecutableInWorkspace, abs)
            } else {
                return self.executable_which(arg, FileType::SystemExecutable);
            }
        };
        assert!(abs_path.is_absolute());
        let cwd_path = if file_type == FileType::SystemExecutable {
            abs_path
        } else {
            self.rel_path(&abs_path.to_str().unwrap().into())?
        };
        self.input_file_for_rel_path(arg, file_type, cwd_path)
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
            for input_id in chain(command.executables.iter(), command.inputs.iter()) {
                if let Some(dep) = self.files[*input_id].creating_command {
                    command.unfinished_deps.push(dep);
                    rdeps.push((dep, command.id));
                }
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
                    tx.send((id, Digest::for_file(path).await)).await.ok();
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

    fn start_ready_commands(&mut self, tx: &Sender<ExecutionResultChannel>) {
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            self.start_next_command(id, tx.clone());
        }
        self.update_status();
    }

    fn update_status(&mut self) {
        self.tui.status(
            self.succeeded.len(),
            self.cache_hits,
            self.failed.len(),
            self.scheduler.running(),
            self.waiting.len() + self.scheduler.ready(),
        );
    }

    fn collect_input_file_paths_for_sandbox(&self, command: &Command) -> Vec<PathBuf> {
        let command_executables = command.executables.iter().filter(|&&x| {
            if matches!(command.executor, Executor::Wasi(_)) {
                false
            } else if let Some(self_file_id) = self.self_file_id {
                // razel never calls itself
                x != self_file_id
            } else {
                true
            }
        });
        chain(command_executables, command.inputs.iter())
            .map(|x| self.files[*x].path.clone())
            .collect()
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
    fn start_next_command(&mut self, id: CommandId, tx: Sender<ExecutionResultChannel>) {
        let command = &self.commands[id];
        assert_eq!(command.schedule_state, ScheduleState::Ready);
        assert_eq!(command.unfinished_deps.len(), 0);
        let action = self.get_bzl_action_for_command(command);
        let action_digest = Digest::for_message(&action);
        let cache = self.cache.clone();
        let read_cache = self.read_cache;
        let executor = command.executor.clone();
        let sandbox_input_paths = self.collect_input_file_paths_for_sandbox(command);
        let output_paths = self.collect_output_file_paths_for_command(command);
        let sandbox = executor
            .use_sandbox()
            .then(|| Sandbox::new(&command.id.to_string()));
        let cgroup = self.cgroup.clone();
        let out_dir = self.out_dir.clone();
        tokio::task::spawn(async move {
            let (execution_result, action_result) = Self::exec_action_with_cache(
                &action_digest,
                &cache,
                read_cache,
                &executor,
                &sandbox_input_paths,
                &output_paths,
                &sandbox,
                cgroup,
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
                    None,
                )
            });
            // ignore SendError - channel might be closed if a previous command failed
            tx.send((id, execution_result, action_result)).await.ok();
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_action_with_cache(
        action_digest: &MessageDigest,
        cache: &Cache,
        read_cache: bool,
        executor: &Executor,
        sandbox_input_paths: &Vec<PathBuf>,
        output_paths: &Vec<PathBuf>,
        sandbox: &Option<Sandbox>,
        cgroup: Option<CGroup>,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Option<ActionResult>), anyhow::Error> {
        let (execution_result, action_result) =
            if let Some(x) = Self::get_action_from_cache(action_digest, cache, read_cache).await {
                x
            } else {
                Self::exec_action(
                    action_digest,
                    cache,
                    executor,
                    sandbox_input_paths,
                    output_paths,
                    sandbox,
                    cgroup,
                    out_dir,
                )
                .await
                .context("exec_action()")?
            };
        if let Some(action_result) = &action_result {
            cache
                .symlink_output_files_into_out_dir(action_result, out_dir)
                .await
                .context("symlink_output_files_into_out_dir()")?;
        }
        Ok((execution_result, action_result))
    }

    async fn get_action_from_cache(
        action_digest: &MessageDigest,
        cache: &Cache,
        read_cache: bool,
    ) -> Option<(ExecutionResult, Option<ActionResult>)> {
        if read_cache {
            if let Some(action_result) = cache.get_action_result(action_digest).await {
                let exit_code = Some(action_result.exit_code);
                let metadata = action_result.execution_metadata.as_ref();
                let execution_result = ExecutionResult {
                    status: ExecutionStatus::Success,
                    exit_code,
                    error: None,
                    cache_hit: true,
                    stdout: action_result.stdout_raw.clone(),
                    stderr: action_result.stderr_raw.clone(),
                    duration: metadata
                        .and_then(|x| x.virtual_execution_duration.as_ref())
                        .map(|x| Duration::new(x.seconds as u64, x.nanos as u32)),
                };
                return Some((execution_result, Some(action_result)));
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_action(
        action_digest: &MessageDigest,
        cache: &Cache,
        executor: &Executor,
        sandbox_input_paths: &Vec<PathBuf>,
        output_paths: &Vec<PathBuf>,
        sandbox: &Option<Sandbox>,
        cgroup: Option<CGroup>,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Option<ActionResult>), anyhow::Error> {
        if let Some(sandbox) = &sandbox {
            sandbox
                .create(sandbox_input_paths, output_paths)
                .await
                .context("Sandbox::create()")?;
        } else {
            // remove expected output files for tasks, because symlinks will not be overwritten
            // maybe a proper sandbox would be better
            for x in output_paths {
                force_remove_file(x).await?;
            }
        }
        let execution_result = executor
            .exec(sandbox.as_ref().map(|x| x.dir.clone()), cgroup)
            .await;
        let action_result = if execution_result.success() {
            Some(
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_paths,
                    sandbox.as_ref().map(|x| x.dir.clone()),
                    out_dir,
                    cache,
                )
                .await
                .with_context(|| "cache_action_result()")?,
            )
        } else {
            None
        };
        if let Some(sandbox) = &sandbox {
            sandbox
                .destroy()
                .await
                .with_context(|| "Sandbox::destroy()")?;
        }
        Ok((execution_result, action_result))
    }

    async fn cache_action_result(
        action_digest: &MessageDigest,
        execution_result: &ExecutionResult,
        output_paths: &Vec<PathBuf>,
        sandbox_dir: Option<PathBuf>,
        out_dir: &PathBuf,
        cache: &Cache,
    ) -> Result<ActionResult, anyhow::Error> {
        assert!(execution_result.success());
        let mut output_files: Vec<OutputFile> = Vec::with_capacity(output_paths.len());
        for path in output_paths {
            output_files.push(
                cache
                    .move_output_file_into_cache(&sandbox_dir, out_dir, path)
                    .await
                    .context("move_output_file_into_cache()")?,
            );
        }
        let mut action_result = ActionResult {
            output_files,
            output_file_symlinks: vec![],
            output_symlinks: vec![],
            output_directories: vec![],
            output_directory_symlinks: vec![],
            exit_code: execution_result.exit_code.unwrap(),
            stdout_raw: vec![],
            stdout_digest: None,
            stderr_raw: vec![],
            stderr_digest: None,
            execution_metadata: Some(ExecutedActionMetadata {
                worker: "".to_string(),
                queued_timestamp: None,
                worker_start_timestamp: None,
                worker_completed_timestamp: None,
                input_fetch_start_timestamp: None,
                input_fetch_completed_timestamp: None,
                execution_start_timestamp: None,
                execution_completed_timestamp: None,
                virtual_execution_duration: execution_result.duration.map(|x| {
                    prost_types::Duration {
                        seconds: x.as_secs() as i64,
                        nanos: x.subsec_nanos() as i32,
                    }
                }),
                output_upload_start_timestamp: None,
                output_upload_completed_timestamp: None,
                auxiliary_metadata: vec![],
            }),
        };
        // TODO add stdout/stderr files for non-small outputs
        action_result.stdout_raw = execution_result.stdout.clone();
        action_result.stderr_raw = execution_result.stderr.clone();
        cache
            .push_action_result(action_digest, &action_result)
            .await?;
        Ok(action_result)
    }

    fn on_command_finished(
        &mut self,
        id: CommandId,
        execution_result: &ExecutionResult,
        action_result: Option<ActionResult>,
    ) {
        let retry = self.scheduler.set_finished_and_get_retry_flag(
            id,
            execution_result.status == ExecutionStatus::Killed,
        );
        if retry {
            self.on_command_retry(id, execution_result);
        } else {
            self.measurements
                .collect(&self.commands[id].name, execution_result);
            self.profile.collect(&self.commands[id], execution_result);
            if execution_result.success() {
                self.set_output_file_digests(action_result.unwrap().output_files);
                self.on_command_succeeded(id, execution_result);
            } else {
                self.on_command_failed(id, execution_result);
            }
        }
    }

    fn set_output_file_digests(&mut self, output_files: Vec<OutputFile>) {
        for output_file in output_files {
            assert!(!output_file.path.starts_with("./"));
            let path = PathBuf::from(output_file.path);
            assert!(!path.starts_with(&self.out_dir));
            assert!(!path.is_absolute());
            let file = &mut self.files[self.path_to_file_id[&path]];
            assert!(file.digest.is_none());
            file.digest = output_file.digest;
        }
    }

    /// Track state and check if reverse dependencies are ready
    fn on_command_succeeded(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        self.succeeded.push(id);
        if execution_result.cache_hit {
            self.cache_hits += 1;
        }
        let command = &mut self.commands[id];
        command.schedule_state = ScheduleState::Succeeded;
        self.tui.command_succeeded(command, execution_result);
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

    fn on_command_retry(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        let command = &self.commands[id];
        self.tui.command_retry(command, execution_result);
    }

    fn on_command_failed(&mut self, id: CommandId, execution_result: &ExecutionResult) {
        self.failed.push(id);
        let command = &self.commands[id];
        self.tui.command_failed(command, execution_result);
    }

    fn get_bzl_action_for_command(&self, command: &Command) -> bazel_remote_exec::Action {
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
        bazel_remote_exec::Action {
            command_digest: Some(Digest::for_message(&bzl_command)),
            input_root_digest: Some(Digest::for_message(&bzl_input_root)),
            ..Default::default()
        }
    }

    fn write_metadata(&self) -> Result<(), anyhow::Error> {
        let dir = self.out_dir.join("razel-metadata");
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create metadata directory: {dir:?}"))?;
        write_graphs_html(&self.commands, &self.files, &dir.join("graphs.html"))?;
        self.measurements.write_csv(&dir.join("measurements.csv"))?;
        self.profile.write_json(&dir.join("execution_times.json"))?;
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
                )
                .unwrap();
        }
        assert_eq!(razel.len(), n);
        let stats = razel.run(false, true).await.unwrap();
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
