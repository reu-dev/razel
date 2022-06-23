use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, fs};

use anyhow::{bail, Context};
use itertools::Itertools;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use which::which;

use crate::bazel_remote_exec::{ActionResult, Digest, OutputFile};
use crate::cache::{BlobDigest, Cache, LocalCache, MessageDigest};
use crate::executors::{ExecutionResult, ExecutionStatus, Executor};
use crate::{
    bazel_remote_exec, config, Arena, Command, CommandBuilder, CommandId, File, FileId, Sandbox,
};

#[derive(Debug, PartialEq)]
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

#[derive(Debug, Default, PartialEq)]
pub struct SchedulerExecStats {
    pub succeeded: usize,
    pub failed: usize,
    pub not_run: usize,
}

type ExecutionResultChannel = (CommandId, ExecutionResult, Option<ActionResult>);

pub struct Scheduler {
    pub read_cache: bool,
    worker_threads: usize,
    /// absolute directory to resolve relative paths of input/output files
    workspace_dir: PathBuf,
    /// current working directory, read-only, used to execute commands
    current_dir: PathBuf,
    /// directory of output files
    out_dir: PathBuf,
    cache: Cache,
    files: Arena<File>,
    path_to_file_id: HashMap<PathBuf, FileId>,
    which_to_file_id: HashMap<String, FileId>,
    commands: Arena<Command>,
    waiting: HashSet<CommandId>,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready: VecDeque<CommandId>,
    running: usize,
    succeeded: Vec<CommandId>,
    failed: Vec<CommandId>,
    cache_hits: usize,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        let worker_threads = num_cpus::get();
        assert!(worker_threads > 0);
        let current_dir = env::current_dir().unwrap();
        let workspace_dir = current_dir.clone();
        let out_dir = PathBuf::from(config::OUT_DIR);
        debug!("workspace_dir: {:?}", workspace_dir);
        debug!("out_dir:       {:?}", out_dir);
        Scheduler {
            read_cache: true,
            worker_threads,
            workspace_dir,
            current_dir,
            out_dir,
            cache: Cache::new().unwrap(),
            files: Default::default(),
            path_to_file_id: Default::default(),
            which_to_file_id: Default::default(),
            commands: Default::default(),
            waiting: Default::default(),
            ready: Default::default(),
            running: 0,
            succeeded: vec![],
            failed: vec![],
            cache_hits: 0,
        }
    }

    /// Remove the binary directory
    pub fn clean(&self) {
        fs::remove_dir_all(&self.out_dir).ok();
    }

    /// Set the directory to resolve relative paths of input/output files
    pub fn set_workspace_dir(&mut self, workspace: &Path) {
        if workspace.is_absolute() {
            self.workspace_dir = workspace.into();
        } else {
            self.workspace_dir = self.current_dir.join(workspace);
        }
        debug!("workspace_dir: {:?}", self.workspace_dir);
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn push_custom_command(
        &mut self,
        name: String,
        executable: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Result<CommandId, anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args);
        builder.inputs(&inputs, self)?;
        builder.outputs(&outputs, self)?;
        builder.custom_command_executor(executable, env, self)?;
        self.push(builder)
    }

    pub fn push(&mut self, builder: CommandBuilder) -> Result<CommandId, anyhow::Error> {
        let id = self.commands.alloc_with_id(|id| builder.build(id));
        // TODO check if name is unique
        // patch outputs.creating_command
        for output_id in &self.commands[id].outputs {
            let output = &mut self.files[*output_id];
            assert!(output.creating_command.is_none());
            output.creating_command = Some(id);
        }
        Ok(id)
    }

    #[cfg(test)]
    pub fn get_command(&self, id: CommandId) -> Option<&Command> {
        self.commands.get(id)
    }

    pub async fn run(&mut self) -> Result<SchedulerStats, anyhow::Error> {
        let preparation_start = Instant::now();
        if self.commands.is_empty() {
            bail!("no commands added");
        }
        self.create_dependency_graph();
        self.digest_input_files().await?;
        self.create_output_dirs()?;
        let (tx, mut rx) = mpsc::channel(32);
        let execution_start = Instant::now();
        self.start_ready_commands(&tx);
        while self.ready.len() + self.running != 0 {
            if let Some((id, execution_result, action_result)) = rx.recv().await {
                self.on_command_finished(id, execution_result, action_result)
                    .await;
                self.start_ready_commands(&tx);
            }
        }
        Ok(SchedulerStats {
            exec: SchedulerExecStats {
                succeeded: self.succeeded.len(),
                failed: self.failed.len(),
                not_run: self.waiting.len() + self.ready.len(),
            },
            cache_hits: self.cache_hits,
            preparation_duration: execution_start.duration_since(preparation_start),
            execution_duration: execution_start.elapsed(),
        })
    }

    /// Register an executable to be used for a command
    pub fn executable(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        if arg.contains('.') {
            self.input_file(arg)
        } else if let Some(x) = self.which_to_file_id.get(&arg) {
            Ok(&self.files[*x])
        } else {
            let path = which(&arg)?;
            info!("which({}) => {:?}", arg, path);
            let id = self.input_file(path.to_str().unwrap().into())?.id;
            self.which_to_file_id.insert(arg, id);
            Ok(&self.files[id])
        }
    }

    pub fn input_file(&mut self, arg: String) -> Result<&File, anyhow::Error> {
        let rel_path = self.rel_path(&arg)?;
        let id = self
            .path_to_file_id
            .get(&rel_path)
            .cloned()
            .unwrap_or_else(|| {
                let id = self.files.alloc_with_id(|id| File {
                    id,
                    arg,
                    exec_path: rel_path.clone(),
                    out_path: rel_path.clone(),
                    creating_command: None,
                    digest: None,
                });
                self.path_to_file_id.insert(rel_path, id);
                id
            });
        Ok(&self.files[id])
    }

    pub fn output_file(&mut self, arg: &String) -> Result<&File, anyhow::Error> {
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
        let id = self.files.alloc_with_id(|id| File {
            id,
            creating_command: None, // will be patched in Scheduler::push()
            exec_path: rel_path.clone(),
            out_path: self.out_dir.join(&rel_path),
            arg: arg.clone(),
            digest: None,
        });
        self.path_to_file_id.insert(rel_path, id);
        Ok(&self.files[id])
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
            for input_id in &command.inputs {
                if let Some(dep) = self.files[*input_id].creating_command {
                    command.unfinished_deps.push(dep);
                    rdeps.push((dep, command.id));
                }
            }
            if command.unfinished_deps.is_empty() {
                command.schedule_state = ScheduleState::Ready;
                self.ready.push_back(command.id);
            } else {
                command.schedule_state = ScheduleState::Waiting;
                self.waiting.insert(command.id);
            }
        }
        for (id, rdep) in rdeps {
            self.commands[id].reverse_deps.push(rdep);
        }
        self.check_for_circular_dependencies();
        assert!(!self.ready.is_empty());
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
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
                let path = file.exec_path.clone();
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
            .map(|x| x.out_path.parent().unwrap())
            .sorted_unstable()
            .dedup();
        for x in dirs {
            fs::create_dir_all(x)
                .with_context(|| format!("Failed to create output directory: {:?}", x.clone()))?;
        }
        Ok(())
    }

    fn start_ready_commands(&mut self, tx: &Sender<ExecutionResultChannel>) {
        while self.running < self.worker_threads && !self.ready.is_empty() {
            let id = self.ready.pop_front().unwrap();
            self.start_next_command(id, tx.clone());
        }
    }

    fn collect_input_file_paths_for_command(&self, command: &Command) -> Vec<PathBuf> {
        command
            .inputs
            .iter()
            .map(|x| self.files[*x].out_path.clone())
            .collect()
    }

    fn collect_output_file_paths_for_command(&self, command: &Command) -> Vec<PathBuf> {
        command
            .outputs
            .iter()
            .map(|x| self.files[*x].out_path.clone())
            .collect()
    }

    /// Execute a command in a worker thread with caching.
    ///
    /// If the executed command failed, action_result will be None and the action will not be cached.
    /// Panic only in case of system errors.
    fn start_next_command(&mut self, id: CommandId, tx: Sender<ExecutionResultChannel>) {
        self.running += 1;
        let command = &self.commands[id];
        assert_eq!(command.schedule_state, ScheduleState::Ready);
        assert_eq!(command.unfinished_deps.len(), 0);
        let action = self.get_bzl_action_for_command(command);
        let action_digest = Digest::for_message(&action);
        info!("Execute {}", command.name);
        let cache = self.cache.clone();
        let read_cache = self.read_cache;
        let executor = command.executor.clone();
        let input_paths = self.collect_input_file_paths_for_command(command);
        let output_paths = self.collect_output_file_paths_for_command(command);
        let sandbox = executor
            .use_sandbox()
            .then(|| Sandbox::new(&command.id.to_string()));
        let out_dir = self.out_dir.clone();
        tokio::task::spawn(async move {
            let (execution_result, action_result) = if let Some(x) =
                Self::get_action_from_cache(&action_digest, &cache, read_cache).await
            {
                x
            } else {
                Self::exec_action(
                    &action_digest,
                    &cache,
                    &executor,
                    &input_paths,
                    &output_paths,
                    &sandbox,
                    &out_dir,
                )
                .await
                .context("exec_action()")
                .with_context(|| executor.command_line())
                .unwrap()
            };
            if let Some(action_result) = &action_result {
                cache
                    .symlink_output_files_into_out_dir(action_result, &out_dir)
                    .await
                    .context("symlink_output_files_into_out_dir()")
                    .with_context(|| executor.command_line())
                    .unwrap();
            }
            tx.send((id, execution_result, action_result))
                .await
                .unwrap();
        });
    }

    async fn get_action_from_cache(
        action_digest: &MessageDigest,
        cache: &Cache,
        read_cache: bool,
    ) -> Option<(ExecutionResult, Option<ActionResult>)> {
        if read_cache {
            if let Some(action_result) = cache.get_action_result(&action_digest).await {
                let exit_code = Some(action_result.exit_code);
                let execution_result = ExecutionResult {
                    status: ExecutionStatus::Success,
                    exit_code,
                    error: None,
                    cache_hit: true,
                };
                return Some((execution_result, Some(action_result)));
            }
        }
        None
    }

    async fn exec_action(
        action_digest: &MessageDigest,
        cache: &Cache,
        executor: &Executor,
        input_paths: &Vec<PathBuf>,
        output_paths: &Vec<PathBuf>,
        sandbox: &Option<Sandbox>,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Option<ActionResult>), anyhow::Error> {
        if let Some(sandbox) = &sandbox {
            sandbox
                .create(&input_paths, &output_paths)
                .await
                .context("Sandbox::create()")?;
        }
        let execution_result = executor.exec(sandbox.as_ref().map(|x| x.dir.clone())).await;
        let action_result = if execution_result.success() {
            Some(
                Self::cache_action_result(
                    &action_digest,
                    &execution_result,
                    &output_paths,
                    sandbox.as_ref().map(|x| x.dir.clone()),
                    &out_dir,
                    &cache,
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
                    .await?,
            );
        }
        let action_result = ActionResult {
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
            execution_metadata: None,
        };
        cache
            .push_action_result(action_digest, &action_result)
            .await;
        Ok(action_result)
    }

    async fn on_command_finished(
        &mut self,
        id: CommandId,
        execution_result: ExecutionResult,
        action_result: Option<ActionResult>,
    ) {
        self.running -= 1;
        if execution_result.success() {
            self.set_output_file_digests(action_result.unwrap().output_files);
            self.on_command_succeeded(id, execution_result);
        } else {
            self.on_command_failed(id, execution_result);
        }
    }

    fn set_output_file_digests(&mut self, output_files: Vec<OutputFile>) {
        for output_file in output_files {
            let mut output_file_path = PathBuf::from(output_file.path);
            if let Ok(x) = output_file_path.strip_prefix(&self.out_dir) {
                output_file_path = x.into();
            }
            assert!(output_file_path.is_relative());
            let file = &mut self.files[self.path_to_file_id[&output_file_path]];
            assert!(file.digest.is_none());
            file.digest = output_file.digest;
        }
    }

    /// Track state and check if reverse dependencies are ready
    fn on_command_succeeded(&mut self, id: CommandId, execution_result: ExecutionResult) {
        self.succeeded.push(id);
        if execution_result.cache_hit {
            self.cache_hits += 1;
        }
        let command = &mut self.commands[id];
        command.schedule_state = ScheduleState::Succeeded;
        info!("Success {}: {:?}", command.name, execution_result);
        for rdep_id in command.reverse_deps.clone() {
            let rdep = &mut self.commands[rdep_id];
            assert_eq!(rdep.schedule_state, ScheduleState::Waiting);
            assert!(!rdep.unfinished_deps.is_empty());
            rdep.unfinished_deps
                .swap_remove(rdep.unfinished_deps.iter().position(|x| *x == id).unwrap());
            if rdep.unfinished_deps.is_empty() {
                rdep.schedule_state = ScheduleState::Ready;
                self.waiting.remove(&rdep_id);
                self.ready.push_back(rdep_id);
            }
        }
    }

    fn on_command_failed(&mut self, id: CommandId, result: ExecutionResult) {
        self.failed.push(id);
        let command = &self.commands[id];
        error!("Error  {}: {:?}", command.name, result);
    }

    fn get_bzl_action_for_command(&self, command: &Command) -> bazel_remote_exec::Action {
        let bzl_command = bazel_remote_exec::Command {
            arguments: command.executor.args_with_executable(),
            environment_variables: vec![],
            output_paths: command
                .outputs
                .iter()
                .map(|x| self.files[*x].exec_path.to_str().unwrap())
                .sorted_unstable()
                .dedup()
                .map_into()
                .collect(),
            working_directory: "".to_string(),
            ..Default::default()
        };
        // TODO properly build bazel_remote_exec::Directory tree
        let bzl_input_root = bazel_remote_exec::Directory {
            files: command
                .inputs
                .iter()
                .map(|x| {
                    let file = &self.files[*x];
                    assert!(
                        file.digest.is_some(),
                        "digest missing for {:?}",
                        file.exec_path
                    );
                    bazel_remote_exec::FileNode {
                        name: file.exec_path.to_str().unwrap().into(),
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
        let bzl_action = bazel_remote_exec::Action {
            command_digest: Some(Digest::for_message(&bzl_command)),
            input_root_digest: Some(Digest::for_message(&bzl_input_root)),
            ..Default::default()
        };
        bzl_action
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;
    use serial_test::serial;

    use crate::{Scheduler, SchedulerExecStats};

    /// Test that commands are actually run in parallel limited by Scheduler::worker_threads
    #[tokio::test]
    #[serial]
    async fn parallel() {
        let mut scheduler = Scheduler::new();
        scheduler.read_cache = false;
        let threads = scheduler.worker_threads;
        let n = threads * 3;
        let sleep_duration = 0.5;
        for i in 0..n {
            scheduler
                .push_custom_command(
                    format!("{}", i),
                    "cmake".into(),
                    vec!["-E".into(), "sleep".into(), sleep_duration.to_string()],
                    Default::default(),
                    vec![],
                    vec![],
                )
                .unwrap();
        }
        assert_eq!(scheduler.len(), n);
        let stats = scheduler.run().await.unwrap();
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
