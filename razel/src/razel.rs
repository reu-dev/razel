use crate::bazel_remote_exec::{ActionResult, BazelDigest, ExecutedActionMetadata, OutputFile};
use crate::cache::{BlobDigest, Cache, MessageDigest};
use crate::cli::HttpRemoteExecConfig;
use crate::executors::{
    CommandExecutor, ExecutionResult, ExecutionStatus, Executor, HttpRemoteExecState,
    HttpRemoteExecutor, TaskExecutor, WasiExecutor,
};
use crate::metadata::{LogFile, Measurements, Profile, Report};
use crate::targets_builder::TargetsBuilder;
use crate::tui::TUI;
use crate::types::{
    CommandTarget, DependencyGraph, Digest, ExecutableType, File, FileId, RazelJson,
    RazelJsonCommand, RazelJsonHandler, Tag, Target, TargetId, TargetKind, Task, TaskTarget,
};
use crate::{
    bazel_remote_exec, config, create_cgroup, force_remove_file, is_file_executable,
    select_cache_dir, select_sandbox_dir, write_gitignore, BoxedSandbox, CGroup, SandboxDir,
    Scheduler, TmpDirSandbox, WasiSandbox, GITIGNORE_FILENAME,
};
use anyhow::{bail, Context, Result};
use itertools::{chain, Itertools};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, fs};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use url::Url;

#[derive(Debug, PartialEq, Eq)]
pub enum ScheduleState {
    New,
    /// Target is filtered out
    Excluded,
    /// Target can not yet be executed because dependencies are still missing
    Waiting,
    /// Target is ready for being executed
    Ready,
    /// Target execution finished successfully
    Succeeded,
    /// Target execution failed
    Failed,
    /// Target could not be started because it depends on a failed condition
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

pub type ExecutionResultChannel = (TargetId, ExecutionResult, Vec<OutputFile>, bool);

pub struct Razel {
    pub read_cache: bool,
    worker_threads: usize,
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
    targets_builder: Option<TargetsBuilder>,
    dep_graph: DependencyGraph,
    /// maps paths relative to current_dir (without out_dir prefix) to FileId
    file_by_path: HashMap<PathBuf, FileId>,
    excluded_targets_len: usize,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    http_remote_exec_state: HttpRemoteExecState,
    wasi_module_by_executable: HashMap<FileId, wasmtime::Module>,
    scheduler: Scheduler,
    running_remotely: usize,
    succeeded: Vec<TargetId>,
    failed: Vec<TargetId>,
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
        let out_dir = PathBuf::from(config::OUT_DIR);
        let targets_builder = TargetsBuilder::new();
        Razel {
            read_cache: true,
            worker_threads,
            current_dir,
            out_dir,
            cache: None,
            sandbox_dir: None,
            targets_builder: Some(targets_builder),
            dep_graph: Default::default(),
            file_by_path: Default::default(),
            excluded_targets_len: 0,
            cgroup: None,
            http_remote_exec_state: Default::default(),
            wasi_module_by_executable: Default::default(),
            scheduler: Scheduler::new(worker_threads),
            running_remotely: 0,
            succeeded: vec![],
            failed: vec![],
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

    pub fn set_http_remote_exec_config(&mut self, config: &HttpRemoteExecConfig) {
        let state = HttpRemoteExecState::new(config);
        self.http_remote_exec_state = state.clone();
        self.scheduler.set_http_remote_exec_config(state);
    }

    pub fn push_json_command(&mut self, json: RazelJsonCommand) -> Result<TargetId> {
        self.targets_builder
            .as_mut()
            .unwrap()
            .push_json_command(json)
    }

    pub fn push_task(
        &mut self,
        name: String,
        args: Vec<String>,
        task: Task,
        tags: Vec<Tag>,
    ) -> Result<TargetId> {
        self.targets_builder
            .as_mut()
            .unwrap()
            .push_task(name, args, task, tags)
    }

    #[doc(hidden)]
    pub fn add_tag_for_command(&mut self, name: &str, tag: Tag) {
        let builder = &mut self.targets_builder.as_mut().unwrap();
        let target = &mut builder.targets[builder.target_by_name[name]];
        target.tags.push(tag);
        TargetsBuilder::check_tags(target).unwrap();
    }

    pub fn list_targets(&mut self) {
        self.create_dependency_graph();
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            let target = &self.dep_graph.targets[id];
            println!("# {}", target.name);
            println!(
                "{}",
                self.tui
                    .format_command_line(&target.kind.command_line_with_redirects())
            );
            self.scheduler
                .set_finished_and_get_retry_flag(target, false);
            for rdep_id in self.dep_graph.reverse_deps[id].clone() {
                let deps = self.dep_graph.deps.get_mut(rdep_id).unwrap();
                assert!(!deps.is_empty());
                deps.swap_remove(deps.iter().position(|x| *x == id).unwrap());
                if deps.is_empty() {
                    self.dep_graph.waiting.remove(&rdep_id);
                    self.scheduler.push_ready(&self.dep_graph.targets[rdep_id]);
                }
            }
        }
    }

    pub fn show_info(&self, cache_dir: Option<PathBuf>) -> Result<()> {
        let builder = self.targets_builder.as_ref().unwrap();
        let output_directory = self.current_dir.join(&self.out_dir);
        println!("current dir:       {:?}", self.current_dir);
        println!("workspace dir:     {:?}", builder.workspace_dir);
        println!("output directory:  {output_directory:?}");
        let cache_dir = match cache_dir {
            Some(x) => x,
            _ => select_cache_dir(&builder.workspace_dir)?,
        };
        println!("cache directory:   {cache_dir:?}");
        println!("sandbox directory: {:?}", select_sandbox_dir(&cache_dir)?);
        println!("worker threads:    {}", self.worker_threads);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &mut self,
        keep_going: bool,
        verbose: bool,
        group_by_tag: &str,
        cache_dir: Option<PathBuf>,
        remote_cache: Vec<String>,
        remote_cache_threshold: Option<u32>,
        remote_exec: Vec<Url>,
    ) -> Result<SchedulerStats> {
        if self.targets_builder.as_ref().unwrap().targets.is_empty() {
            bail!("No targets added");
        }
        self.tui.verbose = verbose;
        if !remote_exec.is_empty() {
            self.run_remotely(keep_going, group_by_tag, cache_dir, remote_exec)
                .await
        } else {
            self.run_locally(
                keep_going,
                group_by_tag,
                cache_dir,
                remote_cache,
                remote_cache_threshold,
            )
            .await
        }
    }

    #[cfg(not(feature = "remote_exec"))]
    async fn run_remotely(
        &mut self,
        _keep_going: bool,
        _group_by_tag: &str,
        _cache_dir: Option<PathBuf>,
        _remote_exec: Vec<Url>,
    ) -> Result<SchedulerStats> {
        bail!("remote exec feature not enabled");
    }

    async fn run_locally(
        &mut self,
        keep_going: bool,
        group_by_tag: &str,
        cache_dir: Option<PathBuf>,
        remote_cache: Vec<String>,
        remote_cache_threshold: Option<u32>,
    ) -> Result<SchedulerStats> {
        let preparation_start = Instant::now();
        self.prepare_run_locally(cache_dir, remote_cache, remote_cache_threshold)
            .await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut interval = tokio::time::interval(self.tui.get_update_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let execution_start = Instant::now();
        self.start_ready_targets(&tx);
        let mut start_more = true;
        while self.scheduler.running() != 0 {
            tokio::select! {
                Some((id, execution_result, output_files, output_files_cached)) = rx.recv() => {
                    self.on_command_finished(id, &execution_result, output_files, output_files_cached);
                    if execution_result.status == ExecutionStatus::SystemError
                        || (!self.failed.is_empty() && !keep_going)
                    {
                        start_more = false;
                    }
                    if start_more {
                        self.start_ready_targets(&tx);
                    }
                },
                _ = interval.tick() => self.update_status(),
            }
        }
        self.remove_outputs_of_not_run_actions_from_out_dir();
        TmpDirSandbox::cleanup(self.sandbox_dir.as_ref().unwrap());
        self.push_logs_for_not_started_targets();
        let stats = SchedulerStats {
            exec: SchedulerExecStats {
                succeeded: self.succeeded.len(),
                failed: self.failed.len(),
                skipped: self.dep_graph.skipped.len(),
                not_run: self.dep_graph.waiting.len() + self.scheduler.ready(),
            },
            cache_hits: self.cache_hits,
            preparation_duration: execution_start.duration_since(preparation_start),
            execution_duration: execution_start.elapsed(),
        };
        self.tui.finished(&stats);
        self.write_metadata(group_by_tag)
            .context("Failed to write metadata")?;
        Ok(stats)
    }

    async fn prepare_run_locally(
        &mut self,
        cache_dir: Option<PathBuf>,
        remote_cache: Vec<String>,
        remote_cache_threshold: Option<u32>,
    ) -> Result<()> {
        let builder = self.targets_builder.as_ref().unwrap();
        let output_directory = self.current_dir.join(&self.out_dir);
        debug!("current dir:       {:?}", self.current_dir);
        debug!("workspace dir:     {:?}", builder.workspace_dir);
        debug!("output directory:  {output_directory:?}");
        let cache_dir = match cache_dir {
            Some(x) => x,
            _ => select_cache_dir(&builder.workspace_dir)?,
        };
        debug!("cache directory:   {cache_dir:?}");
        let sandbox_dir = select_sandbox_dir(&cache_dir)?;
        let mut cache = Cache::new(cache_dir, self.out_dir.clone())?;
        debug!("sandbox directory: {sandbox_dir:?}");
        debug!("worker threads:    {}", self.worker_threads);
        cache
            .connect_remote_cache(&remote_cache, remote_cache_threshold)
            .await?;
        TmpDirSandbox::cleanup(&sandbox_dir);
        self.cache = Some(cache);
        self.sandbox_dir = Some(sandbox_dir);
        match create_cgroup() {
            Ok(x) => self.cgroup = x,
            Err(e) => debug!("create_cgroup(): {e}"),
        };
        self.create_dependency_graph();
        self.remove_unknown_or_excluded_files_from_out_dir(&self.out_dir)
            .ok();
        self.digest_input_files().await?;
        self.create_output_dirs()?;
        self.create_wasi_modules()?;
        Ok(())
    }

    fn create_dependency_graph(&mut self) {
        assert!(self.dep_graph.targets.is_empty());
        let mut builder = self.targets_builder.take().unwrap();
        assert_eq!(builder.current_dir, self.current_dir);
        assert_eq!(builder.out_dir, self.out_dir);
        self.file_by_path = std::mem::take(&mut builder.file_by_path);
        self.dep_graph = DependencyGraph::from_builder(builder);
        for target in self
            .dep_graph
            .ready
            .iter()
            .map(|id| &self.dep_graph.targets[*id])
        {
            self.scheduler.push_ready(target);
        }
    }

    fn remove_unknown_or_excluded_files_from_out_dir(&self, dir: &Path) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            if let Ok(path) = entry.map(|x| x.path()) {
                if path.is_dir() {
                    // TODO remove whole dir if not known
                    self.remove_unknown_or_excluded_files_from_out_dir(&path)
                        .ok();
                } else {
                    let path_wo_prefix = path.strip_prefix(&self.out_dir).unwrap();
                    if self
                        .file_by_path
                        .get(path_wo_prefix)
                        .map_or(true, |x| self.dep_graph.files[*x].is_excluded)
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
        for target_id in self
            .dep_graph
            .waiting
            .iter()
            .chain(self.scheduler.ready_ids().iter())
        {
            for file_id in &self.dep_graph.targets[*target_id].outputs {
                fs::remove_file(&self.dep_graph.files[*file_id].path).ok();
            }
        }
    }

    async fn digest_input_files(&mut self) -> Result<()> {
        let concurrent = self.worker_threads;
        let (tx, mut rx) = mpsc::channel(concurrent);
        let mut tx_option = Some(tx);
        let mut next_file_id = 0;
        for _ in 0..concurrent {
            self.spawn_digest_input_file(&mut next_file_id, &mut tx_option);
        }
        let mut missing_files = 0;
        while let Some((id, result)) = rx.recv().await {
            match result {
                Ok(digest) => {
                    self.dep_graph.files[id].digest = Some(digest);
                }
                Err(x) => {
                    warn!("{x}");
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
        tx_option: &mut Option<Sender<(FileId, Result<BlobDigest>)>>,
    ) {
        if tx_option.is_none() {
            return;
        }
        loop {
            let id = *next_id;
            *next_id += 1;
            let Some(file) = self.dep_graph.files.get(id) else {
                break;
            };
            if !file.is_excluded && !self.dep_graph.creator_for_file.contains_key(&file.id) {
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

    fn create_output_dirs(&self) -> Result<()> {
        let dirs = self
            .dep_graph
            .targets
            .iter()
            .filter(|x| !x.is_excluded)
            .flat_map(|x| &x.outputs)
            .map(|x| self.dep_graph.files[*x].path.parent().unwrap())
            .sorted_unstable()
            .dedup();
        for x in dirs {
            fs::create_dir_all(x)
                .with_context(|| format!("Failed to create output directory: {x:?}"))?;
        }
        write_gitignore(&self.out_dir);
        Ok(())
    }

    fn create_wasi_modules(&mut self) -> Result<()> {
        let mut engine = None;
        for target in self
            .dep_graph
            .targets
            .iter()
            .filter(|t| matches!(&t.kind, TargetKind::Wasi(_)))
        {
            let executable_id = *target.executables.first().unwrap();
            if let hash_map::Entry::Vacant(x) = self.wasi_module_by_executable.entry(executable_id)
            {
                if engine.is_none() {
                    engine = Some(WasiExecutor::create_engine()?);
                }
                let path = &self.dep_graph.files[executable_id].path;
                let module = WasiExecutor::create_module(engine.as_ref().unwrap(), path)?;
                x.insert(module);
            }
        }
        Ok(())
    }

    fn start_ready_targets(&mut self, tx: &UnboundedSender<ExecutionResultChannel>) {
        while let Some(id) = self.scheduler.pop_ready_and_run() {
            self.start_target(id, tx.clone());
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
            self.scheduler.running() + self.running_remotely,
            self.dep_graph.waiting.len() + self.scheduler.ready(),
        );
        self.tui_dirty = false;
    }

    fn new_sandbox(&self, target: &Target) -> BoxedSandbox {
        match target.kind {
            TargetKind::Wasi(_) => self.new_wasi_sandbox(target),
            _ => self.new_tmp_dir_sandbox(target),
        }
    }

    fn new_tmp_dir_sandbox(&self, target: &Target) -> BoxedSandbox {
        let inputs = chain(target.executables.iter(), target.inputs.iter())
            .map(|x| self.dep_graph.files[*x].path.clone())
            .filter(|x| x.is_relative())
            .collect();
        Box::new(TmpDirSandbox::new(
            self.sandbox_dir.as_ref().unwrap(),
            &target.id.to_string(),
            inputs,
        ))
    }

    fn new_wasi_sandbox(&self, target: &Target) -> BoxedSandbox {
        //let cache = self.cache.as_ref().unwrap();
        let inputs = target
            .inputs
            .iter()
            .map(|x| &self.dep_graph.files[*x])
            // TODO .filter(|x| x.file_type == FileType::OutputFile)
            .map(|x| {
                (
                    x.path.clone(),
                    None, // TODO x.locally_cached.then_some(cache.cas_path(x.digest.as_ref().unwrap())),
                )
            })
            .collect();
        Box::new(WasiSandbox::new(
            self.sandbox_dir.as_ref().unwrap(),
            &target.id.to_string(),
            inputs,
        ))
    }

    fn collect_output_file_paths_for_target(&self, target: &Target) -> Vec<PathBuf> {
        target
            .outputs
            .iter()
            .map(|x| self.dep_graph.files[*x].path.clone())
            .collect()
    }

    /// Execute a target in a worker thread with caching.
    ///
    /// If the executed target failed, action_result will be None and the action will not be cached.
    fn start_target(&mut self, id: TargetId, tx: UnboundedSender<ExecutionResultChannel>) {
        let total_duration_start = Instant::now();
        let target = &self.dep_graph.targets[id];
        assert_eq!(self.dep_graph.deps[id].len(), 0);
        let executor = self.new_executor(target);
        let (bzl_command, bzl_input_root) = bazel_remote_exec::bzl_action_for_target(
            target,
            &self.dep_graph.files,
            executor.digest(),
        );
        let no_cache_tag = target.tags.contains(&Tag::NoCache);
        let cache = (!no_cache_tag).then(|| self.cache.as_ref().unwrap().clone());
        let read_cache = self.read_cache;
        let use_remote_cache = cache.is_some() && !target.tags.contains(&Tag::NoRemoteCache);
        // make sure output files are written on the same mountpoint as local cache to speed up moving files into cache
        let use_sandbox = !target.outputs.is_empty();
        let sandbox = (use_sandbox && !target.tags.contains(&Tag::NoSandbox))
            .then(|| self.new_sandbox(target));
        let output_paths = self.collect_output_file_paths_for_target(target);
        let cwd = self.current_dir.clone();
        let out_dir = self.out_dir.clone();
        tokio::task::spawn(async move {
            let use_cache = cache.is_some();
            let action = bazel_remote_exec::Action {
                command_digest: Some(BazelDigest::for_message(&bzl_command)),
                input_root_digest: Some(BazelDigest::for_message(&bzl_input_root)),
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
                &cwd,
                &out_dir,
            )
            .await
            .unwrap_or_else(|e| {
                (
                    ExecutionResult {
                        status: ExecutionStatus::SystemError,
                        error: Some(e.to_string()),
                        ..Default::default()
                    },
                    Default::default(),
                )
            });
            execution_result.total_duration = Some(total_duration_start.elapsed());
            let output_files_cached = use_cache && execution_result.success();
            // ignore SendError - channel might be closed if a previous target failed
            tx.send((id, execution_result, output_files, output_files_cached))
                .ok();
        });
    }

    fn new_executor(&self, target: &Target) -> Executor {
        match &target.kind {
            TargetKind::Command(c) => self.new_command_executor(target, c),
            TargetKind::Wasi(c) => self.new_wasi_executor(target, c),
            TargetKind::Task(t) => Executor::Task(TaskExecutor::new(t.task.clone())),
            TargetKind::HttpRemoteExecTask(t) => self.new_http_remote_executor(t),
        }
    }

    fn new_command_executor(&self, target: &Target, command: &CommandTarget) -> Executor {
        let timeout = target.tags.iter().find_map(|t| {
            if let Tag::Timeout(x) = t {
                Some(*x)
            } else {
                None
            }
        });
        Executor::Command(CommandExecutor::new(
            command.clone(),
            timeout,
            self.cgroup.clone(),
        ))
    }

    fn new_wasi_executor(&self, target: &Target, command: &CommandTarget) -> Executor {
        let executable_id = *target.executables.first().unwrap();
        let module = self.wasi_module_by_executable[&executable_id].clone();
        let mut read_dirs = vec![];
        for dir in target.inputs.iter().map(|id| {
            self.dep_graph.files[*id]
                .path
                .parent()
                .unwrap()
                .to_path_buf()
        }) {
            if !read_dirs.contains(&dir) {
                read_dirs.push(dir);
            }
        }
        let write_dir = (target.outputs.len()
            - command.stdout_file.is_some() as usize
            - command.stderr_file.is_some() as usize)
            != 0;
        Executor::Wasi(WasiExecutor::new(
            command.clone(),
            module,
            read_dirs,
            write_dir,
        ))
    }

    fn new_http_remote_executor(&self, task: &TaskTarget) -> Executor {
        let Task::HttpRemoteExec(task) = &task.task else {
            unreachable!()
        };
        Executor::HttpRemote(HttpRemoteExecutor::new(task, &self.http_remote_exec_state))
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
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>)> {
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
                cwd,
                out_dir,
            )
            .await
            .context("exec_action_without_sandbox()")?
        };
        if let Some(cache) = cache.as_ref().filter(|_| execution_result.success()) {
            let output_files = output_files
                .iter()
                .map(|f| File {
                    id: 0, // doesn't matter here
                    path: PathBuf::from(f.path.clone()),
                    digest: Some(f.digest.as_ref().unwrap().into()),
                    executable: if f.is_executable {
                        Some(ExecutableType::ExecutableInWorkspace)
                    } else {
                        None
                    },
                    is_excluded: false,
                })
                .collect();
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
                signal: None,
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
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>)> {
        sandbox
            .create(output_paths)
            .await
            .context("Sandbox::create()")?;
        let sandbox_dir = sandbox.dir();
        let execution_result = executor.exec(cwd, &sandbox_dir).await;
        let output_files = if execution_result.success() {
            Self::new_output_files_with_digest(&sandbox_dir, out_dir, output_paths).await?
        } else {
            Default::default()
        };
        if execution_result.success() {
            if let Some(cache) = cache {
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_files.clone(),
                    &sandbox_dir,
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
        cwd: &Path,
        out_dir: &PathBuf,
    ) -> Result<(ExecutionResult, Vec<OutputFile>)> {
        // remove expected output files, because symlinks will not be overwritten
        for x in output_paths {
            force_remove_file(x).await?;
        }
        let sandbox_dir = SandboxDir::new(None);
        let execution_result = executor.exec(cwd, &sandbox_dir).await;
        let output_files = if execution_result.success() {
            Self::new_output_files_with_digest(&sandbox_dir, out_dir, output_paths).await?
        } else {
            Default::default()
        };
        if let Some(cache) = cache.filter(|_| execution_result.success()) {
            Self::cache_action_result(
                action_digest,
                &execution_result,
                output_files.clone(),
                &sandbox_dir,
                cache,
                use_remote_cache,
            )
            .await
            .with_context(|| "cache_action_result()")?;
        }
        Ok((execution_result, output_files))
    }

    async fn new_output_files_with_digest(
        sandbox_dir: &SandboxDir,
        out_dir: &PathBuf,
        output_paths: &Vec<PathBuf>,
    ) -> Result<Vec<OutputFile>> {
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
        sandbox_dir: &SandboxDir,
        out_dir: &PathBuf,
        exec_path: &PathBuf,
    ) -> Result<OutputFile> {
        let src = sandbox_dir.join(exec_path);
        if src.is_symlink() {
            bail!("Output file must not be a symlink: {:?}", src);
        }
        let file = tokio::fs::File::open(&src)
            .await
            .with_context(|| format!("Failed to open: {src:?}"))?;
        let is_executable = is_file_executable(&file)
            .await
            .with_context(|| format!("is_file_executable(): {src:?}"))?;
        let digest = BazelDigest::for_file(file)
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
        sandbox_dir: &SandboxDir,
        cache: &mut Cache,
        use_remote_cache: bool,
    ) -> Result<Vec<OutputFile>> {
        assert!(execution_result.success());
        let mut action_result = ActionResult {
            output_files,
            exit_code: execution_result.exit_code.unwrap_or_default(),
            execution_metadata: Some(ExecutedActionMetadata {
                virtual_execution_duration: execution_result.exec_duration.map(|x| {
                    bazel_remote_exec::Duration {
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
        id: TargetId,
        execution_result: &ExecutionResult,
        output_files: Vec<OutputFile>,
        output_files_cached: bool,
    ) {
        let target = &self.dep_graph.targets[id];
        let retry = self
            .scheduler
            .set_finished_and_get_retry_flag(target, execution_result.out_of_memory_killed());
        if retry {
            self.on_command_retry(id, execution_result);
        } else {
            let measurements = self.measurements.collect(&target.name, execution_result);
            self.profile.collect(target, execution_result);
            let output_size = output_files
                .iter()
                .map(|x| x.digest.as_ref().unwrap().size_bytes as u64)
                .sum::<u64>()
                + execution_result.stdout.len() as u64
                + execution_result.stderr.len() as u64;
            self.log_file
                .push(target, execution_result, Some(output_size), measurements);
            if execution_result.success() {
                self.set_output_file_digests(output_files, output_files_cached);
                self.on_command_succeeded(id, execution_result);
            } else if target.tags.contains(&Tag::Condition) {
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
            let file = &mut self.dep_graph.files[self.file_by_path[&path]];
            assert!(file.digest.is_none());
            file.digest = Some(output_file.digest.unwrap().into());
            if output_files_cached {
                // TODO file.locally_cached = true;
            }
        }
    }

    /// Track state and check if reverse dependencies are ready
    fn on_command_succeeded(&mut self, id: TargetId, execution_result: &ExecutionResult) {
        self.succeeded.push(id);
        if execution_result.cache_hit.is_some() {
            self.cache_hits += 1;
        }
        let target = &self.dep_graph.targets[id];
        self.tui.target_succeeded(target, execution_result);
        for ready_id in self.dep_graph.set_succeeded(id) {
            let ready = &self.dep_graph.targets[ready_id];
            self.scheduler.push_ready(ready);
        }
    }

    fn on_command_retry(&mut self, id: TargetId, execution_result: &ExecutionResult) {
        let target = &self.dep_graph.targets[id];
        self.tui.target_retry(target, execution_result);
    }

    fn on_command_failed(&mut self, id: TargetId, execution_result: &ExecutionResult) {
        self.failed.push(id);
        let target = &self.dep_graph.targets[id];
        self.tui.target_failed(target, execution_result);
    }

    fn on_condition_failed(&mut self, id: TargetId, execution_result: &ExecutionResult) {
        let target = &self.dep_graph.targets[id];
        self.tui.target_failed(target, execution_result);
        for skipped_id in self.dep_graph.set_failed(id) {
            let skipped = &self.dep_graph.targets[skipped_id];
            self.log_file
                .push_not_run(skipped, ExecutionStatus::Skipped);
        }
    }

    fn push_logs_for_not_started_targets(&mut self) {
        assert_eq!(self.scheduler.running(), 0);
        for id in self
            .dep_graph
            .waiting
            .iter()
            .chain(self.scheduler.ready_ids().iter())
        {
            self.log_file
                .push_not_run(&self.dep_graph.targets[*id], ExecutionStatus::NotStarted);
        }
    }

    fn write_metadata(&self, group_by_tag: &str) -> Result<()> {
        let dir = self.out_dir.join("razel-metadata");
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create metadata directory: {dir:?}"))?;
        self.dep_graph
            .write_graphs_html(self.excluded_targets_len, &dir.join("graphs.html"))?;
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

impl RazelJsonHandler for Razel {
    /// Set the directory to resolve relative paths of input/output files
    fn set_workspace_dir(&mut self, dir: &Path) {
        self.targets_builder
            .as_mut()
            .unwrap()
            .set_workspace_dir(dir);
    }

    fn push_json(&mut self, json: RazelJson) -> Result<TargetId> {
        self.targets_builder.as_mut().unwrap().push_json(json)
    }
}

mod filter;
mod import;
#[cfg(feature = "remote_exec")]
mod remote_exec;
mod system;

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;
    use serial_test::serial;

    use crate::types::RazelJsonCommand;
    use crate::{Razel, SchedulerExecStats};

    /// Test that targets are actually run in parallel limited by Scheduler::worker_threads
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
                .push_json_command(RazelJsonCommand {
                    name: format!("{i}"),
                    executable: "cmake".into(),
                    args: vec!["-E".into(), "sleep".into(), sleep_duration.to_string()],
                    env: Default::default(),
                    inputs: vec![],
                    outputs: vec![],
                    stdout: None,
                    stderr: None,
                    deps: vec![],
                    tags: vec![],
                })
                .unwrap();
        }
        let stats = razel
            .run(false, true, "", None, vec![], None, vec![])
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
