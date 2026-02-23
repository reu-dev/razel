use crate::{QueueMsg, Tx};
use anyhow::{Context, Result, bail};
use itertools::chain;
use razel::cache::{Cache, MessageDigest};
use razel::config::LinkType;
use razel::executors::{
    CommandExecutor, ExecutionResult, ExecutionStatus, Executor, SharedWasiExecutorState,
    TaskExecutor, WasiExecutor,
};
use razel::remote_exec::{ExecuteTargetResult, JobId};
use razel::types::{CommandTarget, Digest, ExecutableType, File, Tag, Target, TargetKind};
use razel::{
    BoxedSandbox, CGroup, TmpDirSandbox, WasiSandbox, WorkspaceDirSandbox, bazel_remote_exec,
    exec_action_with_sandbox, get_execution_result_from_cache,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, instrument};

/// Worker running within the server process
pub struct JobWorker {
    job_id: JobId,
    cache: Cache,
    ws_dir: PathBuf,
    sandbox_dir: PathBuf,
    created_dirs: HashSet<PathBuf>,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    wasi_state: SharedWasiExecutorState,
}

impl JobWorker {
    pub fn new(job_id: JobId, storage: &Path) -> Result<Self> {
        let cache_dir = storage.join("cache");
        let job_dir = storage.join(format!("job-{}", job_id.as_u128()));
        let ws_dir = job_dir.join("ws");
        let sandbox_dir = job_dir.join("sandbox");
        debug!("cache directory:   {cache_dir:?}");
        debug!("job directory:     {job_dir:?}");
        debug!("sandbox directory: {sandbox_dir:?}");
        let cache = Cache::new(cache_dir, ws_dir.clone())?;
        Ok(Self {
            job_id,
            cache,
            ws_dir,
            sandbox_dir,
            created_dirs: Default::default(),
            cgroup: None,
            wasi_state: SharedWasiExecutorState::new(),
        })
    }

    // TODO move to worker thread, drop async
    #[instrument(skip_all)]
    pub async fn link_input_file_into_ws_dir(
        &self,
        digest: &Digest,
        file_path: &PathBuf,
    ) -> Result<()> {
        if file_path.is_absolute() {
            bail!("link_input_file_into_ws_dir: path must be relative: {file_path:?}");
        }
        let cas_path = self.cache.cas_path(digest);
        let ws_path = self.ws_dir.join(file_path);
        tracing::trace!(?cas_path, ?ws_path);
        let link_type = LinkType::Symlink; // TODO move to config file
        match link_type {
            LinkType::Hardlink => razel::force_hardlink(&cas_path, &ws_path).await?,
            LinkType::Symlink => razel::force_symlink(&cas_path, &ws_path).await?,
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn push_target(&mut self, target: &Target, files: &Vec<File>, tx: Tx) {
        let total_duration_start = Instant::now();
        let job_id = self.job_id;
        let target_id = target.id;
        tracing::debug!(target_id, target = target.name);
        self.create_dirs(target, files).unwrap(); // TODO move to worker thread and add error handling
        let executor = self.new_executor(target, files);
        let (bzl_command, bzl_input_root) =
            bazel_remote_exec::bzl_action_for_target(target, files, executor.digest());
        let no_cache_tag = target.tags.contains(&Tag::NoCache);
        let cache = (!no_cache_tag).then(|| self.cache.clone());
        let use_remote_cache = cache.is_some() && !target.tags.contains(&Tag::NoRemoteCache);
        let sandbox = self.new_sandbox(target, files);
        let mut output_files = self.collect_output_files(target, files);
        let cwd = self.ws_dir.clone();
        tokio::task::spawn(async move {
            let action = bazel_remote_exec::Action {
                command_digest: Some(bazel_remote_exec::BazelDigest::for_message(&bzl_command)),
                input_root_digest: Some(bazel_remote_exec::BazelDigest::for_message(
                    &bzl_input_root,
                )),
                ..Default::default()
            };
            let action_digest = Digest::for_message(&action);
            let mut execution_result = Self::exec_action(
                &action_digest,
                cache,
                use_remote_cache,
                &executor,
                &mut output_files,
                sandbox,
                &cwd,
            )
            .await
            .unwrap_or_else(|e| ExecutionResult {
                status: ExecutionStatus::SystemError,
                error: Some(format!("{e:?}")),
                ..Default::default()
            });
            execution_result.total_duration = Some(total_duration_start.elapsed());
            if !execution_result.success() {
                output_files.clear();
            }
            // ignore SendError - channel might be closed if a previous target failed
            tx.send(QueueMsg::ExecuteTargetResult(ExecuteTargetResult {
                job_id,
                target_id,
                action_digest,
                result: execution_result,
                output_files,
            }))
            .ok();
        });
    }

    fn new_sandbox(&self, target: &Target, files: &[File]) -> BoxedSandbox {
        let no_sandbox_tag = target.tags.contains(&Tag::NoSandbox);
        match target.kind {
            TargetKind::Command(_) if no_sandbox_tag => {
                Box::new(WorkspaceDirSandbox::new(self.ws_dir.clone()))
            }
            TargetKind::Command(_) => self.new_tmp_dir_sandbox(target, files),
            TargetKind::Wasi(_) => self.new_wasi_sandbox(target, files),
            TargetKind::Task(_) | TargetKind::HttpRemoteExecTask(_) => {
                Box::new(WorkspaceDirSandbox::new(self.ws_dir.clone()))
            }
        }
    }

    fn new_tmp_dir_sandbox(&self, target: &Target, files: &[File]) -> BoxedSandbox {
        let inputs = chain(target.executables.iter(), target.inputs.iter())
            .map(|x| &files[*x].path)
            .filter(|x| x.is_relative())
            .cloned()
            .collect();
        Box::new(TmpDirSandbox::new(
            self.ws_dir.clone(),
            &self.sandbox_dir,
            &target.id.to_string(),
            inputs,
        ))
    }

    fn new_wasi_sandbox(&self, target: &Target, files: &[File]) -> BoxedSandbox {
        //let cache = &self.cache;
        let inputs = target
            .inputs
            .iter()
            .map(|x| &files[*x])
            // TODO .filter(|x| x.file_type == FileType::OutputFile)
            .map(|x| {
                (
                    x.path.clone(),
                    None, // TODO x.locally_cached.then_some(cache.cas_path(x.digest.as_ref().unwrap())),
                )
            })
            .collect();
        Box::new(WasiSandbox::new(
            self.ws_dir.clone(),
            &self.sandbox_dir,
            &target.id.to_string(),
            inputs,
        ))
    }

    fn create_dirs(&mut self, target: &Target, files: &[File]) -> Result<()> {
        for file in chain!(&target.executables, &target.inputs, &target.outputs).map(|x| &files[*x])
        {
            assert!(!file.is_excluded);
            match file.executable {
                Some(ExecutableType::SystemExecutable) => continue,
                Some(ExecutableType::RazelExecutable) => unreachable!(),
                _ => assert!(file.path.is_relative()),
            }
            let rel_dir = file.path.parent().unwrap();
            if !self.created_dirs.contains(rel_dir) {
                let abs_dir = self.ws_dir.join(rel_dir);
                std::fs::create_dir_all(&abs_dir)
                    .with_context(|| format!("failed to create dir: {abs_dir:?}"))?;
                self.created_dirs.insert(rel_dir.to_path_buf());
            }
        }
        Ok(())
    }

    fn collect_output_files(&self, target: &Target, files: &[File]) -> Vec<File> {
        target.outputs.iter().map(|x| files[*x].clone()).collect()
    }

    fn new_executor(&self, target: &Target, files: &[File]) -> Executor {
        match &target.kind {
            TargetKind::Command(c) => self.new_command_executor(target, c),
            TargetKind::Wasi(c) => self.new_wasi_executor(target, files, c),
            TargetKind::Task(t) => Executor::Task(TaskExecutor::new(t.task.clone())),
            TargetKind::HttpRemoteExecTask(_) => todo!(),
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

    fn new_wasi_executor(
        &self,
        target: &Target,
        files: &[File],
        command: &CommandTarget,
    ) -> Executor {
        let mut read_dirs = vec![];
        for dir in target
            .inputs
            .iter()
            .map(|id| files[*id].path.parent().unwrap().to_path_buf())
        {
            if !read_dirs.contains(&dir) {
                read_dirs.push(dir);
            }
        }
        let write_dir = (target.outputs.len()
            - command.stdout_file.is_some() as usize
            - command.stderr_file.is_some() as usize)
            != 0;
        Executor::Wasi(WasiExecutor::new(
            self.wasi_state.clone(),
            command.clone(),
            read_dirs,
            write_dir,
        ))
    }

    async fn exec_action(
        action_digest: &MessageDigest,
        mut cache: Option<Cache>,
        use_remote_cache: bool,
        executor: &Executor,
        output_files: &mut Vec<File>,
        sandbox: BoxedSandbox,
        cwd: &Path,
    ) -> Result<ExecutionResult> {
        let execution_result = if let Some(x) = get_execution_result_from_cache(
            action_digest,
            cache.as_mut(),
            use_remote_cache,
            output_files,
        )
        .await
        {
            x
        } else {
            exec_action_with_sandbox(
                action_digest,
                cache.as_mut(),
                use_remote_cache,
                executor,
                sandbox,
                output_files,
                cwd,
            )
            .await
            .context("exec_action_with_sandbox()")?
        };
        if let Some(cache) = cache.as_ref().filter(|_| execution_result.success()) {
            cache
                .link_output_files_into_out_dir(output_files)
                .await
                .context("link_output_files_into_out_dir()")?;
        }
        Ok(execution_result)
    }
}
