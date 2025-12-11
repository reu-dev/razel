use crate::{QueueMsg, Tx};
use anyhow::{bail, Context, Result};
use razel::cache::{Cache, MessageDigest};
use razel::executors::{
    CommandExecutor, ExecutionResult, ExecutionStatus, Executor, SharedWasiExecutorState,
    TaskExecutor, WasiExecutor,
};
use razel::remote_exec::{ExecuteTargetResult, JobId};
use razel::types::{CommandTarget, Digest, ExecutableType, File, Tag, Target, TargetKind};
use razel::{
    bazel_remote_exec, force_remove_file, is_file_executable, BoxedSandbox, CGroup, SandboxDir,
    TmpDirSandbox, WasiSandbox,
};
use std::iter::chain;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::log::debug;

/// Worker running within the server process
pub struct JobWorker {
    job_id: JobId,
    #[allow(dead_code)]
    max_parallelism: usize,
    cache: Cache,
    current_dir: PathBuf,
    sandbox_dir: PathBuf,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    wasi_state: SharedWasiExecutorState,
}

impl JobWorker {
    pub fn new(job_id: JobId, max_parallelism: usize, storage: &Path) -> Result<Self> {
        let job_dir = storage.join(format!("job-{}", job_id.as_u128()));
        let current_dir = job_dir.join("ws");
        let out_dir = PathBuf::new();
        let cache_dir = job_dir.join("cache");
        let sandbox_dir = job_dir.join("sandbox");
        debug!("job directory:     {job_dir:?}");
        debug!("cache directory:   {cache_dir:?}");
        debug!("sandbox directory: {sandbox_dir:?}");
        let cache = Cache::new(cache_dir, out_dir.clone())?;
        Ok(Self {
            job_id,
            max_parallelism,
            cache,
            current_dir,
            sandbox_dir,
            cgroup: None,
            wasi_state: SharedWasiExecutorState::new(),
        })
    }

    pub fn push_target(&mut self, target: &Target, files: &Vec<File>, tx: Tx) {
        let total_duration_start = Instant::now();
        let job_id = self.job_id;
        let target_id = target.id;
        let executor = self.new_executor(target, files);
        let (bzl_command, bzl_input_root) =
            bazel_remote_exec::bzl_action_for_target(target, files, executor.digest());
        let no_cache_tag = target.tags.contains(&Tag::NoCache);
        let cache = (!no_cache_tag).then(|| self.cache.clone());
        let use_remote_cache = cache.is_some() && !target.tags.contains(&Tag::NoRemoteCache);
        let use_sandbox = match target.kind {
            TargetKind::Command(_) | TargetKind::Wasi(_) => true,
            TargetKind::Task(_) | TargetKind::HttpRemoteExecTask(_) => false,
        };
        let sandbox = (use_sandbox && !target.tags.contains(&Tag::NoSandbox))
            .then(|| self.new_sandbox(target, files));
        let mut output_files = self.collect_output_files(target, files);
        let cwd = self.current_dir.clone();
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
        match target.kind {
            TargetKind::Wasi(_) => self.new_wasi_sandbox(target, files),
            _ => self.new_tmp_dir_sandbox(target, files),
        }
    }

    fn new_tmp_dir_sandbox(&self, target: &Target, files: &[File]) -> BoxedSandbox {
        let inputs = chain(target.executables.iter(), target.inputs.iter())
            .map(|x| files[*x].path.clone())
            .filter(|x| x.is_relative())
            .collect();
        Box::new(TmpDirSandbox::new(
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
            &self.sandbox_dir,
            &target.id.to_string(),
            inputs,
        ))
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
        sandbox: Option<BoxedSandbox>,
        cwd: &Path,
    ) -> Result<ExecutionResult> {
        let execution_result = if let Some(x) = Self::get_action_from_cache(
            action_digest,
            cache.as_mut(),
            use_remote_cache,
            output_files,
        )
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
                output_files,
                cwd,
            )
            .await
            .context("exec_action_with_sandbox()")?
        } else {
            Self::exec_action_without_sandbox(
                action_digest,
                cache.as_mut(),
                use_remote_cache,
                executor,
                output_files,
                cwd,
            )
            .await
            .context("exec_action_without_sandbox()")?
        };
        if let Some(cache) = cache.as_ref().filter(|_| execution_result.success()) {
            cache
                .link_output_files_into_out_dir(output_files)
                .await
                .context("symlink_output_files_into_out_dir()")?;
        }
        Ok(execution_result)
    }

    async fn get_action_from_cache(
        action_digest: &MessageDigest,
        cache: Option<&mut Cache>,
        use_remote_cache: bool,
        output_files: &mut [File],
    ) -> Option<ExecutionResult> {
        let cache = cache?;
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
            for result_file in action_result.output_files {
                let Some(file) = output_files
                    .iter_mut()
                    .find(|f| f.path.to_str().unwrap() == result_file.path)
                else {
                    panic!("unexpected file in action_result: {:?}", result_file.path);
                };
                file.digest = Some(result_file.digest.unwrap().into());
                set_file_executable(file, result_file.is_executable);
            }
            return Some(execution_result);
        }
        None
    }

    async fn exec_action_with_sandbox(
        action_digest: &Digest,
        cache: Option<&mut Cache>,
        use_remote_cache: bool,
        executor: &Executor,
        sandbox: BoxedSandbox,
        output_files: &mut Vec<File>,
        cwd: &Path,
    ) -> Result<ExecutionResult> {
        sandbox
            .create(
                &output_files
                    .iter()
                    .map(|f| f.path.clone())
                    .collect::<Vec<_>>(),
            )
            .await
            .context("Sandbox::create()")?;
        let sandbox_dir = sandbox.dir();
        let execution_result = executor.exec(cwd, &sandbox_dir).await;
        if execution_result.success() {
            set_output_files_digest(output_files, &sandbox_dir).await?;
            if let Some(cache) = cache {
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_files,
                    &sandbox_dir,
                    cache,
                    use_remote_cache,
                )
                .await
                .with_context(|| "cache_action_result()")?;
            } else {
                sandbox
                    .move_output_files_into_out_dir(
                        &output_files
                            .iter()
                            .map(|f| f.path.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await?;
            }
        }
        sandbox
            .destroy()
            .await
            .with_context(|| "Sandbox::destroy()")?;
        Ok(execution_result)
    }

    async fn exec_action_without_sandbox(
        action_digest: &MessageDigest,
        cache: Option<&mut Cache>,
        use_remote_cache: bool,
        executor: &Executor,
        output_files: &mut Vec<File>,
        cwd: &Path,
    ) -> Result<ExecutionResult> {
        // remove expected output files, because symlinks will not be overwritten
        for file in output_files.iter_mut() {
            force_remove_file(&file.path).await?;
        }
        let sandbox_dir = SandboxDir::new(None);
        let execution_result = executor.exec(cwd, &sandbox_dir).await;
        if execution_result.success() {
            set_output_files_digest(output_files, &sandbox_dir).await?;
            if let Some(cache) = cache {
                Self::cache_action_result(
                    action_digest,
                    &execution_result,
                    output_files,
                    &sandbox_dir,
                    cache,
                    use_remote_cache,
                )
                .await
                .with_context(|| "cache_action_result()")?;
            }
        }
        Ok(execution_result)
    }

    async fn cache_action_result(
        action_digest: &MessageDigest,
        execution_result: &ExecutionResult,
        output_files: &[File],
        sandbox_dir: &SandboxDir,
        cache: &mut Cache,
        use_remote_cache: bool,
    ) -> Result<()> {
        assert!(execution_result.success());
        let mut action_result = bazel_remote_exec::ActionResult {
            output_files: output_files
                .iter()
                .map(|f| bazel_remote_exec::OutputFile {
                    path: f.path.to_str().unwrap().into(),
                    digest: Some(f.digest.as_ref().unwrap().into()),
                    is_executable: f.executable.is_some(),
                    ..Default::default()
                })
                .collect(),
            exit_code: execution_result.exit_code.unwrap_or_default(),
            execution_metadata: Some(bazel_remote_exec::ExecutedActionMetadata {
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
        Ok(())
    }
}

async fn set_output_files_digest(files: &mut Vec<File>, sandbox_dir: &SandboxDir) -> Result<()> {
    for file in files {
        set_output_file_digest(file, sandbox_dir).await?
    }
    Ok(())
}

async fn set_output_file_digest(file: &mut File, sandbox_dir: &SandboxDir) -> Result<()> {
    let src = sandbox_dir.join(&file.path);
    if src.is_symlink() {
        bail!("Output file must not be a symlink: {:?}", src);
    }
    let fs_file = tokio::fs::File::open(&src)
        .await
        .with_context(|| format!("Failed to open: {src:?}"))?;
    set_file_executable(
        file,
        is_file_executable(&fs_file)
            .await
            .with_context(|| format!("is_file_executable(): {src:?}"))?,
    );
    file.digest = Some(
        Digest::for_file(fs_file)
            .await
            .with_context(|| format!("Digest::for_file(): {src:?}"))?,
    );
    Ok(())
}

fn set_file_executable(file: &mut File, flag: bool) {
    file.executable = match flag {
        true if file.path.ends_with(".wasm") => Some(ExecutableType::WasiModule),
        true => Some(ExecutableType::ExecutableInWorkspace),
        false => None,
    };
}
