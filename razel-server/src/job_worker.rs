use anyhow::Result;
use razel::cache::Cache;
use razel::executors::{CommandExecutor, Executor, TaskExecutor, WasiExecutor};
use razel::remote_exec::JobId;
use razel::types::{CommandTarget, File, FileId, Tag, Target, TargetKind};
use razel::{bazel_remote_exec, CGroup};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::log::debug;

type Group = String;
/*
struct ReadyItem {
    job: JobId,
    target: TargetId,
    group: Group,
    slots: usize,
}
ReadyItem {
job: job.id,
target: target.id,
group: Self::group_for_command(target),
slots: job.slots_for_group(&group),
}
 */

/// Worker running within the server process
pub struct JobWorker {
    job_id: JobId,
    max_parallelism: usize,
    cache: Cache,
    sandbox_dir: PathBuf,
    /// single Linux cgroup for all commands to trigger OOM killer
    cgroup: Option<CGroup>,
    wasi_module_by_executable: HashMap<FileId, wasmtime::Module>,
    /// groups targets by estimated resource requirement
    group_to_slots: HashMap<String, usize>,
}

impl JobWorker {
    pub fn new(job_id: JobId, max_parallelism: usize, storage: &Path) -> Result<Self> {
        let cache_dir = storage.join("cache").join(job_id.as_u128().to_string());
        let sandbox_dir = storage.join("sandbox").join(job_id.as_u128().to_string());
        debug!("cache directory:   {cache_dir:?}");
        debug!("sandbox directory: {sandbox_dir:?}");
        // TODO out-dir is not needed
        let cache = Cache::new(cache_dir, storage.join("out"))?;
        Ok(Self {
            job_id,
            max_parallelism,
            cache,
            sandbox_dir,
            cgroup: None,
            wasi_module_by_executable: Default::default(),
            group_to_slots: Default::default(),
        })
    }

    pub fn push_target(&mut self, target: &Target, files: &Vec<File>) {
        let total_duration_start = Instant::now();
        let executor = self.new_executor(target, files);
        let (bzl_command, bzl_input_root) =
            bazel_remote_exec::bzl_action_for_target(target, files, executor.digest());
        todo!();
        /*
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
         */
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
        let executable_id = *target.executables.first().unwrap();
        let module = self.wasi_module_by_executable[&executable_id].clone();
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
            command.clone(),
            module,
            read_dirs,
            write_dir,
        ))
    }

    fn group_for_command(target: &Target) -> Group {
        // assume resource requirements depends just on executable
        // could also use the command line with file arguments stripped
        match &target.kind {
            TargetKind::Command(c) => c.executable.clone(),
            TargetKind::Wasi(c) => c.executable.clone(),
            TargetKind::Task(_) => String::new(),
            TargetKind::HttpRemoteExecTask(_) => String::new(),
        }
    }

    fn slots_for_group(&self, group: &Group) -> usize {
        *self.group_to_slots.get(group).unwrap_or(&1)
    }
}
