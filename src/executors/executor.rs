use crate::CGroup;
use anyhow::Error;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use crate::executors::{AsyncTaskExecutor, BlockingTaskExecutor, CustomCommandExecutor};

#[derive(Clone)]
pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    AsyncTask(AsyncTaskExecutor),
    BlockingTask(BlockingTaskExecutor),
}

impl Executor {
    pub async fn exec(
        &self,
        sandbox_dir: Option<PathBuf>,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        match self {
            Executor::CustomCommand(c) => c.exec(sandbox_dir, cgroup).await,
            Executor::AsyncTask(x) => x.exec(sandbox_dir).await,
            Executor::BlockingTask(t) => t.exec().await,
        }
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        match self {
            Executor::CustomCommand(c) => c.args_with_executable(),
            Executor::AsyncTask(x) => x.args_with_executable(),
            Executor::BlockingTask(t) => t.args_with_executable(),
        }
    }

    pub fn command_line_with_redirects(&self) -> Vec<String> {
        match self {
            Executor::CustomCommand(c) => c.command_line_with_redirects(),
            Executor::AsyncTask(x) => x.args_with_executable(),
            Executor::BlockingTask(t) => t.args_with_executable(),
        }
    }

    pub fn env(&self) -> Option<&HashMap<String, String>> {
        match self {
            Executor::CustomCommand(x) => Some(&x.env),
            Executor::AsyncTask(_) => None,
            Executor::BlockingTask(_) => None,
        }
    }

    /// Returns if a sandbox should be used.
    ///
    /// Internally implemented tasks have well defined inputs and outputs. This might not be true
    /// for other commands, therefore the sandbox must be used to make caching reliable.
    pub fn use_sandbox(&self) -> bool {
        match self {
            Executor::CustomCommand(_) => true,
            Executor::AsyncTask(_) => true,
            Executor::BlockingTask(_) => false,
        }
    }
}

#[derive(Debug, Default)]
pub struct ExecutionResult {
    pub status: ExecutionStatus,
    pub exit_code: Option<i32>,
    pub error: Option<anyhow::Error>,
    pub cache_hit: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: Option<Duration>,
}

impl ExecutionResult {
    pub fn for_task(result: Result<(), Error>, execution_start: Instant) -> Self {
        match result {
            Ok(()) => Self {
                status: ExecutionStatus::Success,
                exit_code: Some(0),
                duration: Some(execution_start.elapsed()),
                ..Default::default()
            },
            Err(e) => Self {
                status: ExecutionStatus::Failed,
                error: Some(e),
                ..Default::default()
            },
        }
    }

    pub fn success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    NotStarted,
    FailedToStart,
    FailedToCreateResponseFile,
    FailedToWriteStdoutFile,
    FailedToWriteStderrFile,
    Failed,
    /// SIGSEGV
    Crashed,
    /// SIGTERM/SIGKILL, e.g. killed by OOM killer
    Killed,
    Timeout,
    Success,
    /// not command related error, e.g. cache, sandbox
    SystemError,
}

impl Default for ExecutionStatus {
    fn default() -> Self {
        Self::NotStarted
    }
}
