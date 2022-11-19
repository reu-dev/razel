use crate::CGroup;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::executors::{CustomCommandExecutor, TaskExecutor};

#[derive(Clone)]
pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Task(TaskExecutor),
}

impl Executor {
    pub async fn exec(
        &self,
        sandbox_dir: Option<PathBuf>,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        match self {
            Executor::CustomCommand(c) => c.exec(sandbox_dir, cgroup).await,
            Executor::Task(t) => t.exec().await,
        }
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        match self {
            Executor::CustomCommand(c) => c.args_with_executable(),
            Executor::Task(t) => t.args_with_executable(),
        }
    }

    pub fn env(&self) -> Option<&HashMap<String, String>> {
        match self {
            Executor::CustomCommand(x) => Some(&x.env),
            Executor::Task(_) => None,
        }
    }

    /// Returns if a sandbox should be used.
    ///
    /// Internally implemented tasks have well defined inputs and outputs. This might not be true
    /// for other commands, therefore the sandbox must be used to make caching reliable.
    pub fn use_sandbox(&self) -> bool {
        match self {
            Executor::CustomCommand(_) => true,
            Executor::Task(_) => false,
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
    pub fn success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExecutionStatus {
    NotStarted,
    FailedToStart,
    FailedToCreateResponseFile,
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
