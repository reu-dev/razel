use crate::executors::{CustomCommandExecutor, TaskExecutor};

#[derive(Clone)]
pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Task(TaskExecutor),
}

impl Executor {
    pub async fn exec(&self) -> ExecutionResult {
        match self {
            Executor::CustomCommand(c) => c.exec().await,
            Executor::Task(t) => t.exec().await,
        }
    }

    pub fn command_line(&self) -> String {
        match self {
            Executor::CustomCommand(c) => c.command_line(),
            Executor::Task(t) => t.command_line.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ExecutionResult {
    pub status: ExecutionStatus,
    pub exit_code: Option<i32>,
    pub error: Option<anyhow::Error>,
}

impl ExecutionResult {
    pub fn success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionStatus {
    NotStarted,
    FailedToStart,
    Failed,
    Timeout,
    Success,
}

impl Default for ExecutionStatus {
    fn default() -> Self {
        Self::NotStarted
    }
}
