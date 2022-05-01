use crate::executors::{ExecutionResult, ExecutionStatus};
use std::sync::Arc;

pub type TaskFn = Arc<dyn Fn() -> Result<(), anyhow::Error> + Send + Sync>;

#[derive(Clone)]
pub struct TaskExecutor {
    pub f: TaskFn,
    pub command_line: String,
}

impl TaskExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        match (self.f)() {
            Ok(()) => {
                result.status = ExecutionStatus::Success;
            }
            Err(e) => {
                result.status = ExecutionStatus::Failed;
                result.error = Some(e);
            }
        }
        result
    }
}
