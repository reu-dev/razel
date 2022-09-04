use std::sync::Arc;
use std::time::Instant;

use crate::executors::{ExecutionResult, ExecutionStatus};

pub type TaskFn = Arc<dyn Fn() -> Result<(), anyhow::Error> + Send + Sync>;

#[derive(Clone)]
pub struct TaskExecutor {
    pub f: TaskFn,
    pub args: Vec<String>,
}

impl TaskExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        let execution_start = Instant::now();
        match (self.f)() {
            Ok(()) => {
                result.status = ExecutionStatus::Success;
                result.exit_code = Some(0);
                result.duration = Some(execution_start.elapsed())
            }
            Err(e) => {
                result.status = ExecutionStatus::Failed;
                result.error = Some(e);
            }
        }
        result
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        self.args.clone()
    }
}
