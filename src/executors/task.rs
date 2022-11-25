use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::executors::ExecutionResult;

pub type TaskFn = Arc<dyn Fn() -> Result<(), anyhow::Error> + Send + Sync>;

#[derive(Clone)]
pub struct BlockingTaskExecutor {
    pub f: TaskFn,
    pub args: Vec<String>,
}

impl BlockingTaskExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let execution_start = Instant::now();
        let result = (self.f)();
        ExecutionResult::for_task(result, execution_start)
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        self.args.clone()
    }
}

#[derive(Clone)]
pub struct AsyncTaskExecutor {
    pub task: Arc<dyn AsyncTask + Send + Sync>,
    pub args: Vec<String>,
}

impl AsyncTaskExecutor {
    pub async fn exec(&self, sandbox_dir: Option<PathBuf>) -> ExecutionResult {
        let execution_start = Instant::now();
        let result = self.task.exec(sandbox_dir).await;
        ExecutionResult::for_task(result, execution_start)
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        self.args.clone()
    }
}

#[async_trait]
pub trait AsyncTask {
    async fn exec(&self, sandbox_dir: Option<PathBuf>) -> Result<(), anyhow::Error>;
}
