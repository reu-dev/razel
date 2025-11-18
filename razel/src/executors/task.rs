use crate::executors::ExecutionResult;
use crate::types::Task;
use crate::SandboxDir;
use anyhow::Result;
use async_trait::async_trait;
use std::time::Instant;

#[derive(Clone)]
pub struct TaskExecutor {}

impl TaskExecutor {
    pub async fn exec(&self, task: &Task, sandbox_dir: &SandboxDir) -> ExecutionResult {
        let execution_start = Instant::now();
        let result = task.exec(sandbox_dir).await;
        ExecutionResult::for_task(result, execution_start)
    }
}

#[async_trait]
pub trait AsyncTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()>;
}

impl AsyncTask for Task {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        match self {
            Task::CaptureRegex(x) => x.exec(sandbox_dir).await,
            Task::CsvConcat(x) => x.exec(sandbox_dir).await,
            Task::CsvFilter(x) => x.exec(sandbox_dir).await,
            Task::WriteFile(x) => x.exec(sandbox_dir).await,
            Task::DownloadFile(x) => x.exec(sandbox_dir).await,
            Task::EnsureEqual(x) => x.exec(sandbox_dir).await,
            Task::EnsureNotEqual(x) => x.exec(sandbox_dir).await,
            Task::HttpRemoteExec(_) => unreachable!(),
        }
    }
}
