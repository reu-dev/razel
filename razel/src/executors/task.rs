use crate::SandboxDir;
use crate::executors::ExecutionResult;
use crate::types::Task;
use anyhow::Result;
use std::time::Instant;

pub struct TaskExecutor {
    task: Task,
}

impl TaskExecutor {
    pub fn new(task: Task) -> Self {
        Self { task }
    }

    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> ExecutionResult {
        let execution_start = Instant::now();
        let result = self.task.exec(sandbox_dir).await;
        ExecutionResult::for_task(result, execution_start)
    }
}

impl Task {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        match self {
            Task::CaptureRegex(x) => x.exec(sandbox_dir).await,
            Task::CsvConcat(x) => x.exec(sandbox_dir).await,
            Task::CsvFilter(x) => x.exec(sandbox_dir).await,
            Task::WriteFile(x) => x.exec(sandbox_dir).await,
            Task::DownloadFile(x) => x.exec(sandbox_dir).await,
            Task::EnsureEqual(x) => x.exec(sandbox_dir).await,
            Task::EnsureNotEqual(x) => x.exec(sandbox_dir).await,
            Task::HttpRemoteExec(_) => unreachable!("should use HttpRemoteExecutor"),
            Task::CmakeEnableApi(x) => x.exec().await,
            Task::GitLfsPullCmakeDeps(x) => x.exec().await,
            Task::GitLfsPullCtestDeps(x) => x.exec().await,
        }
    }
}
