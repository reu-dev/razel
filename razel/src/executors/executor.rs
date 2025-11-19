use crate::executors::{
    CommandExecutor, ExecutionResult, HttpRemoteExecutor, TaskExecutor, WasiExecutor,
};
use crate::SandboxDir;
use std::path::Path;

pub enum Executor {
    Command(CommandExecutor),
    Wasi(WasiExecutor),
    Task(TaskExecutor),
    HttpRemote(HttpRemoteExecutor),
}

impl Executor {
    pub async fn exec(&self, cwd: &Path, sandbox_dir: &SandboxDir) -> ExecutionResult {
        match self {
            Executor::Command(e) => e.exec(sandbox_dir).await,
            Executor::Task(e) => e.exec(sandbox_dir).await,
            Executor::Wasi(e) => e.exec(cwd, sandbox_dir).await,
            Executor::HttpRemote(e) => e.exec().await,
        }
    }
}
