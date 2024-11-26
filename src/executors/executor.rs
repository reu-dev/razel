use crate::executors::{
    AsyncTaskExecutor, BlockingTaskExecutor, CustomCommandExecutor, ExecutionResult,
    HttpRemoteExecutor, WasiExecutor,
};
use crate::CGroup;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Wasi(WasiExecutor),
    AsyncTask(AsyncTaskExecutor),
    BlockingTask(BlockingTaskExecutor),
    HttpRemote(HttpRemoteExecutor),
}

impl Executor {
    pub async fn exec(
        &self,
        cwd: &Path,
        sandbox_dir: Option<PathBuf>,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        match self {
            Executor::CustomCommand(c) => c.exec(sandbox_dir, cgroup).await,
            Executor::Wasi(x) => x.exec(cwd, sandbox_dir.as_ref().unwrap()).await,
            Executor::AsyncTask(x) => x.exec(sandbox_dir).await,
            Executor::BlockingTask(t) => t.exec().await,
            Executor::HttpRemote(x) => x.exec().await,
        }
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        match self {
            Executor::CustomCommand(c) => c.args_with_executable(),
            Executor::Wasi(x) => x.args_with_executable(),
            Executor::AsyncTask(x) => x.args_with_executable(),
            Executor::BlockingTask(t) => t.args_with_executable(),
            Executor::HttpRemote(x) => x.args_with_executable(),
        }
    }

    pub fn command_line_with_redirects(&self, razel_executable: &str) -> Vec<String> {
        match self {
            Executor::CustomCommand(c) => c.command_line_with_redirects(),
            Executor::Wasi(x) => x.command_line_with_redirects(razel_executable),
            Executor::AsyncTask(x) => x.args_with_executable(),
            Executor::BlockingTask(t) => t.args_with_executable(),
            Executor::HttpRemote(x) => x.args_with_executable(),
        }
    }

    pub fn args(&self) -> &Vec<String> {
        match self {
            Executor::CustomCommand(x) => &x.args,
            Executor::Wasi(x) => &x.args,
            Executor::AsyncTask(x) => &x.args,
            Executor::BlockingTask(x) => &x.args,
            Executor::HttpRemote(x) => &x.args,
        }
    }

    pub fn env(&self) -> Option<&HashMap<String, String>> {
        match self {
            Executor::CustomCommand(x) => Some(&x.env),
            Executor::Wasi(x) => Some(&x.env),
            Executor::AsyncTask(_) => None,
            Executor::BlockingTask(_) => None,
            Executor::HttpRemote(_) => None,
        }
    }

    pub fn stdout_file(&self) -> Option<&PathBuf> {
        match self {
            Executor::CustomCommand(x) => x.stdout_file.as_ref(),
            Executor::Wasi(x) => x.stdout_file.as_ref(),
            _ => unreachable!(),
        }
    }

    pub fn stderr_file(&self) -> Option<&PathBuf> {
        match self {
            Executor::CustomCommand(x) => x.stderr_file.as_ref(),
            Executor::Wasi(x) => x.stderr_file.as_ref(),
            _ => unreachable!(),
        }
    }

    /// Returns if a sandbox should be used.
    ///
    /// Internally implemented tasks have well defined inputs and outputs. This might not be true
    /// for other commands, therefore the sandbox must be used to make caching reliable.
    pub fn use_sandbox(&self) -> bool {
        match self {
            Executor::CustomCommand(_) => true,
            Executor::Wasi(_) => true,
            Executor::AsyncTask(_) => true,
            Executor::BlockingTask(_) => false,
            Executor::HttpRemote(_) => false,
        }
    }
}
