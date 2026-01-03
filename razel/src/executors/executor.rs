use crate::SandboxDir;
use crate::executors::{
    CommandExecutor, ExecutionResult, HttpRemoteExecutor, TaskExecutor, WasiExecutor,
};
use crate::types::Digest;
use std::path::Path;
use std::sync::OnceLock;

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

    /// used in Action::input_root_digest for versioning breaking changes in task/wasi executors
    ///
    /// Not using the digest of the razel executable to allow cache hits across platforms and razel versions.
    pub fn digest(&self) -> Option<&Digest> {
        static WASI_EXECUTOR_DIGEST: OnceLock<Digest> = OnceLock::new();
        static TASK_EXECUTOR_DIGEST: OnceLock<Digest> = OnceLock::new();
        static HTTP_REMOTE_EXECUTOR_DIGEST: OnceLock<Digest> = OnceLock::new();
        match self {
            Executor::Command(_) => None,
            Executor::Wasi(_) => Some(WASI_EXECUTOR_DIGEST.get_or_init(|| Digest::for_string("1"))),
            Executor::Task(_) => Some(TASK_EXECUTOR_DIGEST.get_or_init(|| Digest::for_string("1"))),
            Executor::HttpRemote(_) => {
                Some(HTTP_REMOTE_EXECUTOR_DIGEST.get_or_init(|| Digest::for_string("1")))
            }
        }
    }
}
