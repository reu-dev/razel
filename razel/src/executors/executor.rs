use crate::executors::{
    CustomCommandExecutor, ExecutionResult, HttpRemoteExecutor, TaskExecutor, WasiExecutor,
};
use crate::types::{Tag, Target, TargetKind};
use crate::{CGroup, SandboxDir};
use std::path::Path;

pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Wasi(WasiExecutor),
    Task(TaskExecutor),
    HttpRemote(HttpRemoteExecutor),
}

impl Executor {
    pub async fn exec(
        &self,
        target: &Target,
        cwd: &Path,
        sandbox_dir: &SandboxDir,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        match (self, &target.kind) {
            (Executor::CustomCommand(e), TargetKind::Command(c)) => {
                let timeout = target.tags.iter().find_map(|t| {
                    if let Tag::Timeout(x) = t {
                        Some(*x)
                    } else {
                        None
                    }
                });
                e.exec(c, sandbox_dir, cgroup, timeout).await
            }
            (Executor::Task(e), TargetKind::Task(t)) => e.exec(&t.task, sandbox_dir).await,
            (Executor::Wasi(e), TargetKind::Wasi(c)) => e.exec(c, cwd, sandbox_dir).await,
            (Executor::HttpRemote(e), _) => e.exec().await,
            _ => unreachable!(),
        }
    }
}
