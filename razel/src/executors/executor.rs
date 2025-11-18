use crate::executors::{
    CustomCommandExecutor, ExecutionResult, HttpRemoteExecutor, TaskExecutor, WasiExecutor,
};
use crate::types::{Tag, Target, TargetKind, Task, TaskTarget};
use crate::{CGroup, SandboxDir};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Clone)]
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
            (Executor::Wasi(e), TargetKind::Command(c)) => {
                e.exec(c, cwd, sandbox_dir.dir.as_ref().unwrap()).await
            }
            (Executor::Task(e), TargetKind::Task(t)) => e.exec(&t.task, sandbox_dir).await,
            (
                Executor::HttpRemote(e),
                TargetKind::Task(TaskTarget {
                    task: Task::HttpRemoteExec(t),
                    ..
                }),
            ) => e.exec(t).await,
            _ => unreachable!(),
        }
    }

    pub fn args(&self) -> &Vec<String> {
        match self {
            Executor::CustomCommand(x) => &x.args,
            Executor::Wasi(x) => &x.args,
            Executor::Task(x) => &x.args,
            Executor::HttpRemote(x) => &x.args,
        }
    }

    pub fn env(&self) -> Option<&HashMap<String, String>> {
        match self {
            Executor::CustomCommand(x) => Some(&x.env),
            Executor::Wasi(x) => Some(&x.env),
            Executor::Task(_) => None,
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
    /// Internally implemented tasks have well-defined inputs and outputs. This might not be true
    /// for other commands, therefore the sandbox must be used to make caching reliable.
    pub fn use_sandbox(&self) -> bool {
        match self {
            Executor::CustomCommand(_) => true,
            Executor::Wasi(_) => true,
            Executor::Task(_) => true,
            Executor::HttpRemote(_) => false,
        }
    }
}
