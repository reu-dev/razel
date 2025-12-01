use crate::config;
use crate::types::{Tag, Task};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::iter::once;
use std::path::PathBuf;

pub type TargetId = usize;
pub type FileId = usize;

#[derive(Clone, Serialize, Deserialize)]
pub struct File {
    pub id: FileId,
    pub path: PathBuf,
    pub digest: Option<Digest>,
    pub executable: Option<ExecutableType>,
    pub is_excluded: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExecutableType {
    ExecutableInWorkspace,
    ExecutableOutsideWorkspace,
    WasiModule,
    SystemExecutable,
    RazelExecutable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    /// The hash, represented as a lowercase hexadecimal string, padded with
    /// leading zeroes up to the hash function length.
    pub hash: String,
    pub size_bytes: i64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub kind: TargetKind,
    /// user specified executable and optionally runtimes, e.g. razel for WASI
    pub executables: Vec<FileId>,
    /// input files excluding <Self::executables>
    pub inputs: Vec<FileId>,
    pub outputs: Vec<FileId>,
    /// dependencies on other targets in addition to input files
    pub deps: Vec<TargetId>,
    pub tags: Vec<Tag>,
    pub is_excluded: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TargetKind {
    Command(CommandTarget),
    Wasi(CommandTarget),
    Task(TaskTarget),
    HttpRemoteExecTask(TaskTarget),
}

impl TargetKind {
    pub fn args_with_executable(&self) -> Vec<String> {
        match self {
            TargetKind::Command(x) => x.args_with_executable(),
            TargetKind::Wasi(x) => [
                config::EXECUTABLE.to_string(),
                "command".into(),
                "--".into(),
            ]
            .into_iter()
            .chain(x.args_with_executable())
            .collect(),
            TargetKind::Task(x) | TargetKind::HttpRemoteExecTask(x) => {
                x.command_line_with_redirects()
            }
        }
    }

    pub fn command_line_with_redirects(&self) -> Vec<String> {
        match self {
            TargetKind::Command(x) => x.command_line_with_redirects(),
            TargetKind::Wasi(x) => [
                config::EXECUTABLE.to_string(),
                "command".into(),
                "--".into(),
            ]
            .into_iter()
            .chain(x.command_line_with_redirects())
            .collect(),
            TargetKind::Task(x) | TargetKind::HttpRemoteExecTask(x) => {
                x.command_line_with_redirects()
            }
        }
    }

    pub fn env(&self) -> Option<&HashMap<String, String>> {
        match self {
            TargetKind::Command(c) | TargetKind::Wasi(c) => Some(&c.env),
            TargetKind::Task(_) | TargetKind::HttpRemoteExecTask(_) => None,
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CommandTarget {
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
}

impl CommandTarget {
    pub fn args_with_executable(&self) -> Vec<String> {
        once(&self.executable)
            .chain(self.args.iter())
            .cloned()
            .collect()
    }

    pub fn command_line_with_redirects(&self) -> Vec<String> {
        once(&self.executable)
            .chain(self.args.iter())
            .chain(
                self.stdout_file
                    .as_ref()
                    .map(|x| [">".into(), x.to_str().unwrap().into()])
                    .iter()
                    .flatten(),
            )
            .chain(
                self.stderr_file
                    .as_ref()
                    .map(|x| ["2>".into(), x.to_str().unwrap().into()])
                    .iter()
                    .flatten(),
            )
            .cloned()
            .collect()
    }
}

/// A razel builtin task, see `razel task`
#[derive(Clone, Serialize, Deserialize)]
pub struct TaskTarget {
    pub args: Vec<String>,
    pub task: Task,
}

impl TaskTarget {
    pub fn args_with_executable(&self) -> Vec<String> {
        self.args.clone()
    }

    pub fn command_line_with_redirects(&self) -> Vec<String> {
        self.args.clone()
    }
}
