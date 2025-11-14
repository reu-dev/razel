use crate::types::Tag;
use crate::CliTask;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

pub type TargetId = usize;
pub type FileId = usize;

pub struct File {
    pub id: FileId,
    pub path: PathBuf,
    pub digest: Option<Digest>,
    pub executable: Option<ExecutableType>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExecutableType {
    ExecutableInWorkspace,
    ExecutableOutsideWorkspace,
    WasiModule,
    SystemExecutable,
    RazelExecutable,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    /// The hash, represented as a lowercase hexadecimal string, padded with
    /// leading zeroes up to the hash function length.
    pub hash: String,
    pub size_bytes: i64,
}

#[derive(Serialize, Deserialize)]
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
}

#[derive(Serialize, Deserialize)]
pub enum TargetKind {
    Command(CommandTarget),
    Wasi(WasiTarget),
    Task(TaskTarget),
    Service(ServiceTarget),
}

#[derive(Serialize, Deserialize)]
pub struct CommandTarget {
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
pub struct WasiTarget {
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
}

/// A razel builtin task, see `razel task`
#[derive(Serialize, Deserialize)]
pub struct TaskTarget {
    pub args: Vec<String>,
    pub task: CliTask,
}

/// A service provided by a worker.
#[derive(Serialize, Deserialize)]
pub struct ServiceTarget {
    pub name: String,
    pub version: String,
    pub args: Vec<String>,
}
