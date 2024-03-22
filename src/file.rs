use crate::cache::BlobDigest;
use crate::{config, ArenaId, CommandId};
use anyhow::{anyhow, Context};
use itertools::Itertools;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

#[derive(Debug, Eq, PartialEq)]
pub enum FileType {
    DataFile,
    OutputFile,
    ExecutableInWorkspace,
    ExecutableOutsideWorkspace,
    WasiModule,
    SystemExecutable,
    RazelExecutable,
}

impl FileType {
    /// Tries to avoid canonicalize() if possible because that bails on non-existing files.
    /// `Razel::digest_input_files()` reports all missing files at once.
    pub fn from_executable_arg(
        arg: &str,
        workspace_dir: &Path,
    ) -> Result<(Self, Option<PathBuf>), anyhow::Error> {
        let path = Path::new(arg);
        let (file_type, abs_path) = if path
            .file_name()
            .ok_or_else(|| anyhow!("no valid filename: \"{arg}\""))?
            == arg
        {
            assert!(!arg.contains('/'));
            if arg == config::EXECUTABLE {
                (Self::RazelExecutable, None)
            } else {
                (Self::SystemExecutable, None)
            }
        } else if path.iter().contains(&OsStr::new("..")) {
            let canonicalized = path
                .canonicalize()
                .with_context(|| format!("canonicalize \"{arg}\""))?;
            if canonicalized.starts_with(workspace_dir) {
                (Self::ExecutableInWorkspace, Some(canonicalized))
            } else {
                (Self::ExecutableOutsideWorkspace, Some(canonicalized))
            }
        } else if path.is_relative() {
            (Self::ExecutableInWorkspace, Some(workspace_dir.join(path)))
        } else if path.starts_with(workspace_dir) {
            (Self::ExecutableInWorkspace, Some(path.into()))
        } else {
            (Self::ExecutableOutsideWorkspace, Some(path.into()))
        };
        Ok((file_type, abs_path))
    }
}

pub struct File {
    pub id: FileId,
    /// argument from original command line
    pub arg: String,
    pub file_type: FileType,
    pub path: PathBuf,
    /// files without creating_command are input files (data or executable) which must exist before running any commands
    pub creating_command: Option<CommandId>,
    pub digest: Option<BlobDigest>,
    pub locally_cached: bool,
}

impl File {
    pub fn new(id: FileId, arg: String, file_type: FileType, path: PathBuf) -> Self {
        match file_type {
            FileType::DataFile => {}
            FileType::OutputFile | FileType::ExecutableInWorkspace | FileType::WasiModule => {
                assert!(path.is_relative())
            }
            FileType::ExecutableOutsideWorkspace
            | FileType::SystemExecutable
            | FileType::RazelExecutable => assert!(path.is_absolute()),
        };
        Self {
            id,
            arg,
            file_type,
            path,
            creating_command: None,
            digest: None,
            locally_cached: false,
        }
    }

    pub fn executable_for_command_line(&self) -> String {
        match self.file_type {
            FileType::DataFile => {
                panic!();
            }
            FileType::OutputFile | FileType::ExecutableInWorkspace => {
                format!("./{}", self.path.to_str().unwrap())
            }
            FileType::WasiModule => {
                // TODO command line should be directly executable
                self.path.to_str().unwrap().to_string()
            }
            FileType::ExecutableOutsideWorkspace
            | FileType::SystemExecutable
            | FileType::RazelExecutable => self.path.to_str().unwrap().to_string(),
        }
    }
}

pub type FileId = ArenaId<File>;
