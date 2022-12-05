use crate::cache::BlobDigest;
use crate::{ArenaId, CommandId};
use std::path::PathBuf;

#[derive(Debug, Eq, PartialEq)]
pub enum FileType {
    DataFile,
    OutputFile,
    ExecutableInWorkspace,
    SystemExecutable,
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
}

impl File {
    pub fn new(id: FileId, arg: String, file_type: FileType, path: PathBuf) -> Self {
        match file_type {
            FileType::DataFile => {}
            FileType::OutputFile => assert!(path.is_relative()),
            FileType::ExecutableInWorkspace => assert!(path.is_relative()),
            FileType::SystemExecutable => assert!(path.is_absolute()),
        };
        Self {
            id,
            arg,
            file_type,
            path,
            creating_command: None,
            digest: None,
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
            FileType::SystemExecutable => self.path.to_str().unwrap().to_string(),
        }
    }
}

pub type FileId = ArenaId<File>;
