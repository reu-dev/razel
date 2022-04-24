use crate::{ArenaId, CommandId};

pub struct File {
    pub id: FileId,
    /// files without creating_command are data files which must exist before running any commands
    pub creating_command: Option<CommandId>,
    /// path to be used for exec
    pub path: String,
}

pub type FileId = ArenaId<File>;
