pub enum LinkType {
    Hardlink,
    Symlink,
}

/// The max number of args to show in command lines, or show all if not set.
pub static UI_COMMAND_ARGS_LIMIT: Option<usize> = Some(100);
pub static UI_UPDATE_INTERVAL_TTY: f32 = 0.2;
pub static UI_UPDATE_INTERVAL_NON_TTY: f32 = 20.0;
pub static EXECUTABLE: &str = "razel";
pub static OUT_DIR: &str = "razel-out";
/// The prefix for using a param/response file as command args
pub static RESPONSE_FILE_PREFIX: &str = "@";
pub static SANDBOX_LINK_TYPE: LinkType = LinkType::Symlink;
pub static OUT_DIR_LINK_TYPE: LinkType = LinkType::Symlink;
