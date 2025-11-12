use std::fs;
use std::path::Path;

pub const GITIGNORE_FILENAME: &str = ".gitignore";

pub fn write_gitignore(dir: &Path) {
    let gitignore = dir.join(GITIGNORE_FILENAME);
    if !gitignore.exists() {
        fs::write(gitignore, "*\n").ok();
    }
}
