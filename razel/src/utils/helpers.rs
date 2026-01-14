use anyhow::{Result, anyhow};
use std::fs;
use std::path::Path;

pub const GITIGNORE_FILENAME: &str = ".gitignore";

pub fn read_json_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = fs::read(path).map_err(|e| anyhow!("failed to read {path:?}: {e:?}"))?;
    let value = serde_json::from_slice(&contents)
        .map_err(|e| anyhow!("failed to deserialize {path:?}: {e:?}"))?;
    Ok(value)
}

pub fn write_gitignore(dir: &Path) {
    let gitignore = dir.join(GITIGNORE_FILENAME);
    if !gitignore.exists() {
        fs::write(gitignore, "*\n").ok();
    }
}
