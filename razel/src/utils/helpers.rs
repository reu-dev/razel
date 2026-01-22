use anyhow::{Result, anyhow};
use std::fs;
use std::path::{Component, Path, PathBuf};

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

/// Drops ../ components from paths without disk read
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(Component::Prefix(c)) = components.peek() {
        let buf = PathBuf::from(c.as_os_str());
        components.next();
        buf
    } else {
        PathBuf::new()
    };
    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_normalize_path(path: &str, exp: &str) {
        let act = super::normalize_path(Path::new(path))
            .to_string_lossy()
            .to_string();
        assert_eq!(act, exp);
    }

    #[test]
    fn normalize_path() {
        if cfg!(windows) {
            test_normalize_path("file", "file");
            test_normalize_path("C:/Users/user", "C:\\Users\\user");
            test_normalize_path("C:/Users/user/../file", "C:\\Users\\file");
        } else {
            test_normalize_path("file", "file");
            test_normalize_path("/home/user", "/home/user");
            test_normalize_path("/home/user/../file", "/home/file");
        }
    }
}
