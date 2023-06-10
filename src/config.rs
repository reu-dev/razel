use anyhow::Context;
use directories::ProjectDirs;
use std::path::{Path, PathBuf};

/// The max number of args to show in command lines, or show all if not set.
pub static UI_COMMAND_ARGS_LIMIT: Option<usize> = Some(100);

pub static EXECUTABLE: &str = "razel";
pub static OUT_DIR: &str = "razel-out";

/// The prefix for using a param/response file as command args
pub static RESPONSE_FILE_PREFIX: &str = "@";

pub fn select_cache_dir(workspace_dir: &Path) -> Result<PathBuf, anyhow::Error> {
    let project_dirs = ProjectDirs::from("de", "reu-dev", EXECUTABLE).unwrap();
    let home_cache: PathBuf = project_dirs.cache_dir().into();
    std::fs::create_dir_all(&home_cache)?;
    let home_cache_device = device_of_dir(&home_cache)?;
    let workspace_device = device_of_dir(workspace_dir)?;
    Ok(if home_cache_device == workspace_device {
        home_cache
    } else {
        workspace_dir.join(".razel-cache")
    })
}

#[cfg(target_family = "unix")]
fn device_of_dir(dir: &Path) -> Result<u64, anyhow::Error> {
    use std::os::unix::fs::MetadataExt;
    Ok(dir
        .metadata()
        .with_context(|| format!("device_of_dir: {dir:?}"))?
        .dev())
}

#[cfg(target_family = "windows")]
fn device_of_dir(dir: &Path) -> Result<String, anyhow::Error> {
    use std::path::Component;
    match dir
        .components()
        .next()
        .with_context(|| format!("device_of_dir: {:?}", dir))?
    {
        Component::Prefix(x) => Ok(x.as_os_str().to_str().unwrap().to_string()),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TempDir;
    use crate::unique_test_name;
    use directories::UserDirs;
    use std::env;

    fn check_cache_dir(workspace_dir: &PathBuf) {
        println!("workspace_dir: {workspace_dir:?}");
        let cache_dir = select_cache_dir(workspace_dir).unwrap();
        println!("cache_dir:     {cache_dir:?}");
        assert!(cache_dir.is_absolute());
        assert_eq!(
            device_of_dir(cache_dir.parent().unwrap()).unwrap(),
            device_of_dir(workspace_dir.parent().unwrap()).unwrap()
        );
    }

    #[test]
    fn workspace_within_home() {
        let user_dirs = UserDirs::new().unwrap();
        let home_dir = user_dirs.home_dir();
        let workspace = TempDir::with_dir(home_dir.join(format!(".tmp-{}", unique_test_name!())));
        check_cache_dir(workspace.dir());
    }

    #[test]
    fn workspace_within_temp() {
        let workspace =
            TempDir::with_dir(env::temp_dir().join(format!(".tmp-{}", unique_test_name!())));
        check_cache_dir(workspace.dir());
    }
}
