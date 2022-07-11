use directories::ProjectDirs;
use std::path::{Path, PathBuf};

pub static EXECUTABLE: &str = "razel";
pub static OUT_DIR: &str = "razel-out";
/// TODO SANDBOX_DIR should be outside the workspace to help IDE indexer
pub static SANDBOX_DIR: &str = "razel-out";

pub fn select_cache_dir(workspace_dir: &PathBuf) -> PathBuf {
    let project_dirs = ProjectDirs::from("", "reu-dev", EXECUTABLE).unwrap();
    let home_cache: PathBuf = project_dirs.cache_dir().into();
    return if device_of_dir(home_cache.parent().unwrap())
        == device_of_dir(workspace_dir.parent().unwrap())
    {
        home_cache
    } else {
        workspace_dir.parent().unwrap().join(".razel-cache")
    };
}

#[cfg(target_family = "unix")]
fn device_of_dir(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    dir.metadata().unwrap().dev()
}

#[cfg(target_family = "windows")]
fn device_of_dir(dir: &Path) -> String {
    use std::path::Component;
    match dir.components().next().unwrap() {
        Component::Prefix(x) => x.as_os_str().to_str().unwrap().to_string(),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directories::UserDirs;
    use std::env;

    fn check_cache_dir(workspace_dir: &PathBuf, cache_dir: &PathBuf) {
        println!(
            "workspace_dir: {:?}, cache_dir: {:?}",
            workspace_dir, cache_dir
        );
        assert!(cache_dir.is_absolute());
        assert_eq!(
            device_of_dir(&cache_dir),
            device_of_dir(&workspace_dir.parent().unwrap())
        );
    }

    #[test]
    fn workspace_within_home() {
        let user_dirs = UserDirs::new().unwrap();
        let home_dir = user_dirs.home_dir();
        let workspace_dir = home_dir.join("ws");
        let cache_dir = select_cache_dir(&workspace_dir);
        check_cache_dir(&workspace_dir, &cache_dir);
    }

    #[test]
    fn workspace_within_temp() {
        let temp_dir = env::temp_dir();
        let workspace_dir = temp_dir.join("ws");
        let cache_dir = select_cache_dir(&workspace_dir);
        check_cache_dir(&workspace_dir, &cache_dir);
    }
}
