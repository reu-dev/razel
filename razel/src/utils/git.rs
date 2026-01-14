use std::collections::HashMap;
use std::path::Path;

pub async fn find_repo_for_path(mut path: &Path) -> Option<&Path> {
    if path.is_file() {
        path = path.parent()?;
    }
    loop {
        // .git is a file in case of submodule or worktree, which is fine
        if path.join(".git").exists() {
            return Some(path);
        }
        path = path.parent()?;
    }
}

pub async fn find_repo_for_path_cached<'a>(
    path: &'a Path,
    cache: &mut HashMap<&'a Path, &'a Path>,
) -> Option<&'a Path> {
    let initial_dir = if path.is_file() { path.parent()? } else { path };
    let mut current_dir = initial_dir;
    loop {
        if let Some(repo) = cache.get(current_dir) {
            return Some(*repo);
        }
        // .git is a file in case of submodule or worktree, which is fine
        if current_dir.join(".git").exists() {
            break;
        }
        current_dir = current_dir.parent()?;
    }
    cache.insert(current_dir, current_dir);
    cache.insert(initial_dir, current_dir);
    Some(current_dir)
}
