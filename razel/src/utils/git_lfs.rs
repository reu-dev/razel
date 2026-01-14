use crate::config::EXE_SUFFIX;
use crate::git::find_repo_for_path_cached;
use anyhow::{Result, anyhow};
use itertools::Itertools;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::{info, instrument, warn};

const LFS_HEADER: &str = "version https://git-lfs.github.com/spec/v1\n";

#[instrument(skip_all)]
pub async fn pull_files(paths: &Vec<PathBuf>) -> Result<()> {
    let mut cache = Default::default();
    let mut files_for_repo: HashMap<&Path, Vec<&Path>> = Default::default();
    let mut lfs_pointers = 0;
    for path in paths {
        match is_lfs_pointer_file(path).await {
            Ok(false) => continue,
            Ok(true) => {
                lfs_pointers += 1;
                let repo = find_repo_for_path_cached(path, &mut cache)
                    .await
                    .ok_or_else(|| anyhow!("No git repository found: {path:?}"))?;
                files_for_repo
                    .entry(repo)
                    .or_default()
                    .push(path.strip_prefix(repo).unwrap())
            }
            Err(e) => warn!("{e:?}: {path:?}"),
        }
    }
    info!(
        paths = paths.len(),
        lfs_pointers,
        repositories = files_for_repo.len(),
    );
    for (dir, files) in files_for_repo {
        pull_files_within_single_repo(dir, files).await?;
    }
    Ok(())
}

pub async fn pull_files_within_single_repo(repo: &Path, files: Vec<&Path>) -> Result<()> {
    assert!(!files.is_empty());
    let program = format!("git{EXE_SUFFIX}");
    let files_joined = files
        .into_iter()
        .map(|p| p.to_str().unwrap())
        .sorted()
        .join(",");
    let args = ["lfs", "pull", "-I", &files_joined];
    info!("cd {repo:?} && {program} {}", args.join(" "));
    Command::new(program)
        .args(args)
        .current_dir(repo)
        .status()
        .await?;
    Ok(())
}

pub async fn is_lfs_pointer_file(path: &Path) -> Result<bool> {
    let mut file = File::open(path).await?;
    let mut buffer = [0; LFS_HEADER.len()];
    let len = file.read(&mut buffer).await?;
    Ok(len == LFS_HEADER.len() && buffer == LFS_HEADER.as_bytes())
}
