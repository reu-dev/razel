use crate::config::EXE_SUFFIX;
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::{debug, warn};

const LFS_HEADER: &str = "version https://git-lfs.github.com/spec/v1";

pub async fn pull_files(paths: &Vec<PathBuf>) -> Result<()> {
    let mut filenames_per_dir: HashMap<&Path, Vec<&str>> = Default::default();
    for path in paths {
        match is_lfs_pointer_file(path).await {
            Ok(false) => continue,
            Ok(true) => filenames_per_dir
                .entry(path.parent().unwrap())
                .or_default()
                .push(path.file_name().unwrap().to_str().unwrap()),
            Err(e) => warn!("{e:?}: {path:?}"),
        }
    }
    for (dir, files) in filenames_per_dir {
        pull_files_in_dir(dir, files).await?;
    }
    Ok(())
}

pub async fn pull_files_in_dir(dir: &Path, files: Vec<&str>) -> Result<()> {
    let program = format!("git{EXE_SUFFIX}");
    let args: Vec<_> = ["lfs", "pull", "--include"]
        .into_iter()
        .chain(files.into_iter())
        .collect();
    debug!("cd {dir:?} && {program} {}", args.join(" "));
    Command::new(program)
        .args(args)
        .current_dir(dir)
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
