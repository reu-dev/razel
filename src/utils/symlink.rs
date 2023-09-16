use crate::force_remove_file;
use anyhow::{bail, Context};
use std::io;
use std::path::PathBuf;
use tokio::fs;

/// Force creating a symlink: overwrite existing file and create parent directories
pub async fn force_symlink(src: &PathBuf, dst: &PathBuf) -> Result<(), anyhow::Error> {
    {
        if src == dst {
            bail!("symlink dst must not equal src");
        }
        let src_abs = fs::canonicalize(&src)
            .await
            .with_context(|| format!("canonicalize() {src:?}"))?;
        if let Ok(existing) = fs::read_link(&dst).await {
            if existing == src_abs {
                return Ok(());
            }
        }
        force_remove_file(&dst).await?; // to avoid symlink() fail with "File exists"
        let parent = dst.parent().unwrap();
        fs::create_dir_all(&parent)
            .await
            .with_context(|| format!("fs::create_dir_all() {parent:?}"))?;
        symlink_file(&src_abs, dst)
            .with_context(|| format!("symlink_file() {src_abs:?} -> {dst:?}"))
    }
    .with_context(|| format!("force_symlink() {src:?} -> {dst:?}"))?;
    Ok(())
}

#[cfg(target_family = "windows")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::windows::fs::symlink_file(src, dst)
}

#[cfg(target_family = "unix")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::unix::fs::symlink(src, dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use std::fs;

    const FIRST_CONTENT: &str = "FIRST_CONTENT";
    const OTHER_CONTENT: &str = "OTHER_CONTENT";

    #[tokio::test]
    async fn create_recreate_and_modify() {
        let src_dir = new_tmp_dir!();
        let first_src = src_dir.join_and_write_file("first-src-file", FIRST_CONTENT);
        let other_src = src_dir.join_and_write_file("other-src-file", OTHER_CONTENT);
        let dst_dir = new_tmp_dir!();
        let dst = dst_dir.join("dst-dir").join("dst-file");
        // create initial link
        force_symlink(&first_src, &dst).await.unwrap();
        assert_eq!(fs::read_to_string(&first_src).unwrap(), FIRST_CONTENT);
        assert_eq!(fs::read_to_string(&dst).unwrap(), FIRST_CONTENT);
        // recreate with same source
        force_symlink(&first_src, &dst).await.unwrap();
        assert_eq!(fs::read_to_string(&first_src).unwrap(), FIRST_CONTENT);
        assert_eq!(fs::read_to_string(&dst).unwrap(), FIRST_CONTENT);
        // modify to other source
        force_symlink(&other_src, &dst).await.unwrap();
        assert_eq!(fs::read_to_string(&first_src).unwrap(), FIRST_CONTENT);
        assert_eq!(fs::read_to_string(&other_src).unwrap(), OTHER_CONTENT);
        assert_eq!(fs::read_to_string(&dst).unwrap(), OTHER_CONTENT);
    }
}
