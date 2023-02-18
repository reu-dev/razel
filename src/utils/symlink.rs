use crate::force_remove_file;
use anyhow::{anyhow, bail, Context};
use std::io;
use std::path::PathBuf;
use tokio::fs;

/// Force creating a symlink: overwrite existing file and create parent directories
pub async fn force_symlink(src: &PathBuf, dst: &PathBuf) -> Result<(), anyhow::Error> {
    {
        if src == dst {
            bail!("symlink dst must not equal src");
        }
        if let Ok(existing) = fs::read_link(&dst).await {
            if existing == *src {
                return Ok(());
            }
        }
        force_remove_file(&dst).await?; // to avoid symlink() fail with "File exists"
        fs::create_dir_all(dst.parent().unwrap())
            .await
            .with_context(|| {
                anyhow!(
                    "Failed to create destination directory: {:?}",
                    dst.parent().unwrap()
                )
            })?;
        symlink_file(src, dst).with_context(|| anyhow!("symlink_file"))
    }
    .with_context(|| anyhow!("force_symlink {src:?} -> {dst:?}"))?;
    Ok(())
}

#[cfg(target_family = "windows")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::windows::fs::symlink_file(&src, &dst)
}

#[cfg(target_family = "unix")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::unix::fs::symlink(src, dst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use temp_dir::TempDir;

    const FIRST_CONTENT: &str = "FIRST_CONTENT";
    const OTHER_CONTENT: &str = "OTHER_CONTENT";

    #[tokio::test]
    async fn create_recreate_and_modify() {
        let src_dir = TempDir::new().unwrap();
        let dst_dir = TempDir::new().unwrap();
        let first_src = src_dir.child("first-src-file");
        let other_src = src_dir.child("other-src-file");
        let dst = dst_dir.child("dst-dir").join("dst-file");
        fs::write(&first_src, FIRST_CONTENT).unwrap();
        fs::write(&other_src, OTHER_CONTENT).unwrap();
        // create initial symlink
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
