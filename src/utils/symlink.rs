use anyhow::Context;
use std::fs;
use std::io;
use std::path::PathBuf;
use tokio::task::spawn_blocking;

/// Force creating a symlink: overwrite existing file and create parent directories
pub async fn force_symlink(src: &PathBuf, dst: &PathBuf) -> Result<(), anyhow::Error> {
    assert_ne!(src, dst);
    {
        let src = src.clone();
        let dst = dst.clone();
        spawn_blocking(move || {
            if let Ok(existing) = fs::read_link(&dst) {
                if existing == *src {
                    return Ok(());
                }
            }
            fs::remove_file(&dst).ok(); // to avoid symlink() fail with "File exists"
            fs::create_dir_all(dst.parent().unwrap())?;
            symlink_file(&src, &dst)
        })
        .await?
    }
    .with_context(|| format!("symlink {:?} -> {:?}", src, dst))?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::windows::fs::symlink_file(&src, &dst)
}

#[cfg(target_os = "linux")]
fn symlink_file(src: &PathBuf, dst: &PathBuf) -> io::Result<()> {
    std::os::unix::fs::symlink(&src, &dst)
}

#[cfg(test)]
mod tests {
    use temp_dir::TempDir;

    use super::*;

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
