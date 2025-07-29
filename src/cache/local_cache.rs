use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use log::warn;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::bazel_remote_exec::{ActionResult, Digest, OutputFile};
use crate::cache::{message_to_pb_buf, BlobDigest, MessageDigest};
use crate::config::LinkType;
use crate::{force_remove_file, set_file_readonly, write_gitignore};

#[derive(Clone)]
pub struct LocalCache {
    pub dir: PathBuf,
    ac_dir: PathBuf,
    cas_dir: PathBuf,
}

impl LocalCache {
    pub fn new(dir: PathBuf) -> Result<Self, anyhow::Error> {
        let ac_dir = dir.join("ac");
        let cas_dir = dir.join("cas");
        std::fs::create_dir_all(&ac_dir)?;
        std::fs::create_dir_all(&cas_dir)?;
        write_gitignore(&dir);
        Ok(Self {
            dir,
            ac_dir,
            cas_dir,
        })
    }

    pub fn cas_path(&self, digest: &BlobDigest) -> PathBuf {
        self.cas_dir.join(&digest.hash)
    }

    pub async fn get_action_result(&self, digest: &MessageDigest) -> Option<ActionResult> {
        let path = self.ac_dir.join(&digest.hash);
        match Self::try_read_pb_file(&path).await {
            Ok(x) => x,
            Err(x) => {
                warn!("{x:?}");
                force_remove_file(path).await.ok();
                None
            }
        }
    }

    pub async fn push_action_result(
        &self,
        digest: &MessageDigest,
        result: &ActionResult,
    ) -> Result<(), anyhow::Error> {
        let path = self.ac_dir.join(&digest.hash);
        Self::write_pb_file(&path, result)
            .await
            .with_context(|| format!("push_action_result(): {path:?}"))
    }

    pub async fn is_blob_cached(&self, digest: &Digest) -> bool {
        let path = self.cas_path(digest);
        if let Ok(metadata) = tokio::fs::metadata(&path).await {
            if !metadata.permissions().readonly() {
                // readonly flag was removed - assume file was modified
                force_remove_file(path).await.ok();
                return false;
            }
            let act_size = metadata.len();
            let exp_size = digest.size_bytes as u64;
            if act_size != exp_size {
                warn!("OutputFile has wrong size (act: {act_size}, exp:{exp_size}): {path:?}");
                force_remove_file(path).await.ok();
                return false;
            }
            true
        } else {
            false
        }
    }

    /// To be called before Self::move_file_into_cache() without mutex lock
    pub async fn prepare_file_to_move(&self, src: &PathBuf) -> Result<(), anyhow::Error> {
        set_file_readonly(src)
            .await
            .with_context(|| format!("Error in set_readonly {src:?}"))
    }

    /// Self::prepare_file_for_moving_to_cache() must have been called before
    pub async fn move_file_into_cache(
        &self,
        src: &PathBuf,
        digest: &Digest,
    ) -> Result<PathBuf, anyhow::Error> {
        let dst = self.cas_path(digest);
        match tokio::fs::rename(src, &dst).await {
            Ok(()) => {}
            Err(e) => {
                if !self.is_blob_cached(digest).await {
                    return Err(e).with_context(|| format!("mv {src:?} -> {dst:?}"));
                }
                // behave like src was moved
                force_remove_file(src).await?;
            }
        }
        Ok(dst)
    }

    pub async fn link_output_files_into_out_dir(
        &self,
        output_files: &Vec<OutputFile>,
        out_dir: &Path,
    ) -> Result<(), anyhow::Error> {
        for file in output_files {
            let cas_path = self.cas_path(file.digest.as_ref().unwrap());
            let out_path = out_dir.join(&file.path);
            match crate::config::OUT_DIR_LINK_TYPE {
                LinkType::Hardlink => crate::force_hardlink(&cas_path, &out_path).await?,
                LinkType::Symlink => crate::force_symlink(&cas_path, &out_path).await?,
            }
        }
        Ok(())
    }

    async fn try_read_pb_file<T: prost::Message + Default>(
        path: &PathBuf,
    ) -> Result<Option<T>, anyhow::Error> {
        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    return Ok(None);
                }
                bail!(err);
            }
        };
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;
        match T::decode(buf.as_slice()) {
            Ok(x) => Ok(Some(x)),
            Err(x) => bail!(x),
        }
    }

    async fn write_pb_file<T: prost::Message>(path: &PathBuf, msg: &T) -> std::io::Result<()> {
        let buf = message_to_pb_buf(msg);
        tokio::fs::write(path, buf).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use std::time::Duration;

    #[tokio::test]
    async fn move_output_file_into_cache() {
        let src_dir = new_tmp_dir!();
        let src = src_dir.join_and_write_file("some-output-file", "some content");
        let dst_dir = new_tmp_dir!();
        let dst = dst_dir.join("file-in-cache");
        set_file_readonly(&src).await.unwrap();
        let src_mtime = src.metadata().unwrap().modified().unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        tokio::fs::rename(&src, &dst).await.unwrap();
        assert!(tokio::fs::metadata(&dst)
            .await
            .unwrap()
            .permissions()
            .readonly());
        let dst_mtime = dst.metadata().unwrap().modified().unwrap();
        assert_eq!(dst_mtime, src_mtime);
    }
}
