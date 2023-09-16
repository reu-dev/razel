use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use log::warn;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::bazel_remote_exec::{ActionResult, Digest, OutputFile};
use crate::cache::{message_to_pb_buf, MessageDigest};
use crate::config::select_cache_dir;
use crate::{force_remove_file, force_symlink, set_file_readonly, write_gitignore};

#[derive(Clone)]
pub struct LocalCache {
    pub dir: PathBuf,
    ac_dir: PathBuf,
    cas_dir: PathBuf,
}

impl LocalCache {
    pub fn new(workspace_dir: &Path) -> Result<Self, anyhow::Error> {
        let dir = select_cache_dir(workspace_dir)?;
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

    pub async fn get_action_result(&self, digest: &MessageDigest) -> Option<ActionResult> {
        let path = self.ac_dir.join(&digest.hash);
        match Self::try_read_pb_file(&path).await {
            Ok(x) => x,
            Err(x) => {
                warn!("{:?}", x);
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

    pub async fn is_action_completely_cached(&self, result: &ActionResult) -> bool {
        for file in &result.output_files {
            if let Some(digest) = &file.digest {
                if !self.is_blob_cached(digest).await {
                    return false;
                }
            } else {
                warn!("OutputFile has no digest: {}", file.path);
                return false;
            }
        }
        true
    }

    pub async fn is_blob_cached(&self, digest: &Digest) -> bool {
        let path = self.cas_dir.join(&digest.hash);
        if let Ok(metadata) = tokio::fs::metadata(&path).await {
            if !metadata.permissions().readonly() {
                // readonly flag was removed - assume file was modified
                force_remove_file(path).await.ok();
                return false;
            }
            let act_size = metadata.len();
            let exp_size = digest.size_bytes as u64;
            if act_size != exp_size {
                warn!(
                    "OutputFile has wrong size (act: {act_size}, exp:{exp_size}): {:?}",
                    path
                );
                force_remove_file(path).await.ok();
                return false;
            }
            true
        } else {
            false
        }
    }

    pub async fn move_output_file_into_cache(
        &self,
        sandbox_dir: Option<&PathBuf>,
        out_dir: &PathBuf,
        file: &OutputFile,
    ) -> Result<PathBuf, anyhow::Error> {
        let src = if let Some(sandbox_dir) = sandbox_dir {
            sandbox_dir.join(out_dir).join(&file.path)
        } else {
            out_dir.join(&file.path)
        };
        if src.is_symlink() {
            bail!("output file must not be a symlink: {:?}", src);
        }
        let digest = file.digest.as_ref().unwrap();
        let dst = self.cas_dir.join(&digest.hash);
        /* call set_file_readonly() before renaming, because is_blob_cached()
         * might remove the file in between from another thread */
        set_file_readonly(&src)
            .await
            .with_context(|| format!("Error in set_readonly {src:?}"))?;
        match tokio::fs::rename(&src, &dst).await {
            Ok(()) => {}
            Err(e) => {
                if !self.is_blob_cached(digest).await {
                    return Err(e).with_context(|| format!("mv {src:?} -> {dst:?}"));
                }
                // behave like src was moved
                force_remove_file(&src).await?;
            }
        }
        Ok(dst)
    }

    pub async fn symlink_output_files_into_out_dir(
        &self,
        output_files: &Vec<OutputFile>,
        out_dir: &Path,
    ) -> Result<(), anyhow::Error> {
        if out_dir.starts_with(&self.cas_dir) {
            bail!("out_dir should not be within cas dir: {:?}", out_dir);
        }
        for file in output_files {
            let cas_path = self.cas_dir.join(&file.digest.as_ref().unwrap().hash);
            let out_path = out_dir.join(&file.path);
            force_symlink(&cas_path, &out_path).await?;
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

    #[tokio::test]
    async fn move_output_file_into_cache() {
        let src_dir = new_tmp_dir!();
        let src = src_dir.join_and_write_file("some-output-file", "some content");
        let dst_dir = new_tmp_dir!();
        let dst = dst_dir.join("file-in-cache");
        set_file_readonly(&src).await.unwrap();
        tokio::fs::rename(&src, &dst).await.unwrap();
        assert!(tokio::fs::metadata(&dst)
            .await
            .unwrap()
            .permissions()
            .readonly());
    }
}
