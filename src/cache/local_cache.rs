use std::io::ErrorKind;
use std::path::PathBuf;

use anyhow::{bail, Context};
use directories::ProjectDirs;
use log::warn;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::bazel_remote_exec::{ActionResult, Digest};
use crate::cache::{message_to_pb_buf, MessageDigest};
use crate::config;

#[derive(Clone)]
pub struct LocalCache {
    pub ac_dir: PathBuf,
    #[allow(dead_code)]
    pub cas_dir: PathBuf,
}

impl LocalCache {
    pub fn new() -> Result<Self, anyhow::Error> {
        let dir = Self::dir();
        let ac_dir = dir.join("ac");
        let cas_dir = dir.join("cas");
        std::fs::create_dir_all(&ac_dir)?;
        std::fs::create_dir_all(&cas_dir)?;
        Ok(Self { ac_dir, cas_dir })
    }

    pub fn dir() -> PathBuf {
        let project_dirs = ProjectDirs::from("", "reu-dev", config::EXECUTABLE).unwrap();
        project_dirs.cache_dir().into()
    }

    pub async fn get_action_result(&self, digest: &MessageDigest) -> Option<ActionResult> {
        let path = self.ac_dir.join(&digest.hash);
        match Self::try_read_pb_file(&path).await {
            Ok(x) => x,
            Err(x) => {
                warn!("{:?}", x);
                tokio::fs::remove_file(path).await.ok();
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
            .with_context(|| format!("push_action_result(): {:?}", path))
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
            let act_size = metadata.len();
            let exp_size = digest.size_bytes as u64;
            if act_size != exp_size {
                warn!(
                    "OutputFile has wrong size (act: {act_size}, exp:{exp_size}): {:?}",
                    path
                );
                tokio::fs::remove_file(path).await.ok();
                return false;
            }
            true
        } else {
            false
        }
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
