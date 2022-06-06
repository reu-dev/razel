use std::io::ErrorKind;
use std::path::PathBuf;

use anyhow::bail;
use directories::ProjectDirs;
use log::warn;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::bazel_remote_exec::ActionResult;
use crate::cache::ActionDigest;
use crate::config;

pub struct LocalCache {
    ac_dir: PathBuf,
    #[allow(dead_code)]
    cas_dir: PathBuf,
}

impl LocalCache {
    pub fn new() -> Result<Self, anyhow::Error> {
        let project_dirs = ProjectDirs::from("", "reu-dev", config::EXECUTABLE).unwrap();
        let dir = project_dirs.cache_dir();
        let ac_dir = dir.join("ac");
        let cas_dir = dir.join("cas");
        std::fs::create_dir_all(&ac_dir)?;
        std::fs::create_dir_all(&cas_dir)?;
        Ok(Self { ac_dir, cas_dir })
    }
}

impl LocalCache {
    pub async fn get_action_result(&self, digest: &ActionDigest) -> Option<ActionResult> {
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

    pub async fn push_action_result(&self, digest: &ActionDigest, result: &ActionResult) {
        let path = self.ac_dir.join(&digest.hash);
        match Self::write_pb_file(&path, result).await {
            Ok(()) => (),
            Err(x) => {
                warn!("{:?}", x);
            }
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
        let buf = Self::message_to_pb_buf(msg);
        tokio::fs::write(path, buf).await
    }

    fn message_to_pb_buf<T: prost::Message>(msg: &T) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.reserve(msg.encoded_len());
        msg.encode(&mut vec).unwrap();
        vec
    }
}
