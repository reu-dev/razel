use std::fmt::Debug;
use std::path::{Path, PathBuf};

use anyhow::Context;
use sha2::Sha256;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

use crate::bazel_remote_exec;
use crate::bazel_remote_exec::{ActionResult, Digest, OutputFile};
use crate::cache::{GrpcRemoteCache, LocalCache};

#[derive(Clone)] // TODO is Cache::clone() a good idea?
pub struct Cache {
    pub local_cache: LocalCache,
    remote_cache: Option<GrpcRemoteCache>,
}

impl Cache {
    pub fn new(workspace_dir: &Path) -> Result<Self, anyhow::Error> {
        Ok(Self {
            local_cache: LocalCache::new(workspace_dir)
                .with_context(|| "Failed to create local cache")?,
            remote_cache: None, // TODO
        })
    }

    pub async fn get_action_result(&self, action_digest: &MessageDigest) -> Option<ActionResult> {
        if let Some(action_result) = self.local_cache.get_action_result(action_digest).await {
            if self
                .local_cache
                .is_action_completely_cached(&action_result)
                .await
            {
                return Some(action_result);
            }
        }
        None
    }

    pub async fn push_action_result(
        &self,
        digest: &MessageDigest,
        result: &ActionResult,
    ) -> Result<(), anyhow::Error> {
        if let Some(remote_cache) = &self.remote_cache {
            remote_cache.push_action_result(digest.clone(), result.clone());
        }
        self.local_cache.push_action_result(digest, result).await
    }

    pub async fn move_output_file_into_cache(
        &self,
        sandbox_dir: &Option<PathBuf>,
        out_dir: &PathBuf,
        exec_path: &PathBuf,
    ) -> Result<OutputFile, anyhow::Error> {
        let (output_file, cache_path) = self
            .local_cache
            .move_output_file_into_cache(sandbox_dir, out_dir, exec_path)
            .await?;
        if let Some(remote_cache) = &self.remote_cache {
            remote_cache.push_blob(output_file.digest.clone().unwrap(), cache_path);
        }
        Ok(output_file)
    }

    pub async fn symlink_output_files_into_out_dir(
        &self,
        action_result: &ActionResult,
        out_dir: &Path,
    ) -> Result<(), anyhow::Error> {
        self.local_cache
            .symlink_output_files_into_out_dir(action_result, out_dir)
            .await
    }
}

pub type MessageDigest = Digest;
pub type BlobDigest = Digest;

impl Digest {
    pub async fn for_file(path: impl AsRef<Path> + Debug) -> Result<BlobDigest, anyhow::Error> {
        use sha2::Digest;
        let file = File::open(&path)
            .await
            .with_context(|| format!("Failed to open {:?}", path))?;
        let mut reader = BufReader::new(file);
        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];
        let mut len = 0;
        loop {
            let count = reader
                .read(&mut buffer)
                .await
                .with_context(|| format!("Failed to read {:?}", path))?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            len += count;
        }
        Ok(bazel_remote_exec::Digest {
            hash: Self::hex(&hasher.finalize()),
            size_bytes: len as i64,
        })
    }

    pub fn for_message<T: prost::Message>(msg: &T) -> MessageDigest {
        use sha2::Digest;
        let buf = message_to_pb_buf(msg);
        bazel_remote_exec::Digest {
            hash: Self::hex(&Sha256::digest(&buf)),
            size_bytes: buf.len() as i64,
        }
    }

    pub fn for_string(text: &String) -> MessageDigest {
        use sha2::Digest;
        bazel_remote_exec::Digest {
            hash: Self::hex(&Sha256::digest(text.as_bytes())),
            size_bytes: text.len() as i64,
        }
    }

    fn hex(input: &[u8]) -> String {
        base16ct::lower::encode_string(input)
    }
}

pub fn message_to_pb_buf<T: prost::Message>(msg: &T) -> Vec<u8> {
    let mut vec = Vec::new();
    vec.reserve(msg.encoded_len());
    msg.encode(&mut vec).unwrap();
    vec
}

#[cfg(test)]
mod tests {
    use sha2::Digest;

    use super::*;

    fn digest_file_sha256_simple(path: impl AsRef<Path>) -> Result<super::Digest, anyhow::Error> {
        let bytes = std::fs::read(path)?;
        Ok(super::Digest {
            hash: super::Digest::hex(&Sha256::digest(&bytes)),
            size_bytes: bytes.len() as i64,
        })
    }

    #[tokio::test]
    async fn digest_for_small_file() {
        let path = "test/data/a.csv";
        let act = super::Digest::for_file(&path).await.unwrap();
        let exp = digest_file_sha256_simple(path).unwrap();
        assert_eq!(act, exp);
        // check vs: sha256sum test/data/a.csv line endings
        if act.size_bytes == 18 {
            // test/data/a.csv has CRLF
            assert_eq!(
                act,
                super::Digest {
                    hash: "11f5756d3300e967b28969ee86532fe891b0ea42e5ba843bc212fe444cf0f37d".into(),
                    size_bytes: 18,
                }
            );
        } else {
            // test/data/a.csv has LF line endings
            assert_eq!(
                act,
                super::Digest {
                    hash: "e0f702d446912234e5767af1db3f8b23b04beade5cdd1ea72d78c4f88c869b80".into(),
                    size_bytes: 16,
                }
            );
        }
    }

    #[tokio::test]
    async fn digest_for_bigger_file() {
        let path = "Cargo.lock";
        let act = super::Digest::for_file(&path).await.unwrap();
        let exp = digest_file_sha256_simple(path).unwrap();
        assert_eq!(act, exp);
    }

    #[test]
    fn digest_for_string() {
        assert_eq!(
            super::Digest::for_string(&"Hello World!".into()),
            super::Digest {
                // echo -n "Hello World!" | sha256sum
                hash: "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069".into(),
                size_bytes: 12,
            }
        );
    }
}
