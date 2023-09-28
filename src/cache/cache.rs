use crate::bazel_remote_exec;
use crate::bazel_remote_exec::{ActionResult, Digest, OutputFile};
use crate::cache::{GrpcRemoteCache, LocalCache};
use anyhow::{bail, Context};
use log::info;
use sha2::Sha256;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tonic::transport::Uri;

#[derive(Clone)] // TODO is Cache::clone() a good idea?
pub struct Cache {
    local_cache: LocalCache,
    remote_cache: Option<GrpcRemoteCache>,
}

impl Cache {
    pub fn new(workspace_dir: &Path) -> Result<Self, anyhow::Error> {
        Ok(Self {
            local_cache: LocalCache::new(workspace_dir)
                .with_context(|| "Failed to create local cache")?,
            remote_cache: None,
        })
    }

    pub fn dir(&self) -> &PathBuf {
        &self.local_cache.dir
    }

    pub async fn connect_remote_cache(&mut self, urls: &Vec<String>) -> Result<(), anyhow::Error> {
        for url in urls {
            let uri: Uri = url
                .parse()
                .with_context(|| format!("remote cache: {url}"))
                .context(
                    "remote cache should be an URI, e.g. grpc://localhost:9092[/instance_name]",
                )?;
            match uri.scheme_str() {
                Some("grpc") => match GrpcRemoteCache::new(uri, &self.local_cache.dir).await {
                    Ok(x) => {
                        self.remote_cache = Some(x);
                        info!("connected to remote cache: {url}");
                        break;
                    }
                    _ => {
                        info!("failed to connect to remote cache: {url}");
                    }
                },
                _ => bail!("only grpc remote caches are supported: {url}"),
            }
        }
        Ok(())
    }

    pub async fn get_action_result(&self, digest: &MessageDigest) -> Option<ActionResult> {
        let action_result =
            if let Some(action_result) = self.local_cache.get_action_result(digest).await {
                action_result
            } else if let Some(remote_cache) = &self.remote_cache {
                let x = remote_cache.get_action_result(digest.clone()).await?;
                self.local_cache.push_action_result(digest, &x).await.ok()?;
                x
            } else {
                return None;
            };
        let missing_files = self
            .local_cache
            .get_list_of_missing_output_files(&action_result)
            .await;
        match (missing_files.is_empty(), &self.remote_cache) {
            (true, _) => Some(action_result),
            (false, None) => None,
            (false, Some(remote_cache)) => {
                let downloaded = remote_cache
                    .download_and_store_blobs(&missing_files)
                    .await
                    .ok()?;
                // store all downloaded files even if incomplete, might be used by other action
                for (digest, path) in &downloaded {
                    self.local_cache
                        .move_file_into_cache(path, digest)
                        .await
                        .ok()?;
                }
                if downloaded.len() == missing_files.len() {
                    Some(action_result)
                } else {
                    None
                }
            }
        }
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
        sandbox_dir: Option<&PathBuf>,
        out_dir: &PathBuf,
        file: &OutputFile,
    ) -> Result<(), anyhow::Error> {
        let cache_path = self
            .local_cache
            .move_output_file_into_cache(sandbox_dir, out_dir, file)
            .await?;
        if let Some(remote_cache) = &self.remote_cache {
            remote_cache.push_blob(file.digest.clone().unwrap(), cache_path);
        }
        Ok(())
    }

    pub async fn symlink_output_files_into_out_dir(
        &self,
        output_files: &Vec<OutputFile>,
        out_dir: &Path,
    ) -> Result<(), anyhow::Error> {
        self.local_cache
            .symlink_output_files_into_out_dir(output_files, out_dir)
            .await
    }
}

pub type MessageDigest = Digest;
pub type BlobDigest = Digest;

impl Digest {
    pub async fn for_file(file: File) -> Result<BlobDigest, anyhow::Error> {
        use sha2::Digest;
        let mut reader = BufReader::new(file);
        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];
        let mut len = 0;
        loop {
            let count = reader.read(&mut buffer).await?;
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

    pub async fn for_path(path: impl AsRef<Path> + Debug) -> Result<BlobDigest, anyhow::Error> {
        let file = File::open(&path)
            .await
            .with_context(|| format!("Digest::for_path() {path:?}"))?;
        Self::for_file(file)
            .await
            .with_context(|| format!("Digest::for_file(): {path:?}"))
    }

    pub fn for_bytes(bytes: impl AsRef<[u8]>) -> MessageDigest {
        use sha2::Digest;
        bazel_remote_exec::Digest {
            hash: Self::hex(&Sha256::digest(bytes.as_ref())),
            size_bytes: bytes.as_ref().len() as i64,
        }
    }

    pub fn for_message<T: prost::Message>(msg: &T) -> MessageDigest {
        Self::for_bytes(message_to_pb_buf(msg))
    }

    pub fn for_string(text: &String) -> MessageDigest {
        Self::for_bytes(text.as_bytes())
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
        let act = super::Digest::for_path(&path).await.unwrap();
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
        let act = super::Digest::for_path(&path).await.unwrap();
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
