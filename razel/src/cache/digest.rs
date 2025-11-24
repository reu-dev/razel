use crate::bazel_remote_exec::message_to_pb_buf;
use crate::types::Digest;
use anyhow::{Context, Result};
use sha2::Sha256;
use std::fmt::Debug;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

pub type MessageDigest = Digest;
pub type BlobDigest = Digest;

impl Digest {
    pub async fn for_file(file: File) -> Result<Self> {
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
        Ok(Self {
            hash: Self::hex(&hasher.finalize()),
            size_bytes: len as i64,
        })
    }

    pub async fn for_path(path: impl AsRef<Path> + Debug) -> Result<Self> {
        let file = File::open(&path)
            .await
            .with_context(|| format!("Digest::for_path() {path:?}"))?;
        Self::for_file(file)
            .await
            .with_context(|| format!("Digest::for_file(): {path:?}"))
    }

    pub fn for_bytes(bytes: impl AsRef<[u8]>) -> Self {
        use sha2::Digest;
        Self {
            hash: Self::hex(&Sha256::digest(bytes.as_ref())),
            size_bytes: bytes.as_ref().len() as i64,
        }
    }

    pub fn for_message<T: prost::Message>(msg: &T) -> Self {
        Self::for_bytes(message_to_pb_buf(msg))
    }

    pub fn for_string(text: &str) -> Self {
        Self::for_bytes(text.as_bytes())
    }

    fn hex(input: &[u8]) -> String {
        base16ct::lower::encode_string(input)
    }
}

pub trait DigestData {
    fn hash(&self) -> &String;
    fn size(&self) -> i64;
}

impl DigestData for Digest {
    fn hash(&self) -> &String {
        &self.hash
    }

    fn size(&self) -> i64 {
        self.size_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_file_sha256_simple(path: impl AsRef<Path>) -> Result<Digest> {
        use sha2::Digest;
        let bytes = std::fs::read(path)?;
        Ok(super::Digest {
            hash: super::Digest::hex(&Sha256::digest(&bytes)),
            size_bytes: bytes.len() as i64,
        })
    }

    #[tokio::test]
    async fn digest_for_small_file() {
        let path = "../examples/data/a.csv";
        let act = Digest::for_path(&path).await.unwrap();
        let exp = digest_file_sha256_simple(path).unwrap();
        assert_eq!(act, exp);
        // check vs: sha256sum examples/data/a.csv line endings
        if act.size_bytes == 18 {
            // examples/data/a.csv has CRLF
            assert_eq!(
                act,
                Digest {
                    hash: "11f5756d3300e967b28969ee86532fe891b0ea42e5ba843bc212fe444cf0f37d".into(),
                    size_bytes: 18,
                }
            );
        } else {
            // examples/data/a.csv has LF line endings
            assert_eq!(
                act,
                Digest {
                    hash: "e0f702d446912234e5767af1db3f8b23b04beade5cdd1ea72d78c4f88c869b80".into(),
                    size_bytes: 16,
                }
            );
        }
    }

    #[tokio::test]
    async fn digest_for_bigger_file() {
        let path = "../Cargo.lock";
        let act = Digest::for_path(&path).await.unwrap();
        let exp = digest_file_sha256_simple(path).unwrap();
        assert_eq!(act, exp);
    }

    #[test]
    fn digest_for_string() {
        assert_eq!(
            Digest::for_string("Hello World!"),
            Digest {
                // echo -n "Hello World!" | sha256sum
                hash: "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069".into(),
                size_bytes: 12,
            }
        );
    }
}
