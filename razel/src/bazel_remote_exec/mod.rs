#![allow(clippy::all, dead_code)]

use crate::cache::DigestData;
use crate::types::Digest;
pub use build::bazel::remote::execution::v2::{
    action_cache_client::ActionCacheClient, batch_update_blobs_request,
    capabilities_client::CapabilitiesClient, command::EnvironmentVariable,
    content_addressable_storage_client::ContentAddressableStorageClient, digest_function, Action,
    ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, Command, Directory,
    ExecutedActionMetadata, FileNode, GetActionResultRequest, GetCapabilitiesRequest, OutputFile,
    ServerCapabilities, UpdateActionResultRequest,
};
use tokio::fs::File;
use anyhow::Result;

pub type BazelDigest = build::bazel::remote::execution::v2::Digest;
pub type BazelMessageDigest = BazelDigest;
pub type BazelBlobDigest = BazelDigest;

impl BazelDigest {
    pub async fn for_file(file: File) -> Result<Self> {
        Ok(Digest::for_file(file).await?.into())
    }

    pub fn for_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Digest::for_bytes(bytes).into()
    }

    pub fn for_message<T: prost::Message>(msg: &T) -> Self {
        Self::for_bytes(message_to_pb_buf(msg))
    }

    pub fn for_string(text: &String) -> Self {
        Self::for_bytes(text.as_bytes())
    }
}

impl DigestData for BazelDigest {
    fn hash(&self) -> &String {
        &self.hash
    }

    fn size(&self) -> i64 {
        self.size_bytes
    }
}

impl From<Digest> for BazelDigest {
    fn from(value: Digest) -> Self {
        Self {
            hash: value.hash,
            size_bytes: value.size_bytes,
        }
    }
}

impl From<&Digest> for BazelDigest {
    fn from(value: &Digest) -> Self {
        Self {
            hash: value.hash.clone(),
            size_bytes: value.size_bytes,
        }
    }
}

impl From<BazelDigest> for Digest {
    fn from(value: BazelDigest) -> Self {
        Self {
            hash: value.hash,
            size_bytes: value.size_bytes,
        }
    }
}

impl From<&BazelDigest> for Digest {
    fn from(value: &BazelDigest) -> Self {
        Self {
            hash: value.hash.clone(),
            size_bytes: value.size_bytes,
        }
    }
}

pub fn message_to_pb_buf<T: prost::Message>(msg: &T) -> Vec<u8> {
    let mut vec = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut vec).unwrap();
    vec
}

mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }

    pub mod longrunning {
        tonic::include_proto!("google.longrunning");
    }

    #[cfg(not(doctest))]
    mod api {
        tonic::include_proto!("google.api");
    }
}

mod build {
    pub mod bazel {
        mod semver {
            tonic::include_proto!("build.bazel.semver");
        }

        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    tonic::include_proto!("build.bazel.remote.execution.v2");
                }
            }
        }
    }
}
