#![allow(clippy::all, dead_code)]

use crate::cache::DigestData;
use crate::types::{Digest, ExecutableType, Target, TargetKind};
use anyhow::Result;
pub use build::bazel::remote::execution::v2::{
    Action, ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, Command, Directory,
    ExecutedActionMetadata, FileNode, GetActionResultRequest, GetCapabilitiesRequest, OutputFile,
    Platform, ServerCapabilities, UpdateActionResultRequest,
    action_cache_client::ActionCacheClient, batch_update_blobs_request,
    capabilities_client::CapabilitiesClient, command::EnvironmentVariable,
    content_addressable_storage_client::ContentAddressableStorageClient, digest_function,
    platform::Property,
};
pub use google::bytestream::{ReadRequest, WriteRequest, byte_stream_client::ByteStreamClient};
use itertools::{Itertools, chain};
use tokio::fs::File;

pub type BazelDigest = build::bazel::remote::execution::v2::Digest;
pub type BazelMessageDigest = BazelDigest;
pub type BazelBlobDigest = BazelDigest;
pub type Duration = prost_types::Duration;

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

    pub fn for_string(text: &str) -> Self {
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

pub fn bzl_action_for_target(
    target: &Target,
    files: &Vec<crate::types::File>,
    executor_version: Option<&Digest>,
) -> (Command, Directory) {
    let bzl_command = Command {
        arguments: target.kind.args_with_executable(),
        environment_variables: target
            .kind
            .env()
            .map(|x| {
                x.clone()
                    .into_iter()
                    .map(|(name, value)| EnvironmentVariable { name, value })
                    .sorted_unstable_by(|a, b| Ord::cmp(&a.name, &b.name))
                    .collect()
            })
            .unwrap_or_default(),
        output_paths: target
            .outputs
            .iter()
            .map(|x| files[*x].path.to_str().unwrap())
            .sorted_unstable()
            .dedup()
            .map_into()
            .collect(),
        working_directory: "".to_string(),
        ..Default::default()
    };
    let executables = target
        .executables
        .iter()
        .filter(|x| files[**x].executable != Some(ExecutableType::SystemExecutable));
    // TODO properly build bazel_remote_exec::Directory tree
    let bzl_input_root = Directory {
        files: chain(executables, target.inputs.iter())
            .map(|x| {
                let file = &files[*x];
                assert!(
                    file.digest.is_some(),
                    "digest missing for file id={} path={:?}",
                    file.id,
                    file.path
                );
                FileNode {
                    name: file.path.to_str().unwrap().into(),
                    digest: Some(file.digest.as_ref().unwrap().into()),
                    is_executable: file.executable.is_some(),
                    node_properties: None,
                }
            })
            .chain(executor_version.map(|x| FileNode {
                name: "razel".to_string(),
                digest: Some(x.into()),
                is_executable: true,
                node_properties: None,
            }))
            .sorted_unstable_by(|a, b| Ord::cmp(&a.name, &b.name))
            .collect(),
        directories: vec![],
        symlinks: vec![],
        node_properties: None,
    };
    (bzl_command, bzl_input_root)
}

pub fn bzl_platform_for_target(kind: &TargetKind) -> Option<Platform> {
    match kind {
        TargetKind::Command(_) => {
            let properties = [
                ("ISA", std::env::consts::ARCH),
                ("OSFamily", std::env::consts::OS),
            ]
            .into_iter()
            .map(|(name, value)| Property {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect();
            Some(Platform { properties })
        }
        TargetKind::Wasi(_) | TargetKind::Task(_) | TargetKind::HttpRemoteExecTask(_) => {
            // environment-independent
            None
        }
    }
}

mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }

    pub mod longrunning {
        tonic::include_proto!("google.longrunning");
    }

    pub mod bytestream {
        tonic::include_proto!("google.bytestream");
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
