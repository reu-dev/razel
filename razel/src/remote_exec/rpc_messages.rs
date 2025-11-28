use crate::cache::MessageDigest;
use crate::executors::ExecutionResult;
use crate::remote_exec::{Job, JobId};
use crate::types::{Digest, File, Target};
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MessageVersion {
    Unknown = 0x00,
    ClientServerV1 = 0x01,
    ServerServerV1 = 0x81,
}

impl From<u8> for MessageVersion {
    fn from(v: u8) -> Self {
        match v {
            x if x == Self::ClientServerV1 as u8 => Self::ClientServerV1,
            x if x == Self::ServerServerV1 as u8 => Self::ServerServerV1,
            _ => MessageVersion::Unknown,
        }
    }
}

/// Messages exchanged between client and server
#[derive(Serialize, Deserialize)]
pub enum ClientMessage {
    CreateJobRequest(CreateJobRequest),
    CreateJobResponse(CreateJobResponse),
    ExecuteTargetsRequest(ExecuteTargetsRequest),
    ExecuteTargetResult(ExecuteTargetResult),
    UploadFilesRequest(UploadFilesRequest),
}

/// Send by client to server to register a job
#[derive(Serialize, Deserialize)]
pub struct CreateJobRequest {
    pub job: Job,
    pub auth: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateJobResponse {
    pub id: JobId,
    /// Job in webui
    pub url: String,
}

/// Push additional targets for execution
#[derive(Serialize, Deserialize)]
pub struct ExecuteTargetsRequest {
    pub targets: Vec<Target>,
    pub files: Vec<File>,
}

#[derive(Serialize, Deserialize)]
pub struct ExecuteTargetResult {
    action_digest: MessageDigest,
    result: ExecutionResult,
    output_files: Vec<File>,
}

/// Send by server to client for input files missing in CAS
#[derive(Serialize, Deserialize)]
pub struct UploadFilesRequest {
    pub digests: Vec<Digest>,
}

#[derive(Serialize, Deserialize)]
pub struct UploadFile {
    pub digest: Digest,
    pub contents: Vec<u8>,
}
