use crate::cache::MessageDigest;
use crate::executors::ExecutionResult;
use crate::types::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type JobId = Uuid;

#[repr(u8)]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MessageVersion {
    Unknown = 0x00,
    ClientToServerV1 = 0x01,
    ServerToClientV1 = 0x40,
    ServerServerV1 = 0x81,
}

impl From<u8> for MessageVersion {
    fn from(v: u8) -> Self {
        match v {
            x if x == Self::ClientToServerV1 as u8 => Self::ClientToServerV1,
            x if x == Self::ServerToClientV1 as u8 => Self::ServerToClientV1,
            x if x == Self::ServerServerV1 as u8 => Self::ServerServerV1,
            _ => MessageVersion::Unknown,
        }
    }
}

/// Messages send from client to server
#[derive(Serialize, Deserialize)]
pub enum ClientToServerMsg {
    CreateJobRequest(CreateJobRequest),
    ExecuteTargetsRequest(ExecuteTargetsRequest),
}

/// Messages send from server to client
#[derive(Serialize, Deserialize)]
pub enum ServerToClientMsg {
    CreateJobResponse(CreateJobResponse),
    ExecuteTargetResult(ExecuteTargetResult),
    ExecuteStats(ExecuteStats),
    UploadFileRequest(FileId),
}

/// Sent by the client to register a job.
#[derive(Serialize, Deserialize)]
pub struct CreateJobRequest {
    /// JWT (audience `razel`) for CI jobs, opaque string otherwise.
    pub token: String,
    pub kind: JobRequestKind,
    pub default_tags: Vec<WorkerTag>,
    pub docker_image: Option<String>,
    pub docker_pull_credentials: Option<(String, String)>,
}

#[derive(Serialize, Deserialize)]
pub enum JobRequestKind {
    Interactive(InteractiveJobRequest),
    GitLabCi(GitLabJobRequest),
}

#[derive(Serialize, Deserialize)]
pub struct InteractiveJobRequest {
    pub user: String,
    pub project: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct GitLabJobRequest {
    pub job_name: String,
    pub job_url: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateJobResponse {
    pub job_id: JobId,
    /// Job in webui
    pub url: String,
}

/// Push additional targets for execution
#[derive(Serialize, Deserialize)]
pub struct ExecuteTargetsRequest {
    pub job_id: JobId,
    pub targets: Vec<Target>,
    pub files: Vec<File>,
    pub keep_going: bool,
}

#[derive(Serialize, Deserialize)]
pub struct ExecuteTargetResult {
    pub job_id: JobId,
    pub target_id: TargetId,
    pub action_digest: MessageDigest,
    pub result: ExecutionResult,
    pub output_files: Vec<File>,
}

#[derive(Serialize, Deserialize)]
pub struct ExecuteStats {
    pub running: usize,
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
