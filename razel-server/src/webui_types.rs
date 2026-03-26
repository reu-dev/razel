use razel::types::{Job, JobId, WorkerTag};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    pub server_port: u16,
    pub client_port: Option<u16>,
    pub physical_machine: String,
    pub max_cpu_slots: usize,
    pub tags: Vec<WorkerTag>,
    pub storage_max_size_gb: Option<usize>,
}

#[derive(Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub nodes: Vec<(Node, Option<NodeStats>)>,
    pub running_jobs: Vec<RunningJobStats>,
    pub finished_jobs: Vec<FinishedJobStats>,
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeStats {
    pub status: ServerStatus,
    pub server_connections: usize,
    pub client_connections: usize,
    pub storage_used: u64,
    pub jobs_running: usize,
    pub jobs_pending: usize,
    pub cpu_slots: f32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum ServerStatus {
    #[default]
    Unknown,
    Starting,
    Running,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct RunningJobStats {
    pub id: JobId,
    pub job: Job,
    pub node: String,
    pub status: JobStatus,
    pub waiting: usize,
    pub ready: usize,
    pub running: usize,
    pub succeeded: usize,
    pub cached: usize,
    pub failed: usize,
    pub skipped: usize,
    /// sum of exec_duration * cpus over all targets
    pub exec_cpu_secs: f64,
    /// sum of total_duration * cpus over all targets
    pub total_cpu_secs: f64,
    /// total size of all output files and stdout/stderr [bytes]
    pub output_size_bytes: u64,
}

impl RunningJobStats {
    pub fn cache_hit_rate(&self) -> f32 {
        let finished = self.succeeded + self.failed;
        if finished == 0 {
            return 0.0;
        }
        self.cached as f32 / finished as f32
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishedJobStats {
    pub id: JobId,
    pub job: Job,
    pub node: String,
    pub status: JobStatus,
    pub succeeded: usize,
    pub cached: usize,
    pub failed: usize,
    pub skipped: usize,
    /// sum of exec_duration * cpus over all targets
    pub exec_cpu_secs: f64,
    /// sum of total_duration * cpus over all targets
    pub total_cpu_secs: f64,
    /// total size of all output files and stdout/stderr [bytes]
    pub output_size_bytes: u64,
}

impl FinishedJobStats {
    pub fn cache_hit_rate(&self) -> f32 {
        let finished = self.succeeded + self.failed;
        if finished == 0 {
            return 0.0;
        }
        self.cached as f32 / finished as f32
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Success,
    Failed,
    Canceled,
}
