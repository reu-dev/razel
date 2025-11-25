use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type JobId = String;

/// A razel remote exec job created by `razel exec` and sent to server
#[derive(Serialize, Deserialize)]
pub struct Job {
    pub ts: DateTime<Utc>,
    /// to separate caches per project
    pub project: String,
    pub kind: JobKind,
}

#[derive(Serialize, Deserialize)]
pub enum JobKind {
    Interactive(InteractiveJob),
    /// if GITLAB_CI environment variable is set
    GitLabCi(GitLabCiJob),
}

#[derive(Serialize, Deserialize)]
pub struct InteractiveJob {
    pub user: String,
}

#[derive(Serialize, Deserialize)]
pub struct GitLabCiJob {
    /// GITLAB_USER_LOGIN
    pub user: String,
    /// CI_JOB_URL
    pub job_url: String,
    /// CI_JOB_NAME
    pub job_name: String,
    /// CI_JOB_IMAGE
    pub image: Option<String>,
}
