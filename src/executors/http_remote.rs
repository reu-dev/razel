use crate::executors::{ExecutionResult, ExecutionStatus};
use anyhow::anyhow;
use reqwest::{RequestBuilder, Url};
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Not;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs;

type Domain = String;
type Host = String;
type Slots = usize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct HttpRemoteExecConfig(pub HashMap<Domain, HashMap<Host, Slots>>);

impl std::str::FromStr for HttpRemoteExecConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub struct HttpRemoteExecutor {
    pub url: Url,
    pub files: Vec<(String, PathBuf)>,
    pub client: Option<reqwest::Client>,
}

impl HttpRemoteExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let execution_start = Instant::now();
        let request = match self.build_request().await {
            Ok(x) => x,
            Err(x) => {
                return ExecutionResult {
                    status: ExecutionStatus::FailedToStart,
                    error: Some(x),
                    ..Default::default()
                }
            }
        };
        let response = match request.send().await {
            Ok(x) => x,
            Err(x) => {
                return ExecutionResult {
                    status: ExecutionStatus::FailedToSendRequest,
                    error: Some(x.into()),
                    ..Default::default()
                }
            }
        };
        let exec_duration = Some(execution_start.elapsed());
        let status = response.status();
        let text = match response.text().await {
            Ok(x) => x,
            Err(x) => {
                return ExecutionResult {
                    status: ExecutionStatus::FailedToParseResponse,
                    error: Some(x.into()),
                    ..Default::default()
                }
            }
        };
        ExecutionResult {
            status: if status.is_success() {
                ExecutionStatus::Success
            } else {
                ExecutionStatus::Failed
            },
            error: status.is_success().not().then(|| anyhow!(status)),
            stdout: text.into_bytes(),
            exec_duration,
            ..Default::default()
        }
    }

    async fn build_request(&self) -> Result<RequestBuilder, anyhow::Error> {
        let client = self.client.clone().unwrap_or_default();
        let mut form = reqwest::multipart::Form::new();
        for (name, path) in &self.files {
            let bytes = fs::read(path).await?;
            let part = reqwest::multipart::Part::bytes(bytes).file_name(name.clone());
            form = form.part(name.clone(), part);
        }
        Ok(client.post(self.url.clone()).multipart(form))
    }
}
