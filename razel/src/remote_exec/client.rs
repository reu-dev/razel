use crate::remote_exec::rpc_endpoint::new_client_endpoint;
use crate::remote_exec::*;
use anyhow::{anyhow, bail, Result};
use quinn::Connection;
use quinn::Endpoint;
use rand::rng;
use rand::seq::SliceRandom;
use std::net::ToSocketAddrs;
use url::Url;

pub struct Client {
    endpoint: Endpoint,
    connection: Connection,
}

impl Client {
    pub async fn new(mut urls: Vec<Url>) -> Result<Self> {
        let endpoint = new_client_endpoint()?;
        urls.shuffle(&mut rng());
        for url in urls {
            match Self::connect(&endpoint, &url).await {
                Ok(connection) => {
                    return Ok(Self {
                        endpoint,
                        connection,
                    })
                }
                Err(e) => {
                    tracing::info!("failed to connect to {}: {e}", url.as_str());
                }
            }
        }
        endpoint.wait_idle().await;
        bail!("failed to connect to remote executors")
    }

    async fn connect(endpoint: &Endpoint, url: &Url) -> Result<Connection> {
        let host = url.host_str().unwrap();
        let addr = (host, url.port().unwrap_or(4433))
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow!("couldn't resolve address"))?;
        let connection = endpoint.connect(addr, host)?.await?;
        Ok(connection)
    }

    pub async fn create_job(&self) -> Result<CreateJobResponse> {
        let kind = if std::env::var("GITLAB_CI").is_ok() {
            JobKind::GitLabCi(GitLabCiJob {
                user: env_var("GITLAB_USER_LOGIN")?,
                job_url: env_var("CI_JOB_URL")?,
                job_name: env_var("CI_JOB_NAME")?,
                image: std::env::var("GITLAB_CI").ok(),
            })
        } else {
            JobKind::Interactive(InteractiveJob { user: user()? })
        };
        let request = ClientMessage::CreateJobRequest(CreateJobRequest {
            job: Job {
                ts: chrono::Utc::now(),
                project: "".to_string(),
                kind,
            },
            auth: "".to_string(),
        });
        let ClientMessage::CreateJobResponse(response) =
            rpc_request(&self.connection, &request).await?
        else {
            bail!("unexpected response type");
        };
        Ok(response)
    }

    pub async fn close(&mut self) {
        self.connection.close(0u32.into(), b"done");
        self.endpoint.wait_idle().await;
    }
}

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("environment variable missing: {key}"))
}

fn user() -> Result<String> {
    if let Ok(user) = std::env::var("USER") {
        Ok(user)
    } else if let Ok(user) = std::env::var("USERNAME") {
        Ok(user)
    } else {
        bail!("failed to get user from environment");
    }
}
