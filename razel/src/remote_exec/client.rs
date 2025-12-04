use crate::remote_exec::rpc_endpoint::new_client_endpoint;
use crate::remote_exec::*;
use crate::types::File;
use crate::types::Target;
use anyhow::{anyhow, bail, Result};
use quinn::Connection;
use quinn::Endpoint;
use rand::rng;
use rand::seq::SliceRandom;
use std::net::ToSocketAddrs;
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;
use url::Url;
use uuid::Uuid;

pub struct Client {
    pub url: Url,
    endpoint: Endpoint,
    connection: Connection,
    job_id: Option<Uuid>,
}

impl Client {
    pub async fn new(mut urls: Vec<Url>) -> Result<Self> {
        let endpoint = new_client_endpoint()?;
        urls.shuffle(&mut rng());
        for url in urls {
            match Self::connect(&endpoint, &url).await {
                Ok(connection) => {
                    tracing::info!("connected to {}", url.as_str());
                    return Ok(Self {
                        url,
                        endpoint,
                        connection,
                        job_id: None,
                    });
                }
                Err(e) => {
                    tracing::info!("failed to connect to server {}: {e}", url.as_str());
                }
            }
        }
        endpoint.wait_idle().await;
        bail!("failed to connect to remote executors")
    }

    async fn connect(endpoint: &Endpoint, url: &Url) -> Result<Connection> {
        let host = strip_ipv6_brackets(url.host_str().unwrap());
        let addr = (host, url.port().unwrap_or(4433))
            .to_socket_addrs()?
            .into_iter()
            .find(|x| x.is_ipv4())
            .ok_or_else(|| anyhow!("couldn't resolve address"))?;
        let connection = endpoint.connect(addr, host)?.await?;
        Ok(connection)
    }

    pub async fn create_job(&mut self) -> Result<CreateJobResponse> {
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
        let ServerToClientMsg::CreateJobResponse(response) =
            ClientToServerMsg::CreateJobRequest(CreateJobRequest {
                job: Job {
                    ts: chrono::Utc::now(),
                    project: "".to_string(),
                    kind,
                },
                auth: "".to_string(),
            })
            .request(&self.connection)
            .await?
        else {
            bail!("unexpected response type");
        };
        tracing::info!(job = ?response.job_id);
        self.job_id = Some(response.job_id);
        Ok(response)
    }

    #[instrument(skip_all)]
    pub fn spawn_exec(
        &self,
        targets: Vec<Target>,
        files: Vec<File>,
        keep_going: bool,
        tx: UnboundedSender<ClientChannelMsg>,
    ) {
        let connection = self.connection.clone();
        let job_id = self.job_id.unwrap();
        tokio::spawn(async move {
            if let Err(e) =
                Self::spawn_exec_impl(connection, job_id, targets, files, keep_going, tx).await
            {
                tracing::warn!("{e:?}");
                todo!("{e:?}"); // TODO handle losing connection to server
            }
        });
    }

    #[instrument(skip_all)]
    async fn spawn_exec_impl(
        connection: Connection,
        job_id: JobId,
        targets: Vec<Target>,
        files: Vec<File>,
        keep_going: bool,
        tx: UnboundedSender<ClientChannelMsg>,
    ) -> Result<()> {
        let (mut send, mut recv) = connection.open_bi().await?;
        ClientToServerMsg::ExecuteTargetsRequest(ExecuteTargetsRequest {
            job_id,
            targets,
            files,
            keep_going,
        })
        .send(&mut send)
        .await?;
        send.finish()?;
        loop {
            tracing::warn!("loop");
            match ServerToClientMsg::recv(&mut recv).await? {
                ServerToClientMsg::CreateJobResponse(_) => {
                    unreachable!("CreateJobResponse should be handled before starting execution")
                }
                ServerToClientMsg::ExecuteTargetResult(r) => {
                    tx.send(ClientChannelMsg::Result(r)).ok();
                }
                ServerToClientMsg::ExecuteStats(s) => {
                    tx.send(ClientChannelMsg::Stats(s)).ok();
                }
                ServerToClientMsg::ExecuteTargetsFinished => {
                    tx.send(ClientChannelMsg::Finished).ok();
                    break;
                }
                ServerToClientMsg::UploadFilesRequest(_) => todo!(),
            }
        }
        tracing::warn!("finished");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn close(self, reason: &str) {
        tracing::info!("");
        self.connection.close(0u32.into(), reason.as_bytes());
        self.endpoint.wait_idle().await;
    }
}

pub enum ClientChannelMsg {
    Result(ExecuteTargetResult),
    Stats(ExecuteStats),
    Finished,
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
