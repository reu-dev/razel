use crate::executors::{ExecutionResult, ExecutionStatus};
use anyhow::anyhow;
use itertools::Itertools;
use log::warn;
use reqwest::{multipart, Client, Url};
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Not;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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

#[derive(Default)]
pub struct HttpRemoteExecState {
    domains: Vec<Arc<HttpRemoteExecDomain>>,
}

impl HttpRemoteExecState {
    pub fn new(config: &HttpRemoteExecConfig) -> Self {
        let domains = config
            .0
            .iter()
            .filter(|(_, hosts_with_slots)| !hosts_with_slots.is_empty())
            .map(|(domain, hosts_with_slots)| {
                let hosts = hosts_with_slots
                    .iter()
                    .map(|(host, &available_slots)| {
                        let (host, port) =
                            host.split_once(':').map_or((host.clone(), None), |(h, p)| {
                                (h.into(), Some(p.parse().unwrap()))
                            });
                        HttpRemoteExecHost {
                            host,
                            port,
                            client: Default::default(),
                            available_slots,
                            used_slots: Default::default(),
                            is_ok: AtomicBool::new(true),
                        }
                    })
                    .collect_vec();
                let available_slots = hosts.iter().map(|x| x.available_slots).sum();
                Arc::new(HttpRemoteExecDomain {
                    domain: domain.clone(),
                    hosts,
                    available_slots: AtomicUsize::new(available_slots),
                    scheduled_slots: Mutex::new(0),
                })
            })
            .collect();
        Self { domains }
    }

    pub fn for_url(&self, url: &Url) -> Option<Arc<HttpRemoteExecDomain>> {
        let domain = url.domain()?;
        self.domains.iter().find(|x| x.domain == domain).cloned()
    }
}

pub struct HttpRemoteExecDomain {
    domain: String,
    hosts: Vec<HttpRemoteExecHost>,
    available_slots: AtomicUsize,
    scheduled_slots: Mutex<usize>,
}

impl HttpRemoteExecDomain {
    pub fn try_schedule(&self) -> bool {
        let mut scheduled = self.scheduled_slots.lock().unwrap();
        if *scheduled < self.available_slots.load(Ordering::Relaxed) {
            *scheduled += 1;
            true
        } else {
            false
        }
    }

    pub fn unschedule(&self) {
        let mut scheduled = self.scheduled_slots.lock().unwrap();
        assert!(*scheduled > 0);
        *scheduled -= 1;
    }
}

struct HttpRemoteExecHost {
    host: String,
    port: Option<u16>,
    client: Client,
    available_slots: usize,
    used_slots: AtomicUsize,
    /// to ignore host after connection error
    is_ok: AtomicBool,
}

#[derive(Clone)]
pub struct HttpRemoteExecutor {
    pub args: Vec<String>,
    pub state: Option<Arc<HttpRemoteExecDomain>>,
    pub url: Url,
    pub files: Vec<(String, PathBuf)>,
}

impl HttpRemoteExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let result = if let Some(domain) = &self.state {
            self.exec_on_some_host_of_domain(domain).await
        } else {
            self.request(&Default::default(), self.url.clone()).await
        };
        result.unwrap_or_else(|error| ExecutionResult {
            status: ExecutionStatus::SystemError,
            error: Some(error),
            ..Default::default()
        })
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        self.args.clone()
    }

    async fn exec_on_some_host_of_domain(
        &self,
        domain: &Arc<HttpRemoteExecDomain>,
    ) -> anyhow::Result<ExecutionResult> {
        assert!(!domain.hosts.is_empty());
        for host in domain
            .hosts
            .iter()
            .filter(|x| x.is_ok.load(Ordering::Relaxed))
            .sorted_by_key(|x| x.used_slots.load(Ordering::Relaxed) * 100 / x.available_slots)
        {
            if !host.is_ok.load(Ordering::Relaxed) {
                continue;
            }
            host.used_slots.fetch_add(1, Ordering::Relaxed);
            let mut url = self.url.clone();
            url.set_host(Some(&host.host)).unwrap();
            if let Some(port) = host.port {
                url.set_port(Some(port)).unwrap();
            }
            let result = self.request(&host.client, url).await;
            if let Err(err) = &result {
                if host.is_ok.swap(false, Ordering::Relaxed) {
                    domain
                        .available_slots
                        .fetch_sub(host.available_slots, Ordering::Relaxed);
                    warn!("{err}");
                }
            };
            host.used_slots.fetch_sub(1, Ordering::Relaxed);
            if result.is_ok() {
                return result;
            }
        }
        Err(anyhow!(
            "remote exec of {:?} failed on all hosts: {}",
            domain.domain,
            domain.hosts.iter().map(|x| &x.host).join(", ")
        ))
    }

    async fn build_form(&self) -> Result<multipart::Form, anyhow::Error> {
        let mut form = multipart::Form::new();
        for (name, path) in &self.files {
            let bytes = fs::read(path).await?;
            let part = multipart::Part::bytes(bytes).file_name(name.clone());
            form = form.part(name.clone(), part);
        }
        Ok(form)
    }

    async fn request(&self, client: &Client, url: Url) -> anyhow::Result<ExecutionResult> {
        let execution_start = Instant::now();
        let form = self.build_form().await?;
        let response = client.post(url).multipart(form).send().await?;
        let status = response.status();
        let text = response.text().await?;
        Ok(ExecutionResult {
            status: if status.is_success() {
                ExecutionStatus::Success
            } else {
                ExecutionStatus::Failed
            },
            error: status.is_success().not().then(|| anyhow!(status)),
            stdout: text.into_bytes(),
            exec_duration: Some(execution_start.elapsed()),
            ..Default::default()
        })
    }
}
