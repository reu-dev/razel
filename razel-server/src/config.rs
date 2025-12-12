use anyhow::{anyhow, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: HashMap<String, Node>,
}

/// A razel-server process.
///
/// Multiple roles are supported at the same time:
/// Schedulers take jobs from razel clients and distribute them to workers.
/// Schedulers also store job statistics and serve the webui.
///
/// A physical machine can host workers in multiple virtual machines (e.g. one Linux, one Windows).
/// In this case the VM CPU resources should not be limited and `physical_machine`should be set properly
/// to allow Razel to distribute the workload as needed across the workers.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub physical_machine: Option<String>,
    pub server_endpoint: Option<Endpoint>,
    pub max_parallelism: Option<usize>,
    pub scheduler: Option<Scheduler>,
    pub worker: Option<Worker>,
    pub storage: Storage,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub port: u16,
    pub tls: Option<Tls>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scheduler {
    pub client_endpoint: Endpoint,
    pub webui: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub tags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub path: PathBuf,
    pub max_size_gb: Option<usize>,
}

impl Config {
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents =
            std::fs::read_to_string(path).with_context(|| anyhow!("failed to read {path:?}"))?;
        let config: Config =
            toml::from_str(&contents).map_err(|e| anyhow!("failed to parse {path:?}\n{e}"))?;
        config
            .check()
            .with_context(|| anyhow!("failed to validate {path:?}"))?;
        Ok(config)
    }

    pub fn check(&self) -> Result<()> {
        anyhow::ensure!(!self.node.is_empty(), "there should be at least one node");
        for (node_name, node) in &self.node {
            if let Some(scheduler) = &node.scheduler {
                anyhow::ensure!(
                    node.server_endpoint.is_some(),
                    "scheduler on {node_name} needs a server_endpoint"
                );
                ensure!(
                    scheduler.client_endpoint.port != node.server_endpoint.as_ref().unwrap().port,
                    "scheduler on {node_name} needs different port for client_endpoint"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod main {
    use super::*;

    #[test]
    fn example_localhost() {
        let contents = include_str!("../examples/localhost.toml");
        let config: Config = match toml::from_str(contents) {
            Ok(config) => config,
            Err(e) => panic!("{e}"),
        };
        config.check().unwrap();
    }

    #[test]
    fn example_multi_node() {
        let contents = include_str!("../examples/multi_node.toml");
        let config: Config = match toml::from_str(contents) {
            Ok(config) => config,
            Err(e) => panic!("{e}"),
        };
        config.check().unwrap();
    }
}
