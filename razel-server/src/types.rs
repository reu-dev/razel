use quinn::Connection;
use razel::types::WorkerTag;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    pub server_port: u16,
    pub client_port: Option<u16>,
    pub physical_machine: String,
    pub max_parallelism: usize,
    pub tags: Vec<WorkerTag>,
}

pub type RemoteNodeId = usize;

pub struct RemoteNode {
    pub id: RemoteNodeId,
    pub connection: Option<Connection>,
    pub node: Node,
}

impl Display for RemoteNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.node.host, self.node.server_port)
    }
}

impl RemoteNode {
    pub fn from_config(mut nodes: HashMap<String, crate::config::Node>) -> Vec<Self> {
        nodes
            .drain()
            .enumerate()
            .filter_map(|(id, (host, node))| {
                node.server_endpoint.map(|e| RemoteNode {
                    id,
                    connection: None,
                    node: Node {
                        host,
                        server_port: e.port,
                        client_port: node.scheduler.as_ref().map(|x| x.client_endpoint.port),
                        physical_machine: node.physical_machine.unwrap_or_default(),
                        max_parallelism: node.max_parallelism.unwrap_or_default(),
                        tags: node.worker.map(|w| w.tags).unwrap_or_default(),
                    },
                })
            })
            .collect()
    }

    pub fn is_same(&self, host: &str, server_port: u16) -> bool {
        self.node.host == host && self.node.server_port == server_port
    }

    pub fn is_same_node(&self, other: &Node) -> bool {
        self.is_same(&other.host, other.server_port)
    }
}
