use crate::webui_types::NodeStats;
use quinn::Connection;
use std::collections::HashMap;
use std::fmt::Display;

pub type Node = crate::webui_types::Node;
pub type RemoteNodeId = usize;

pub struct RemoteNode {
    pub id: RemoteNodeId,
    pub connection: Option<Connection>,
    pub node: Node,
    /// stats received from remote node
    pub stats: Option<NodeStats>,
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
            .filter_map(|(id, (host, config_node))| {
                config_node.server_endpoint.map(|e| {
                    let node = Node {
                        host,
                        server_port: e.port,
                        client_port: config_node
                            .scheduler
                            .as_ref()
                            .map(|x| x.client_endpoint.port),
                        physical_machine: config_node.physical_machine.unwrap_or_default(),
                        max_cpu_slots: config_node.max_cpu_slots.unwrap_or_default(),
                        tags: config_node.worker.map(|w| w.tags).unwrap_or_default(),
                        storage_max_size_gb: config_node.storage.max_size_gb,
                    };
                    RemoteNode {
                        id,
                        connection: None,
                        node,
                        stats: None,
                    }
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
