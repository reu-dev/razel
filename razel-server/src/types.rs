use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    pub client_port: Option<u16>,
    pub server_port: Option<u16>,
    pub physical_machine: String,
    pub max_parallelism: usize,
}
