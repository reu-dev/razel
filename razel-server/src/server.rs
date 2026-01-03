use crate::config::Config;
use crate::rpc_endpoint::new_server_endpoint;
use crate::rpc_messages::ServerMessage;
use crate::{JobWorker, Node};
use anyhow::{Context, Result, anyhow, bail};
use quinn::{Connection, Endpoint};
use razel::remote_exec::rpc_endpoint::new_client_endpoint;
use razel::remote_exec::{ClientToServerMsg, ExecuteTargetResult, ExecuteTargetsRequest};
use razel::types::DigestHash;
use std::env::current_dir;
use std::fs::create_dir_all;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;
use tracing::{info, instrument};

pub type Tx = mpsc::UnboundedSender<QueueMsg>;
type Rx = mpsc::UnboundedReceiver<QueueMsg>;
type RemoteNodeId = usize;
type ClientId = usize;

pub enum QueueMsg {
    IncomingClientConnection(quinn::Connection),
    ClientConnectionLost(ClientId),
    IncomingServerConnection(quinn::Connection),
    ServerConnectionLost(RemoteNodeId),
    OutgoingConnection((RemoteNodeId, quinn::Connection)),
    ClientMsg((ClientId, ClientToServerMsg, quinn::SendStream)),
    ServerMsg((RemoteNodeId, ServerMessage, quinn::SendStream)),
    ExecuteTargetsRequest(ExecuteTargetsRequest),
    ExecuteTargetResult(ExecuteTargetResult),
    RequestFileFinished(String),
    RequestFileFailed((String, String)),
}

pub struct Server {
    #[allow(dead_code)]
    node: Node,
    client_endpoint: Option<Endpoint>,
    server_endpoint: Endpoint,
    storage: Storage,
    /// other servers to connect to
    nodes: Vec<RemoteNode>,
    scheduler: Option<Scheduler>,
    clients: HashMap<ClientId, ClientConnection>,
    next_client_id: ClientId,
    tx: Tx,
    rx: Rx,
}

impl Server {
    pub fn new(mut config: Config, name: String) -> Result<Self> {
        let Some(mut self_config) = config.node.remove(&name) else {
            bail!("config missing for node name: {name}");
        };
        if self_config.storage.path.is_relative() {
            let cwd = current_dir().context("failed to get current_dir")?;
            self_config.storage.path = cwd.join(&self_config.storage.path).to_path_buf();
        }
        create_dir_all(&self_config.storage.path)?;
        let client_endpoint = if let Some(x) =
            self_config.scheduler.as_ref().map(|x| &x.client_endpoint)
        {
            Some(new_server_endpoint(x).with_context(|| format!("endpoint for client: {x:?}"))?)
        } else {
            None
        };
        let server_endpoint = if let Some(x) = &self_config.server_endpoint {
            new_server_endpoint(x).with_context(|| format!("endpoint for servers: {x:?}"))?
        } else {
            new_client_endpoint().context("endpoint for servers")?
        };
        let available_parallelism: usize = std::thread::available_parallelism().unwrap().into();
        let node = Node {
            host: name.clone(),
            client_port: client_endpoint
                .as_ref()
                .map(|x| x.local_addr().unwrap().port()),
            server_port: self_config
                .server_endpoint
                .as_ref()
                .map(|_| server_endpoint.local_addr().unwrap().port()),
            physical_machine: self_config
                .physical_machine
                .map_or(name.clone(), |x| x.clone()),
            max_parallelism: self_config
                .max_parallelism
                .map_or(available_parallelism, |max| max.min(available_parallelism)),
        };
        let nodes = config
            .node
            .drain()
            .enumerate()
            .filter_map(|(id, (host, node))| {
                node.server_endpoint.map(|e| RemoteNode {
                    id,
                    host,
                    port: e.port,
                    connection: None,
                })
            })
            .collect();
        let storage = Storage::new(self_config.storage.path, self_config.storage.max_size_gb)?;
        let scheduler = self_config.scheduler.map(|_| Scheduler::new());
        let (tx, rx) = mpsc::unbounded_channel();
        Ok(Self {
            node,
            client_endpoint,
            server_endpoint,
            storage,
            nodes,
            scheduler,
            clients: Default::default(),
            next_client_id: 0,
            tx,
            rx,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        self.connect_to_servers();
        self.accept_incoming_server_connections();
        self.accept_incoming_client_connections();
        while let Some(m) = self.rx.recv().await {
            self.handle_queue_msg(m).await?;
        }
        Ok(())
    }

    // TODO drop async
    async fn handle_queue_msg(&mut self, queue_msg: QueueMsg) -> Result<()> {
        match queue_msg {
            QueueMsg::IncomingClientConnection(c) => {
                let id = self.next_client_id;
                self.next_client_id += 1;
                info!(id, "IncomingClientConnection");
                self.clients.insert(
                    id,
                    ClientConnection {
                        connection: c.clone(),
                    },
                );
                let tx = self.tx.clone();
                tokio::spawn(async move { handle_client_connection(id, c, tx).await });
            }
            QueueMsg::ClientConnectionLost(id) => {
                info!(id, "ClientConnectionLost");
                self.clients.remove(&id);
            }
            QueueMsg::IncomingServerConnection(_) => todo!(),
            QueueMsg::ServerConnectionLost(_) => todo!(),
            QueueMsg::OutgoingConnection((i, c)) => {
                self.nodes[i].connection = Some(c.clone());
                let tx = self.tx.clone();
                tokio::spawn(async move { handle_server_connection(i, c, tx).await });
            }
            QueueMsg::ClientMsg((id, msg, send)) => self.handle_client_msg(id, msg, send)?,
            QueueMsg::ServerMsg(_) => todo!(),
            QueueMsg::ExecuteTargetsRequest(_) => todo!(),
            QueueMsg::ExecuteTargetResult(m) => self.handle_execute_target_result(m),
            QueueMsg::RequestFileFinished(hash) => self.handle_request_file_finished(hash).await,
            QueueMsg::RequestFileFailed((hash, err)) => self.handle_request_file_failed(hash, err),
        }
        Ok(())
    }

    pub fn handle_client_msg(
        &mut self,
        client: ClientId,
        msg: ClientToServerMsg,
        send: quinn::SendStream,
    ) -> Result<()> {
        match msg {
            ClientToServerMsg::CreateJobRequest(r) => {
                self.handle_create_job_request(client, send, r)?
            }
            ClientToServerMsg::ExecuteTargetsRequest(r) => {
                self.handle_execute_targets_request(r)?
            }
            ClientToServerMsg::UploadFile => todo!(),
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RemoteNode {
    id: RemoteNodeId,
    host: String,
    port: u16,
    connection: Option<Connection>,
}

impl RemoteNode {
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        std::net::ToSocketAddrs::to_socket_addrs(&(self.host.as_ref(), self.port))?
            .next()
            .ok_or_else(|| anyhow!("couldn't resolve address"))
    }
}

struct ClientConnection {
    connection: quinn::Connection,
}

mod connections;
use connections::*;
mod scheduler;
use scheduler::*;
mod storage;
use storage::*;
