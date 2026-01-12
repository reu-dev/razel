use crate::config::Config;
use crate::rpc_endpoint::new_server_endpoint;
use crate::rpc_messages::{ServerMessage, ServerMessageNodes};
use crate::{JobWorker, Node, RemoteNode, RemoteNodeId};
use anyhow::{Context, Result, anyhow, bail};
use quinn::{Connection, ConnectionError, Endpoint};
use razel::remote_exec::rpc_endpoint::new_client_endpoint;
use razel::remote_exec::{
    ClientToServerMsg, ConnectionCloseCode, ExecuteTargetResult, ExecuteTargetsRequest,
    close_connection,
};
use razel::types::{Digest, WorkerTag};
use std::collections::HashMap;
use std::env::current_dir;
use std::fs::create_dir_all;
use tokio::sync::mpsc;
use tracing::{info, instrument};

pub type Tx = mpsc::UnboundedSender<QueueMsg>;
type Rx = mpsc::UnboundedReceiver<QueueMsg>;
type ClientId = usize;

pub enum QueueMsg {
    IncomingClientConnection(quinn::Connection),
    ClientConnectionLost(ClientId),
    IncomingServerConnection((quinn::Connection, String, u16)),
    OutgoingServerConnection((RemoteNodeId, quinn::Connection)),
    ServerConnectionLost(RemoteNodeId),
    ClientMsg((ClientId, ClientToServerMsg, quinn::SendStream)),
    ServerMsgUni((RemoteNodeId, ServerMessage)),
    ServerMsgBi((RemoteNodeId, ServerMessage, quinn::SendStream)),
    ExecuteTargetsRequest(ExecuteTargetsRequest),
    ExecuteTargetResult(ExecuteTargetResult),
    RequestFileFinished(Digest),
    RequestFileFailed((Digest, String)),
}

pub struct Server {
    #[allow(dead_code)]
    node: Node,
    client_endpoint: Option<Endpoint>,
    server_endpoint: Endpoint,
    storage: Storage,
    remote_nodes: Vec<RemoteNode>,
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
        let client_endpoint =
            if let Some(x) = self_config.scheduler.as_ref().map(|x| &x.client_endpoint) {
                Some(new_server_endpoint(x).map_err(|e| anyhow!("client port={}: {e:?}", x.port))?)
            } else {
                None
            };
        let server_endpoint = if let Some(x) = &self_config.server_endpoint {
            new_server_endpoint(x).map_err(|e| anyhow!("server port={}: {e:?}", x.port))?
        } else {
            new_client_endpoint().context("endpoint for servers")?
        };
        let available_parallelism: usize = std::thread::available_parallelism().unwrap().into();
        let node = Node {
            host: name.clone(),
            server_port: self_config
                .server_endpoint
                .as_ref()
                .map_or(0, |_| server_endpoint.local_addr().unwrap().port()),
            client_port: client_endpoint
                .as_ref()
                .map(|x| x.local_addr().unwrap().port()),
            physical_machine: self_config
                .physical_machine
                .map_or(name.clone(), |x| x.clone()),
            max_parallelism: self_config
                .max_parallelism
                .map_or(available_parallelism, |max| max.min(available_parallelism)),
            tags: self_config
                .worker
                .as_ref()
                .map(|w| {
                    if w.tags.is_empty() {
                        WorkerTag::local_default_tags()
                    } else {
                        w.tags.clone()
                    }
                })
                .unwrap_or_default(),
        };
        if self_config.worker.is_some() {
            info!(max_parallelism=node.max_parallelism, tags=?node.tags, "local worker");
        }
        let storage = Storage::new(self_config.storage.path, self_config.storage.max_size_gb)?;
        let scheduler = self_config
            .scheduler
            .map(|_| Scheduler::new(node.max_parallelism));
        let (tx, rx) = mpsc::unbounded_channel();
        Ok(Self {
            node,
            client_endpoint,
            server_endpoint,
            storage,
            remote_nodes: RemoteNode::from_config(config.node),
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
            QueueMsg::IncomingClientConnection(c) => self.handle_incoming_client_connection(c),
            QueueMsg::ClientConnectionLost(id) => {
                self.handle_client_connection_lost(id);
            }
            QueueMsg::IncomingServerConnection((c, h, p)) => {
                self.handle_incoming_server_connection(c, h, p)
            }
            QueueMsg::OutgoingServerConnection((i, c)) => {
                self.handle_outgoing_server_connection(i, c)
            }
            QueueMsg::ServerConnectionLost(id) => self.handle_server_connection_lost(id),
            QueueMsg::ClientMsg((id, msg, send)) => self.handle_client_msg(id, msg, send).await?,
            QueueMsg::ServerMsgUni((id, msg)) => self.handle_server_msg_uni(id, msg)?,
            QueueMsg::ServerMsgBi((id, msg, send)) => self.handle_server_msg_bi(id, msg, send)?,
            QueueMsg::ExecuteTargetsRequest(_) => todo!(),
            QueueMsg::ExecuteTargetResult(m) => self.handle_execute_target_result(m),
            QueueMsg::RequestFileFinished(d) => self.handle_request_file_finished(d).await,
            QueueMsg::RequestFileFailed((d, err)) => self.handle_request_file_failed(d, err),
        }
        Ok(())
    }

    fn handle_incoming_client_connection(&mut self, connection: Connection) {
        let id = self.next_client_id;
        self.next_client_id += 1;
        info!(id, "new client connection");
        self.clients.insert(
            id,
            ClientConnection {
                connection: connection.clone(),
            },
        );
        let tx = self.tx.clone();
        tokio::spawn(async move { handle_client_connection(id, connection, tx).await });
    }

    fn handle_client_connection_lost(&mut self, id: ClientId) {
        let client = self.clients.get(&id).unwrap();
        let reason = client.connection.close_reason().unwrap();
        info!(id, ?reason, "lost client connection");
        self.clients.remove(&id);
    }

    fn handle_incoming_server_connection(
        &mut self,
        connection: quinn::Connection,
        host: String,
        port: u16,
    ) {
        let id = if let Some(remote_node) = self
            .remote_nodes
            .iter_mut()
            .find(|r| r.is_same(&host, port))
        {
            if remote_node
                .connection
                .as_ref()
                .is_some_and(|c| c.close_reason().is_none())
            {
                close_connection(connection, ConnectionCloseCode::KeepPreviousConnection);
                return;
            }
            remote_node.connection = Some(connection.clone());
            remote_node.id
        } else {
            let id = self.remote_nodes.len();
            self.remote_nodes.push(RemoteNode {
                id,
                connection: Some(connection.clone()),
                node: Node {
                    host,
                    server_port: port,
                    ..Default::default()
                },
            });
            id
        };
        info!(node=%self.remote_nodes[id], addr=?connection.remote_address(), "new incoming server connection");
        self.send_nodes_to_remote_server(id);
        let tx = self.tx.clone();
        tokio::spawn(async move { handle_server_connection(id, connection, tx).await });
    }

    /// Send [Node] for own instance and provide addresses for known remote servers
    fn handle_outgoing_server_connection(
        &mut self,
        id: RemoteNodeId,
        connection: quinn::Connection,
    ) {
        if self.remote_nodes[id]
            .connection
            .as_ref()
            .is_some_and(|c| c.close_reason().is_none())
        {
            close_connection(connection, ConnectionCloseCode::KeepPreviousConnection);
            return;
        }
        info!(node=%self.remote_nodes[id], addr=?connection.remote_address(), "new outgoing server connection");
        self.send_nodes_to_remote_server(id);
        self.remote_nodes[id].connection = Some(connection.clone());
        let tx = self.tx.clone();
        tokio::spawn(async move { handle_server_connection(id, connection, tx).await });
    }

    fn send_nodes_to_remote_server(&self, id: RemoteNodeId) {
        let Some(connection) = self.remote_nodes[id].connection.clone() else {
            return;
        };
        ServerMessage::Nodes(ServerMessageNodes {
            node: self.node.clone(),
            others: self
                .remote_nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != id)
                .map(|(_, n)| (n.node.host.clone(), n.node.server_port))
                .collect(),
        })
        .spawn_send_uni(connection)
        .unwrap();
    }

    fn handle_server_connection_lost(&mut self, id: RemoteNodeId) {
        let node = &mut self.remote_nodes[id];
        if let Some(close_reason) = node.connection.as_ref().and_then(|c| c.close_reason()) {
            info!(%node, ?close_reason, "lost server connection");
            match close_reason {
                ConnectionError::ApplicationClosed(x)
                    if x.error_code == ConnectionCloseCode::KeepPreviousConnection.into() =>
                {
                    tracing::error!("connection should have been closed earlier");
                }
                _ => {}
            }
            node.connection = None;
            self.connect_to_server(&self.remote_nodes[id]);
        }
    }

    // TODO drop async
    async fn handle_client_msg(
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
                self.handle_execute_targets_request(r).await?
            }
        }
        Ok(())
    }

    fn handle_server_msg_uni(&mut self, id: RemoteNodeId, msg: ServerMessage) -> Result<()> {
        match msg {
            ServerMessage::ConnectRequest(_) => unreachable!(),
            ServerMessage::ConnectAck => unreachable!(),
            ServerMessage::Nodes(nodes) => self.handle_server_msg_nodes(id, nodes),
            ServerMessage::ExecuteTargetsRequest(_) => todo!(),
            ServerMessage::ExecuteTargetResult(_) => todo!(),
        }
        Ok(())
    }

    fn handle_server_msg_bi(
        &mut self,
        _id: RemoteNodeId,
        msg: ServerMessage,
        _send: quinn::SendStream,
    ) -> Result<()> {
        match msg {
            ServerMessage::ConnectRequest(_) => unreachable!(),
            ServerMessage::ConnectAck => unreachable!(),
            ServerMessage::Nodes(_) => unreachable!(),
            ServerMessage::ExecuteTargetsRequest(_) => todo!(),
            ServerMessage::ExecuteTargetResult(_) => todo!(),
        }
    }

    fn handle_server_msg_nodes(&mut self, id: RemoteNodeId, nodes: ServerMessageNodes) {
        let remote_node = &mut self.remote_nodes[id];
        assert!(remote_node.is_same_node(&nodes.node));
        remote_node.node = nodes.node;
        for (host, server_port) in nodes.others {
            if !self
                .remote_nodes
                .iter()
                .any(|r| r.is_same(&host, server_port))
            {
                self.remote_nodes.push(RemoteNode {
                    id: self.remote_nodes.len(),
                    connection: None,
                    node: Node {
                        host,
                        server_port,
                        ..Default::default()
                    },
                });
                self.connect_to_server(self.remote_nodes.last().unwrap());
            }
        }
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
