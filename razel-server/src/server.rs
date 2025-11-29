use crate::config::Config;
use crate::rpc_endpoint::new_server_endpoint;
use crate::rpc_messages::ServerMessage;
use crate::Node;
use anyhow::{anyhow, bail, Result};
use quinn::{Connection, Endpoint};
use razel::remote_exec::rpc_endpoint::new_client_endpoint;
use razel::remote_exec::{ClientMessage, CreateJobResponse};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;
use tracing::{info, instrument};
use uuid::Uuid;

type Tx = mpsc::UnboundedSender<QueueMsg>;
type Rx = mpsc::UnboundedReceiver<QueueMsg>;
type RemoteNodeId = usize;
type ClientId = usize;

pub enum QueueMsg {
    IncomingClientConnection(quinn::Connection),
    ClientConnectionLost(ClientId),
    IncomingServerConnection(quinn::Connection),
    ServerConnectionLost(RemoteNodeId),
    OutgoingConnection((RemoteNodeId, quinn::Connection)),
    ClientMsg((ClientId, ClientMessage, quinn::SendStream)),
    ServerMsg((RemoteNodeId, ServerMessage, quinn::SendStream)),
}

pub struct Server {
    #[allow(dead_code)]
    node: Node,
    client_endpoint: Option<Endpoint>,
    server_endpoint: Endpoint,
    /// other servers to connect to
    nodes: Vec<RemoteNode>,
    clients: HashMap<ClientId, ClientConnection>,
    next_client_id: ClientId,
    tx: Tx,
    rx: Rx,
}

impl Server {
    pub fn new(mut config: Config, name: String) -> Result<Self> {
        let Some(self_config) = config.node.remove(&name) else {
            bail!("config missing for node name: {name}");
        };
        let client_endpoint =
            if let Some(x) = self_config.scheduler.as_ref().map(|x| &x.client_endpoint) {
                Some(new_server_endpoint(x)?)
            } else {
                None
            };
        let server_endpoint = if let Some(x) = &self_config.server_endpoint {
            new_server_endpoint(x)?
        } else {
            new_client_endpoint()?
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
                .worker
                .and_then(|w| w.max_parallelism)
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
        let (tx, rx) = mpsc::unbounded_channel();
        Ok(Self {
            node,
            client_endpoint,
            server_endpoint,
            nodes,
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

    #[instrument(skip_all)]
    async fn handle_queue_msg(&mut self, queue_msg: QueueMsg) -> Result<()> {
        match queue_msg {
            QueueMsg::IncomingClientConnection(c) => {
                let id = self.next_client_id;
                self.next_client_id += 1;
                self.clients.insert(
                    id,
                    ClientConnection {
                        id,
                        connection: c.clone(),
                    },
                );
                let tx = self.tx.clone();
                tokio::spawn(async move { handle_client_connection(id, c, tx).await });
            }
            QueueMsg::ClientConnectionLost(_) => todo!(),
            QueueMsg::IncomingServerConnection(_) => todo!(),
            QueueMsg::ServerConnectionLost(_) => todo!(),
            QueueMsg::OutgoingConnection((i, c)) => {
                self.nodes[i].connection = Some(c.clone());
                let tx = self.tx.clone();
                tokio::spawn(async move { handle_server_connection(i, c, tx).await });
            }
            QueueMsg::ClientMsg((id, msg, send)) => self.handle_client_msg(id, msg, send).await?,
            QueueMsg::ServerMsg(_) => todo!(),
        }
        Ok(())
    }

    #[instrument(skip(self, msg, send))]
    async fn handle_client_msg(
        &mut self,
        client_id: ClientId,
        msg: ClientMessage,
        send: quinn::SendStream,
    ) -> Result<()> {
        match msg {
            ClientMessage::CreateJobRequest(_r) => {
                let job_id = Uuid::now_v7();
                info!(?job_id, "CreateJobRequest");
                ClientMessage::CreateJobResponse(CreateJobResponse {
                    id: job_id,
                    url: self.webui_job_url(&job_id),
                })
                .spawn_send(send)?;
            }
            ClientMessage::CreateJobResponse(_) => todo!(),
            ClientMessage::ExecuteTargetsRequest(_) => todo!(),
            ClientMessage::ExecuteTargetResult(_) => todo!(),
            ClientMessage::UploadFilesRequest(_) => todo!(),
        }
        Ok(())
    }

    fn webui_job_url(&self, job_id: &Uuid) -> String {
        let host = &self.node.host;
        let port = self.node.client_port.unwrap();
        let job_id = job_id.as_simple();
        format!("http://{host}:{port}/job/{job_id}")
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
    #[allow(dead_code)]
    id: ClientId,
    #[allow(dead_code)]
    connection: quinn::Connection,
}

mod connections;
use connections::*;
