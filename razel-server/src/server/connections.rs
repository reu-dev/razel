use super::*;
use crate::rpc_messages::ServerMessage;
use anyhow::Result;
use quinn::{Connection, Endpoint};
use razel::remote_exec::ClientToServerMsg;
use tracing::{error, info, instrument, warn};

impl Server {
    pub fn connect_to_servers(&self) {
        for remote_node in &self.remote_nodes {
            self.connect_to_server(remote_node);
        }
    }

    pub fn connect_to_server(&self, remote_node: &RemoteNode) {
        info!(node = %remote_node, "connecting to server");
        let endpoint = self.server_endpoint.clone();
        let id = remote_node.id;
        let local_addr = (self.node.host.clone(), self.node.server_port);
        let remote_addr = (remote_node.node.host.clone(), remote_node.node.server_port);
        let tx = self.tx.clone();
        tokio::spawn(async move {
            connect_to_server_loop(endpoint, id, local_addr, remote_addr, tx).await;
        });
    }

    #[instrument(skip(self))]
    pub fn accept_incoming_server_connections(&self) {
        let endpoint = self.server_endpoint.clone();
        info!(addr=?endpoint.local_addr().as_ref().unwrap());
        let tx = self.tx.clone();
        tokio::spawn(async move {
            while let Some(conn) = endpoint.accept().await {
                if !conn.remote_address_validated() {
                    conn.retry().unwrap();
                    continue;
                }
                match conn.await {
                    Ok(connection) => {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            Self::accept_incoming_server_connection(connection, tx).await
                        });
                    }
                    Err(e) => {
                        warn!("{e}");
                    }
                }
            }
        });
    }

    #[instrument(skip_all)]
    async fn accept_incoming_server_connection(connection: Connection, tx: Tx) {
        match connection.accept_bi().await {
            Ok((mut send, mut recv)) => match ServerMessage::recv(&mut recv).await {
                Ok(ServerMessage::ConnectRequest((host, port))) => {
                    if let Err(e) = ServerMessage::ConnectAck.send(&mut send).await {
                        warn!("error in sending ServerMessage::ConnectAck: {e:?}");
                        return;
                    }
                    tx.send(QueueMsg::IncomingServerConnection((connection, host, port)))
                        .ok();
                }
                Ok(_) => {
                    error!("received unexpected ServerMessage");
                }
                Err(e) => {
                    warn!("error in parsing ServerMessage: {e:?}");
                }
            },
            Err(e) => {
                warn!("error in accept_bi: {e:?}")
            }
        }
    }

    #[instrument(skip(self))]
    pub fn accept_incoming_client_connections(&self) {
        let Some(endpoint) = self.client_endpoint.clone() else {
            return;
        };
        info!(addr=?endpoint.local_addr().as_ref().unwrap());
        let tx = self.tx.clone();
        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                if !incoming.remote_address_validated() {
                    incoming.retry().unwrap();
                    continue;
                }
                match incoming.await {
                    Ok(connection) => {
                        tx.send(QueueMsg::IncomingClientConnection(connection)).ok();
                    }
                    Err(e) => {
                        warn!("{e}");
                    }
                }
            }
        });
    }
}

async fn connect_to_server_loop(
    endpoint: Endpoint,
    id: RemoteNodeId,
    local_addr: (String, u16),
    remote_addr: (String, u16),
    tx: Tx,
) {
    loop {
        match connect_to_server_loop_imp(&endpoint, &local_addr, &remote_addr).await {
            Ok(c) => {
                tx.send(QueueMsg::OutgoingServerConnection((id, c))).ok();
                break;
            }
            Err(e) => {
                warn!("{e}");
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

async fn connect_to_server_loop_imp(
    endpoint: &Endpoint,
    local_addr: &(String, u16),
    remote_addr: &(String, u16),
) -> Result<Connection> {
    let socket_addr = std::net::ToSocketAddrs::to_socket_addrs(remote_addr)?
        .next()
        .ok_or_else(|| anyhow!("couldn't resolve address"))?;
    let connection = endpoint.connect(socket_addr, &remote_addr.0)?.await?;
    let msg = ServerMessage::ConnectRequest((local_addr.0.clone(), local_addr.1));
    msg.request(&connection).await?;
    Ok(connection)
}

pub async fn handle_server_connection(id: RemoteNodeId, connection: Connection, tx: Tx) {
    #[allow(clippy::while_let_loop)]
    loop {
        match tokio::select! {
            r = connection.accept_bi() => r.map(|(send, recv)| (Some(send), recv)),
            r = connection.accept_uni() => r.map(|recv| (None, recv)),
        } {
            Ok((send, mut recv)) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    match ServerMessage::recv(&mut recv).await {
                        Ok(m) => {
                            if let Some(send) = send {
                                tx.send(QueueMsg::ServerMsgBi((id, m, send))).ok();
                            } else {
                                tx.send(QueueMsg::ServerMsgUni((id, m))).ok();
                            }
                        }
                        Err(_) => {
                            tx.send(QueueMsg::ServerConnectionLost(id)).ok();
                        }
                    }
                });
            }
            Err(_) => {
                break;
            }
        }
    }
    tx.send(QueueMsg::ServerConnectionLost(id)).ok();
}

pub async fn handle_client_connection(id: ClientId, connection: Connection, tx: Tx) {
    while let Ok((send, mut recv)) = connection.accept_bi().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            match ClientToServerMsg::recv(&mut recv).await {
                Ok(m) => {
                    tx.send(QueueMsg::ClientMsg((id, m, send))).ok();
                }
                Err(_) => {
                    tx.send(QueueMsg::ClientConnectionLost(id)).ok();
                }
            }
        });
    }
    tx.send(QueueMsg::ClientConnectionLost(id)).ok();
}
