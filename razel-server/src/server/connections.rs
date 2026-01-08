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
        let endpoint = self.server_endpoint.clone();
        let id = remote_node.id;
        let host = remote_node.node.host.clone();
        let port = remote_node.node.server_port;
        let tx = self.tx.clone();
        tokio::spawn(async move {
            connect_to_server_loop(endpoint, id, host, port, tx).await;
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
                            match ServerMessage::recv_uni(&connection).await {
                                Ok(m) => {
                                    tx.send(QueueMsg::IncomingServerConnection((m, connection)))
                                        .ok();
                                }
                                Err(e) => warn!("{e}"),
                            };
                        });
                    }
                    Err(e) => {
                        warn!("{e}");
                        break;
                    }
                }
            }
        });
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
                        if tx
                            .send(QueueMsg::IncomingClientConnection(connection))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("{e}");
                        break;
                    }
                }
            }
        });
    }
}

async fn connect_to_server_loop(
    endpoint: Endpoint,
    id: RemoteNodeId,
    host: String,
    port: u16,
    tx: Tx,
) {
    loop {
        match connect_to_server_loop_imp(&endpoint, &host, port).await {
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
    host: &str,
    port: u16,
) -> Result<Connection> {
    let addr = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?
        .next()
        .ok_or_else(|| anyhow!("couldn't resolve address"))?;
    let connection = endpoint.connect(addr, host)?.await?;
    Ok(connection)
}

#[instrument(skip(connection, tx))]
pub async fn handle_server_connection(id: RemoteNodeId, connection: quinn::Connection, tx: Tx) {
    info!(addr=?connection.remote_address());
    loop {
        match connection.accept_bi().await {
            Ok((send, mut recv)) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    match ServerMessage::recv(&mut recv).await {
                        Ok(m) => {
                            tx.send(QueueMsg::ServerMsg((id, m, send))).ok();
                        }
                        Err(e) => error!("handle_server_connection(): {e}"),
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("handle_server_connection: Connection closed by peer");
                break;
            }
            Err(e) => {
                error!("handle_server_connection: Error accepting stream: {e}");
                break;
            }
        }
    }
    tx.send(QueueMsg::ServerConnectionLost(id)).ok();
}

#[instrument(skip(connection, tx))]
pub async fn handle_client_connection(id: ClientId, connection: quinn::Connection, tx: Tx) {
    info!(addr=?connection.remote_address());
    loop {
        match connection.accept_bi().await {
            Ok((send, mut recv)) => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    match ClientToServerMsg::recv(&mut recv).await {
                        Ok(m) => {
                            tx.send(QueueMsg::ClientMsg((id, m, send))).ok();
                        }
                        Err(e) => error!("{e:?}"),
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed(x)) => {
                info!("Connection closed by peer: {x:?}");
                break;
            }
            Err(e) => {
                error!("Error accepting stream: {e:?}");
                break;
            }
        }
    }
    tracing::warn!("ClientConnectionLost");
    tx.send(QueueMsg::ClientConnectionLost(id)).ok();
}
