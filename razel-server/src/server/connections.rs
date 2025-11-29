use super::*;
use crate::rpc_messages::ServerMessage;
use anyhow::Result;
use quinn::{Connection, Endpoint};
use razel::remote_exec::ClientMessage;
use tracing::{error, info, instrument, warn};

impl Server {
    pub fn connect_to_servers(&self) {
        for node in self.nodes.iter().cloned() {
            let endpoint = self.server_endpoint.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                connect_to_server_loop(endpoint, node, tx).await;
            });
        }
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
                        if tx
                            .send(QueueMsg::IncomingServerConnection(connection))
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
            todo!("send QueueMsg::ServerConnectionLost");
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
            todo!("send QueueMsg::ClientConnectionLost");
        });
    }
}

async fn connect_to_server_loop(endpoint: Endpoint, node: RemoteNode, tx: Tx) {
    loop {
        match connect_to_server_loop_imp(&endpoint, &node).await {
            Ok(c) => {
                tx.send(QueueMsg::OutgoingConnection((node.id, c))).ok();
            }
            Err(e) => {
                warn!("{e}");
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn connect_to_server_loop_imp(endpoint: &Endpoint, node: &RemoteNode) -> Result<Connection> {
    let addr = node.socket_addr()?;
    let connection = endpoint.connect(addr, &node.host)?.await?;
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
                    match ClientMessage::recv(&mut recv).await {
                        Ok(m) => {
                            tx.send(QueueMsg::ClientMsg((id, m, send))).ok();
                        }
                        Err(e) => error!("handle_client_connection(): {e}"),
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("handle_client_connection: Connection closed by peer");
                break;
            }
            Err(e) => {
                error!("handle_client_connection: Error accepting stream: {e}");
                break;
            }
        }
    }
    tx.send(QueueMsg::ClientConnectionLost(id)).ok();
}
