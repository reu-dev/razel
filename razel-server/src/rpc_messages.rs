use crate::Node;
use anyhow::Result;
use razel::remote_exec::{
    ExecuteTargetResult, ExecuteTargetsRequest, JobId, MessageVersion, rpc_recv_impl,
    rpc_recv_uni_impl, rpc_send_impl, rpc_spawn_send_uni,
};
use serde::{Deserialize, Serialize};

/// Messages exchanged between servers
#[derive(Serialize, Deserialize)]
pub enum ServerMessage {
    ConnectRequest((String, u16)),
    ConnectAck,
    Nodes(ServerMessageNodes),
    ExecuteTargetsRequest(ExecuteTargetsRequest),
    ExecuteTargetResult((JobId, ExecuteTargetResult)),
}

impl ServerMessage {
    pub async fn send(&self, stream: &mut quinn::SendStream) -> Result<()> {
        rpc_send_impl(stream, MessageVersion::ServerServerV1, self).await
    }

    pub fn spawn_send_uni(&self, connection: quinn::Connection) -> Result<()> {
        rpc_spawn_send_uni(connection, MessageVersion::ServerServerV1, self)
    }

    pub async fn recv(stream: &mut quinn::RecvStream) -> Result<Self> {
        rpc_recv_impl(stream, MessageVersion::ServerServerV1).await
    }

    pub async fn recv_uni(connection: &quinn::Connection) -> Result<Self> {
        rpc_recv_uni_impl(connection, MessageVersion::ServerServerV1).await
    }

    pub async fn request(&self, connection: &quinn::Connection) -> Result<ServerMessage> {
        let (mut send, mut recv) = connection.open_bi().await?;
        self.send(&mut send).await?;
        send.finish()?;
        ServerMessage::recv(&mut recv).await
    }
}

#[derive(Serialize, Deserialize)]
pub struct ServerMessageNodes {
    pub node: Node,
    pub others: Vec<(String, u16)>,
}
