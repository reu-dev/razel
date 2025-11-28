use anyhow::Result;
use razel::remote_exec::{rpc_recv_impl, rpc_send_impl, MessageVersion};
use serde::{Deserialize, Serialize};

/// Messages exchanged between servers
#[derive(Serialize, Deserialize)]
pub enum ServerMessage {
    NodeRequest,
    NodeResponse,
}

impl ServerMessage {
    pub async fn send(&self, stream: &mut quinn::SendStream) -> Result<()> {
        rpc_send_impl(stream, MessageVersion::ServerServerV1, self).await
    }

    pub async fn recv(stream: &mut quinn::RecvStream) -> Result<Self> {
        rpc_recv_impl(stream, MessageVersion::ServerServerV1).await
    }
}
