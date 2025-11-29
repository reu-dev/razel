use crate::remote_exec::{ClientMessage, MessageVersion};
use anyhow::{ensure, Context, Result};
use quinn::{Connection, RecvStream, SendStream};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type LengthPrefix = u32;
const MAX_BUFFER_LEN: usize = 10 * 1024 * 1024;

impl ClientMessage {
    pub fn spawn_send(&self, stream: quinn::SendStream) -> Result<()> {
        rpc_spawn_send(stream, MessageVersion::ClientServerV1, self)
    }

    pub async fn send(&self, stream: &mut quinn::SendStream) -> Result<()> {
        rpc_send_impl(stream, MessageVersion::ClientServerV1, self).await
    }

    pub async fn recv(stream: &mut quinn::RecvStream) -> Result<Self> {
        rpc_recv_impl(stream, MessageVersion::ClientServerV1).await
    }
}

pub fn rpc_spawn_send<T: Serialize>(
    mut stream: SendStream,
    version: MessageVersion,
    msg: &T,
) -> Result<()> {
    let data = postcard::to_stdvec(msg)?;
    ensure!(
        data.len() <= MAX_BUFFER_LEN,
        "rpc_spawn_send(): buffer too large: {}MB",
        data.len() / 1024 / 1024
    );
    tokio::spawn(async move {
        let len = data.len() as LengthPrefix;
        let len_bytes = len.to_le_bytes();
        stream.write_u8(version as u8).await.ok();
        stream.write_all(&len_bytes).await.ok();
        stream.write_all(&data).await.ok();
        stream.finish().ok();
    });
    Ok(())
}

pub async fn rpc_send_impl<T: Serialize>(
    stream: &mut SendStream,
    version: MessageVersion,
    msg: &T,
) -> Result<()> {
    let data = postcard::to_stdvec(msg)?;
    ensure!(
        data.len() <= MAX_BUFFER_LEN,
        "rpc_send_impl(): buffer too large: {}MB",
        data.len() / 1024 / 1024
    );
    let len = data.len() as LengthPrefix;
    let len_bytes = len.to_le_bytes();
    stream.write_u8(version as u8).await?;
    stream.write_all(&len_bytes).await?;
    stream.write_all(&data).await?;
    Ok(())
}

pub async fn rpc_recv_impl<T: DeserializeOwned>(
    stream: &mut RecvStream,
    exp_version: MessageVersion,
) -> Result<T> {
    let act_version = MessageVersion::from(stream.read_u8().await?);
    ensure!(
        act_version == exp_version,
        "received message with unexpected version: {act_version:?}"
    );
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = LengthPrefix::from_le_bytes(len_buf) as usize;
    // Safety check: Prevent allocating too much RAM if a malicious packet sends a huge length
    ensure!(
        len <= MAX_BUFFER_LEN,
        "rpc_recv_impl(): buffer too large: {}MB",
        len / 1024 / 1024
    );
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .context("rpc_recv_message() read_exact")?;
    assert_eq!(buf.len(), len);
    let msg = postcard::from_bytes(&buf).context("rpc_recv_message() from_bytes")?;
    Ok(msg)
}

pub async fn rpc_request(
    connection: &Connection,
    request: &ClientMessage,
) -> Result<ClientMessage> {
    let (mut send, mut recv) = connection.open_bi().await?;
    request.send(&mut send).await?;
    send.finish()?;
    ClientMessage::recv(&mut recv).await
}
