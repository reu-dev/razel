use anyhow::{bail, Context, Result};
use quinn::{Connection, RecvStream, SendStream};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;
use tracing::instrument;

type LengthPrefix = u32;
const MAX_BUFFER_LEN: usize = 10 * 1024 * 1024;

/// Send a message with postcard encoding and length prefix
///
/// TODO optimize using a scratch buffer?
#[instrument(skip_all)]
pub async fn rpc_send_message<T: Serialize>(stream: &mut SendStream, msg: &T) -> Result<()> {
    let data = postcard::to_stdvec(msg)?;
    if data.len() > MAX_BUFFER_LEN {
        bail!(
            "rpc_send_message(): buffer too large: {}MB",
            data.len() / 1024 / 1024
        );
    }
    let len = data.len() as LengthPrefix;
    let len_bytes = len.to_le_bytes();
    info!(len);
    stream.write_all(&len_bytes).await?;
    stream.write_all(&data).await?;
    Ok(())
}

/// Receive a message with postcard encoding and length prefix
#[instrument(skip_all)]
pub async fn rpc_recv_message<T: DeserializeOwned>(stream: &mut RecvStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = LengthPrefix::from_le_bytes(len_buf) as usize;
    // Safety check: Prevent allocating too much RAM if a malicious packet sends a huge length
    if len > MAX_BUFFER_LEN {
        bail!(
            "rpc_recv_message(): buffer too large: {}MB",
            len / 1024 / 1024
        );
    }
    info!(len);
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .context("rpc_recv_message() read_exact")?;
    assert_eq!(buf.len(), len);
    let msg = postcard::from_bytes(&buf).context("rpc_recv_message() from_bytes")?;
    Ok(msg)
}

pub async fn rpc_request<Request: Serialize, Response: DeserializeOwned>(
    connection: &Connection,
    request: &Request,
) -> Result<Response> {
    let (mut send, mut recv) = connection.open_bi().await?;
    rpc_send_message(&mut send, request).await?;
    send.finish()?;
    rpc_recv_message(&mut recv).await
}
