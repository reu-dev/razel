use crate::bazel_remote_exec::action_cache_client::ActionCacheClient;
use crate::bazel_remote_exec::bytestream::byte_stream_client::ByteStreamClient;
use crate::bazel_remote_exec::bytestream::{ReadRequest, WriteRequest};
use crate::bazel_remote_exec::capabilities_client::CapabilitiesClient;
use crate::bazel_remote_exec::content_addressable_storage_client::ContentAddressableStorageClient;
use crate::bazel_remote_exec::{
    batch_update_blobs_request, digest_function, ActionResult, BatchReadBlobsRequest,
    BatchUpdateBlobsRequest, Digest, GetActionResultRequest, GetCapabilitiesRequest, OutputFile,
    ServerCapabilities, UpdateActionResultRequest,
};
use crate::cache::{BlobDigest, MessageDigest};
use crate::make_file_executable;
use anyhow::{anyhow, bail, Context};
use futures_util::stream;
use itertools::Itertools;
use log::warn;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::transport::{Channel, Uri};
use tonic::Code;
use uuid::Uuid;

const BYTESTREAM_CHUNK_SIZE: usize = 1024 * 1024;

// TODO add Zstd compression for blobs
#[derive(Clone)]
pub struct GrpcRemoteCache {
    instance_name: String,
    download_dir: PathBuf,
    ac_client: ActionCacheClient<Channel>,
    cas_client: ContentAddressableStorageClient<Channel>,
    bytestream_client: ByteStreamClient<Channel>,
    max_batch_blob_size: i64,
    ac_upload_tx: UnboundedSender<(MessageDigest, ActionResult)>,
    cas_upload_batch_tx: UnboundedSender<(BlobDigest, PathBuf)>,
    cas_upload_bytestream_tx: UnboundedSender<(BlobDigest, PathBuf)>,
}

impl GrpcRemoteCache {
    pub async fn new(uri: Uri, dir: &Path) -> anyhow::Result<Self> {
        let instance_name = uri
            .path()
            .strip_prefix('/')
            .unwrap_or(uri.path())
            .to_string();
        let uri_wo_instance_name = Uri::builder()
            .scheme("grpc")
            .authority(uri.authority().unwrap().clone())
            .path_and_query("")
            .build()
            .unwrap();
        let download_dir = dir.join("download").join(std::process::id().to_string());
        std::fs::create_dir_all(&download_dir)?;
        let channel = Channel::builder(uri_wo_instance_name).connect().await?;
        let ac_client = ActionCacheClient::new(channel.clone());
        let cas_client = ContentAddressableStorageClient::new(channel.clone());
        let bytestream_client = ByteStreamClient::new(channel.clone());
        let (ac_upload_tx, ac_upload_rx) = mpsc::unbounded_channel();
        let (cas_upload_batch_tx, cas_upload_batch_rx) = mpsc::unbounded_channel();
        let (cas_upload_bytestream_tx, cas_upload_bytestream_rx) = mpsc::unbounded_channel();
        Self::spawn_ac_upload(instance_name.clone(), ac_client.clone(), ac_upload_rx);
        Self::spawn_cas_upload_batch(
            instance_name.clone(),
            cas_client.clone(),
            cas_upload_batch_rx,
        );
        Self::spawn_cas_upload_bytestream(
            instance_name.clone(),
            bytestream_client.clone(),
            cas_upload_bytestream_rx,
        );
        let mut client = Self {
            instance_name,
            download_dir,
            ac_client,
            cas_client,
            bytestream_client,
            max_batch_blob_size: 0,
            ac_upload_tx,
            cas_upload_batch_tx,
            cas_upload_bytestream_tx,
        };
        client.check_capabilities(channel.clone()).await?;
        Ok(client)
    }

    async fn check_capabilities(&mut self, channel: Channel) -> anyhow::Result<()> {
        let mut client = CapabilitiesClient::new(channel);
        let capabilities: ServerCapabilities = client
            .get_capabilities(tonic::Request::new(GetCapabilitiesRequest {
                ..Default::default()
            }))
            .await?
            .into_inner();
        let cache_capabilities = capabilities
            .cache_capabilities
            .ok_or_else(|| anyhow!("ServerCapabilities::cache_capabilities missing"))?;
        if !cache_capabilities
            .digest_functions
            .contains(&digest_function::Value::Sha256.into())
        {
            bail!("Sha256 support missing");
        }
        if cache_capabilities
            .action_cache_update_capabilities
            .map(|x| x.update_enabled)
            != Some(true)
        {
            bail!("ActionCacheUpdateCapabilities::update_enabled not set");
        }
        let max_batch_total_size_bytes = if cache_capabilities.max_batch_total_size_bytes != 0 {
            cache_capabilities.max_batch_total_size_bytes as usize
        } else {
            4 * 1024 * 1024 // see https://github.com/grpc/grpc-java/issues/1676#issuecomment-229809402
        };
        self.max_batch_blob_size =
            Self::get_max_batch_blob_size(self.instance_name.clone(), max_batch_total_size_bytes)
                as i64;
        Ok(())
    }

    /// Returns max_batch_total_size_bytes minus overhead for BatchUpdateBlobsRequest
    fn get_max_batch_blob_size(instance_name: String, max_batch_total_size_bytes: usize) -> usize {
        use prost::Message;
        let mut data: Vec<u8> = Default::default();
        data.resize(max_batch_total_size_bytes, 0);
        let encoded_len = BatchUpdateBlobsRequest {
            instance_name,
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(Digest::for_bytes(&data)),
                data,
                compressor: 0,
            }],
            digest_function: digest_function::Value::Sha256.into(),
        }
        .encoded_len();
        assert!(encoded_len > max_batch_total_size_bytes);
        let overhead = encoded_len - max_batch_total_size_bytes;
        max_batch_total_size_bytes - overhead
    }

    fn spawn_ac_upload(
        instance_name: String,
        mut client: ActionCacheClient<Channel>,
        mut rx: UnboundedReceiver<(MessageDigest, ActionResult)>,
    ) {
        tokio::spawn(async move {
            while let Some((action_digest, action_result)) = rx.recv().await {
                match client
                    .update_action_result(tonic::Request::new(UpdateActionResultRequest {
                        instance_name: instance_name.clone(),
                        action_digest: Some(action_digest),
                        action_result: Some(action_result),
                        ..Default::default()
                    }))
                    .await
                {
                    Ok(_) => {}
                    Err(x) => {
                        if x.code() != Code::Ok {
                            warn!("Remote cache error in update_action_result(): {x:?}");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// TODO Use FindMissingBlobsRequest before uploading big files
    /// TODO upload multiple files at once, until max_batch_total_size_bytes
    fn spawn_cas_upload_batch(
        instance_name: String,
        mut client: ContentAddressableStorageClient<Channel>,
        mut rx: UnboundedReceiver<(BlobDigest, PathBuf)>,
    ) {
        tokio::spawn(async move {
            while let Some((digest, path)) = rx.recv().await {
                let data = tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("Read file from local cache: {path:?}"))
                    .unwrap();
                match client
                    .batch_update_blobs(tonic::Request::new(BatchUpdateBlobsRequest {
                        instance_name: instance_name.clone(),
                        requests: vec![batch_update_blobs_request::Request {
                            digest: Some(digest),
                            data,
                            compressor: 0,
                        }],
                        digest_function: digest_function::Value::Sha256.into(),
                    }))
                    .await
                {
                    Ok(_) => {}
                    Err(x) => {
                        if x.code() != Code::Ok {
                            warn!("Remote cache error in batch_update_blobs(): {x:?}");
                            break;
                        }
                    }
                }
            }
        });
    }

    fn spawn_cas_upload_bytestream(
        instance_name: String,
        mut client: ByteStreamClient<Channel>,
        mut rx: UnboundedReceiver<(BlobDigest, PathBuf)>,
    ) {
        tokio::spawn(async move {
            while let Some((digest, path)) = rx.recv().await {
                if let Err(e) =
                    Self::bytestream_upload(&mut client, &instance_name, &digest, &path).await
                {
                    warn!("Remote cache error in spawn_cas_upload_bytestream({path:?}): {e:?}");
                }
            }
        });
    }

    async fn bytestream_upload(
        client: &mut ByteStreamClient<Channel>,
        instance_name: &str,
        digest: &BlobDigest,
        path: &Path,
    ) -> anyhow::Result<()> {
        let resource_name = Self::bytestream_upload_resource_name(instance_name, digest);
        let file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open {path:?}"))?;
        let total = digest.size_bytes;
        let request_stream = stream::unfold(
            (file, resource_name, 0i64, total, true, false),
            |(mut file, resource_name, sent, total, first, done)| async move {
                if done {
                    return None;
                }
                let mut buf = vec![0u8; BYTESTREAM_CHUNK_SIZE];
                let mut filled = 0usize;
                while filled < buf.len() {
                    match file.read(&mut buf[filled..]).await {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(_) => return None,
                    }
                }
                buf.truncate(filled);
                let write_offset = sent;
                let new_sent = sent + filled as i64;
                if filled == 0 && new_sent < total {
                    return None;
                }
                let finish_write = new_sent >= total;
                let (rn, next_resource) = if first {
                    (resource_name, String::new())
                } else {
                    (String::new(), resource_name)
                };
                let req = WriteRequest {
                    resource_name: rn,
                    write_offset,
                    finish_write,
                    data: buf,
                };
                Some((
                    req,
                    (file, next_resource, new_sent, total, false, finish_write),
                ))
            },
        );
        let response = client.write(request_stream).await?.into_inner();
        if response.committed_size != total {
            warn!(
                "committed_size {} != digest size {total}",
                response.committed_size,
            );
        }
        Ok(())
    }

    fn bytestream_upload_resource_name(instance_name: &str, digest: &BlobDigest) -> String {
        let uuid = Uuid::now_v7();
        if instance_name.is_empty() {
            format!("uploads/{uuid}/blobs/{}/{}", digest.hash, digest.size_bytes)
        } else {
            format!(
                "{instance_name}/uploads/{uuid}/blobs/{}/{}",
                digest.hash, digest.size_bytes
            )
        }
    }

    fn bytestream_read_resource_name(instance_name: &str, digest: &BlobDigest) -> String {
        if instance_name.is_empty() {
            format!("blobs/{}/{}", digest.hash, digest.size_bytes)
        } else {
            format!(
                "{instance_name}/blobs/{}/{}",
                digest.hash, digest.size_bytes
            )
        }
    }

    pub async fn get_action_result(&self, digest: MessageDigest) -> Option<ActionResult> {
        match self
            .ac_client
            .clone()
            .get_action_result(tonic::Request::new(GetActionResultRequest {
                instance_name: self.instance_name.clone(),
                action_digest: Some(digest),
                inline_stdout: true,
                inline_stderr: true,
                ..Default::default()
            }))
            .await
        {
            Ok(x) => Some(x.into_inner()),
            Err(x) => {
                if x.code() != Code::NotFound {
                    warn!("Remote cache error in get_action_result(): {x:?}");
                }
                None
            }
        }
    }

    pub fn push_action_result(&self, digest: MessageDigest, result: ActionResult) {
        self.ac_upload_tx.send((digest, result)).ok();
    }

    pub async fn get_blob(&self, digest: BlobDigest) -> Option<Vec<u8>> {
        match self
            .cas_client
            .clone()
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: self.instance_name.clone(),
                digests: vec![digest],
                ..Default::default()
            }))
            .await
        {
            Ok(x) => Some(x.into_inner().responses.first().unwrap().data.clone()),
            Err(x) => {
                warn!("Remote cache error in batch_read_blobs(): {x:?}");
                None
            }
        }
    }

    pub async fn download_and_store_blobs(
        &self,
        files: &[&OutputFile],
    ) -> anyhow::Result<Vec<(BlobDigest, PathBuf)>> {
        assert!(!files.is_empty());
        let mut batch_size = 0;
        let mut batch_files = Vec::new();
        let mut stream_files = Vec::new();
        for file in files
            .iter()
            .copied()
            .sorted_by_key(|x| x.digest.as_ref().unwrap().size_bytes)
        {
            let size = file.digest.as_ref().unwrap().size_bytes;
            if batch_size + size <= self.max_batch_blob_size {
                batch_files.push(file);
                batch_size += size;
            } else {
                stream_files.push(file);
            }
        }
        let mut downloaded = Vec::with_capacity(files.len());
        if !batch_files.is_empty() {
            self.batch_download(batch_files, &mut downloaded).await;
        }
        for file in &stream_files {
            let digest = file.digest.as_ref().unwrap().clone();
            match self.bytestream_download(&digest, file.is_executable).await {
                Ok(path) => downloaded.push((digest, path)),
                Err(e) => warn!(
                    "Remote cache error in bytestream_download({:?}): {e:?}",
                    file.path
                ),
            }
        }
        Ok(downloaded)
    }

    async fn batch_download(
        &self,
        files: Vec<&OutputFile>,
        downloaded: &mut Vec<(Digest, PathBuf)>,
    ) {
        match self
            .cas_client
            .clone()
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: self.instance_name.clone(),
                digests: files
                    .iter()
                    .map(|x| x.digest.as_ref().unwrap().clone())
                    .collect(),
                ..Default::default()
            }))
            .await
        {
            Ok(blobs_response) => {
                let responses = blobs_response.into_inner().responses;
                assert_eq!(responses.len(), files.len());
                for (i, response) in responses.into_iter().enumerate() {
                    let file = files[i];
                    if let (Some(digest), Some(status)) = (response.digest, response.status) {
                        assert_eq!(&digest, file.digest.as_ref().unwrap());
                        if status.code == Code::Ok as i32 {
                            assert_eq!(response.data.len() as i64, digest.size_bytes);
                            // TODO validate that hash is a proper basename, does not contain . or /
                            let path = self.get_download_path(&digest);
                            match Self::batch_download_store_blob(
                                &path,
                                &response.data,
                                file.is_executable,
                            )
                            .await
                            {
                                Ok(_) => downloaded.push((digest, path)),
                                Err(e) => {
                                    warn!("Remote cache error in store_blob({path:?}): {e:?}")
                                }
                            }
                        } else if status.code != Code::NotFound as i32 {
                            warn!("Remote cache error in batch_read_blobs(): {status:?}");
                        }
                    } else {
                        warn!("Remote cache returned unexpected response in batch_read_blobs()");
                    }
                }
            }
            Err(x) => {
                warn!("Remote cache error in batch_read_blobs(): {x:?}");
            }
        }
    }

    async fn batch_download_store_blob(
        path: &PathBuf,
        contents: &Vec<u8>,
        is_executable: bool,
    ) -> anyhow::Result<()> {
        tokio::fs::write(&path, contents).await?;
        if is_executable {
            let file = tokio::fs::File::open(path).await?;
            make_file_executable(&file).await?;
        }
        Ok(())
    }

    async fn bytestream_download(
        &self,
        digest: &BlobDigest,
        is_executable: bool,
    ) -> anyhow::Result<PathBuf> {
        let resource_name = Self::bytestream_read_resource_name(&self.instance_name, digest);
        let path = self.get_download_path(digest);
        let mut client = self.bytestream_client.clone();
        let mut stream = client
            .read(tonic::Request::new(ReadRequest {
                resource_name,
                read_offset: 0,
                read_limit: 0,
            }))
            .await?
            .into_inner();
        let mut file = tokio::fs::File::create(&path)
            .await
            .with_context(|| format!("create {path:?}"))?;
        let mut total: i64 = 0;
        while let Some(resp) = stream.message().await? {
            file.write_all(&resp.data).await?;
            total += resp.data.len() as i64;
        }
        file.flush().await?;
        if is_executable {
            make_file_executable(&file).await?;
        }
        if total != digest.size_bytes {
            bail!("read {total}, expected {}", digest.size_bytes);
        }
        Ok(path)
    }

    fn get_download_path(&self, digest: &BlobDigest) -> PathBuf {
        static ID: AtomicUsize = AtomicUsize::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        self.download_dir.join(format!("{}_{id}", digest.hash))
    }

    /// Blob is read from local cache only at upload to avoid keeping too many big files in memory.
    pub fn push_blob(&self, digest: BlobDigest, path: PathBuf) {
        if digest.size_bytes > self.max_batch_blob_size {
            self.cas_upload_bytestream_tx.send((digest, path)).ok();
        } else {
            self.cas_upload_batch_tx.send((digest, path)).ok();
        }
    }
}

impl Drop for GrpcRemoteCache {
    fn drop(&mut self) {
        // TODO only delete after dropping last instance, currently it's cloned
        //std::fs::remove_dir_all(&self.download_dir).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bazel_remote_exec;
    use crate::bazel_remote_exec::{
        batch_update_blobs_request, ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest,
        Digest, GetActionResultRequest, GetCapabilitiesRequest, UpdateActionResultRequest,
    };
    use itertools::Itertools;

    const INSTANCE_NAME: &str = "main";
    const CACHE_URL: &str = "grpc://localhost:9092";

    #[tokio::test]
    async fn grpc_server_capabilities() {
        let mut client = CapabilitiesClient::connect(CACHE_URL).await.unwrap();
        let response = client
            .get_capabilities(tonic::Request::new(GetCapabilitiesRequest {
                instance_name: INSTANCE_NAME.to_string(),
            }))
            .await;
        println!("{response:?}");
        let capabilities = response.unwrap().into_inner();
        assert!(
            capabilities
                .cache_capabilities
                .unwrap()
                .action_cache_update_capabilities
                .unwrap()
                .update_enabled
        );
    }

    /// Test a AC cache server using a random/unique Action
    #[tokio::test]
    async fn grpc_server_ac() {
        let mut client = ActionCacheClient::connect(CACHE_URL).await.unwrap();
        let stdout = format!(
            "Hello pid {} at {:?}",
            std::process::id(),
            std::time::Instant::now()
        );
        let action_digest = Digest::for_message(&bazel_remote_exec::Action {
            command_digest: Some(Digest::for_message(&bazel_remote_exec::Command {
                arguments: vec!["echo".into(), stdout.clone()],
                ..Default::default()
            })),
            ..Default::default()
        });
        let action_result = ActionResult {
            stdout_raw: stdout.clone().into(),
            ..Default::default()
        };
        // download should fail because the Action is unique
        let response = client
            .get_action_result(tonic::Request::new(GetActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                inline_stdout: true,
                ..Default::default()
            }))
            .await;
        assert_eq!(response.unwrap_err().code(), Code::NotFound);
        // upload it
        let response = client
            .update_action_result(tonic::Request::new(UpdateActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                action_result: Some(action_result.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.exit_code, action_result.exit_code);
        assert_eq!(response.stdout_raw, action_result.stdout_raw);
        // now download should succeed
        let response = client
            .get_action_result(tonic::Request::new(GetActionResultRequest {
                instance_name: INSTANCE_NAME.to_string(),
                action_digest: Some(action_digest.clone()),
                inline_stdout: true,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.exit_code, action_result.exit_code);
        assert_eq!(response.stdout_raw, action_result.stdout_raw);
    }

    #[tokio::test]
    async fn grpc_server_cas_batch() {
        let mut client = ContentAddressableStorageClient::connect(CACHE_URL)
            .await
            .unwrap();
        let content = format!(
            "Hello pid {} at {:?}",
            std::process::id(),
            std::time::Instant::now()
        );
        let digest = Digest::for_string(&content);
        // download should fail because the content is unique
        let response = client
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest.clone()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(
            response_0.status.as_ref().unwrap().code,
            Code::NotFound as i32
        );
        assert_eq!(response_0.data, Vec::<u8>::new());
        assert_eq!(response_0.compressor, 0);
        // upload it
        let response = client
            .batch_update_blobs(tonic::Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(digest.clone()),
                    data: content.clone().into_bytes(),
                    compressor: 0,
                }],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(response_0.status.as_ref().unwrap().code, Code::Ok as i32);
        // now download should succeed
        let response = client
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                digests: vec![digest.clone()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses.len(), 1);
        let response_0 = &response.responses[0];
        assert_eq!(response_0.digest, Some(digest.clone()));
        assert_eq!(response_0.status.as_ref().unwrap().code, Code::Ok as i32);
        assert_eq!(response_0.data, content.into_bytes());
        assert_eq!(response_0.compressor, 0);
    }

    #[tokio::test]
    async fn grpc_server_cas_bytestream() {
        const SIZE: usize = 20 * 1024 * 1024;
        let channel = Channel::from_static(CACHE_URL).connect().await.unwrap();
        let mut bs = ByteStreamClient::new(channel);
        // build a blob that is unique per test run so the digest is not cached
        let seed = format!(
            "Hello pid {} at {:?}",
            std::process::id(),
            std::time::Instant::now()
        );
        let mut data: Vec<u8> = Vec::with_capacity(SIZE);
        data.extend_from_slice(seed.as_bytes());
        data.resize(SIZE, 0);
        for (i, b) in data.iter_mut().enumerate().skip(seed.len()) {
            *b = (i as u8).wrapping_mul(31);
        }
        let digest = Digest::for_bytes(&data);
        assert_eq!(digest.size_bytes as usize, SIZE);
        let read_resource = format!(
            "{}/blobs/{}/{}",
            INSTANCE_NAME, digest.hash, digest.size_bytes
        );
        // download via ByteStream should fail because the content is unique.
        // NotFound may be returned on the initial call or on the first stream message,
        // depending on the server implementation.
        let status = match bs
            .read(tonic::Request::new(ReadRequest {
                resource_name: read_resource.clone(),
                read_offset: 0,
                read_limit: 0,
            }))
            .await
        {
            Err(s) => s,
            Ok(resp) => {
                let mut stream_resp = resp.into_inner();
                loop {
                    match stream_resp.message().await {
                        Ok(Some(_)) => continue,
                        Ok(None) => panic!("expected NotFound, got empty stream"),
                        Err(e) => break e,
                    }
                }
            }
        };
        assert_eq!(status.code(), Code::NotFound, "{status:?}");
        // upload via ByteStream Write
        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            Uuid::now_v7(),
            digest.hash,
            digest.size_bytes
        );
        let total = SIZE;
        let chunks: Vec<WriteRequest> = {
            let mut v = Vec::new();
            let mut offset = 0usize;
            let mut first = true;
            while offset < total {
                let end = (offset + BYTESTREAM_CHUNK_SIZE).min(total);
                let rn = if first {
                    resource_name.clone()
                } else {
                    String::new()
                };
                first = false;
                v.push(WriteRequest {
                    resource_name: rn,
                    write_offset: offset as i64,
                    finish_write: end == total,
                    data: data[offset..end].to_vec(),
                });
                offset = end;
            }
            v
        };
        let write_response = bs.write(stream::iter(chunks)).await.unwrap().into_inner();
        assert_eq!(write_response.committed_size as usize, total);
        // download via ByteStream Read
        let mut stream_resp = bs
            .read(tonic::Request::new(ReadRequest {
                resource_name: read_resource,
                read_offset: 0,
                read_limit: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let mut got = Vec::with_capacity(SIZE);
        while let Some(resp) = stream_resp.message().await.unwrap() {
            got.extend_from_slice(&resp.data);
        }
        assert_eq!(got.len(), SIZE);
        assert_eq!(Digest::for_bytes(&got), digest);
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn grpc_server_ac_stresstest() {
        let client = ActionCacheClient::connect(CACHE_URL).await.unwrap();
        println!("connected to {CACHE_URL:?}");
        let tasks = (0..1000)
            .map(|_| {
                let mut client = client.clone();
                tokio::spawn(async move {
                    let stdout = format!(
                        "Hello pid {} at {:?}",
                        std::process::id(),
                        std::time::Instant::now()
                    );
                    let action_digest = Digest::for_message(&bazel_remote_exec::Action {
                        command_digest: Some(Digest::for_message(&bazel_remote_exec::Command {
                            arguments: vec!["echo".into(), stdout],
                            ..Default::default()
                        })),
                        ..Default::default()
                    });
                    // download should fail because the Action is unique
                    let response = client
                        .get_action_result(tonic::Request::new(GetActionResultRequest {
                            instance_name: INSTANCE_NAME.to_string(),
                            action_digest: Some(action_digest.clone()),
                            inline_stdout: true,
                            ..Default::default()
                        }))
                        .await;
                    let err = response.unwrap_err();
                    assert_eq!(err.code(), Code::NotFound, "{err:?}");
                })
            })
            .collect_vec();
        for task in tasks {
            task.await.unwrap();
        }
    }
}
