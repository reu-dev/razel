use crate::bazel_remote_exec::action_cache_client::ActionCacheClient;
use crate::bazel_remote_exec::capabilities_client::CapabilitiesClient;
use crate::bazel_remote_exec::content_addressable_storage_client::ContentAddressableStorageClient;
use crate::bazel_remote_exec::{
    batch_update_blobs_request, digest_function, ActionResult, BatchReadBlobsRequest,
    BatchUpdateBlobsRequest, GetActionResultRequest, GetCapabilitiesRequest, ServerCapabilities,
    UpdateActionResultRequest,
};
use crate::cache::{BlobDigest, MessageDigest};
use anyhow::{anyhow, bail, Context};
use log::warn;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::transport::{Channel, Uri};
use tonic::Code;

// TODO add Zstd compression for blobs
#[derive(Clone)]
pub struct GrpcRemoteCache {
    instance_name: String,
    download_dir: PathBuf,
    ac_client: ActionCacheClient<Channel>,
    cas_client: ContentAddressableStorageClient<Channel>,
    max_batch_total_size_bytes: usize,
    ac_upload_tx: UnboundedSender<(MessageDigest, ActionResult)>,
    cas_upload_tx: UnboundedSender<(BlobDigest, PathBuf)>,
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
        let (ac_upload_tx, ac_upload_rx) = mpsc::unbounded_channel();
        let (cas_upload_tx, cas_upload_rx) = mpsc::unbounded_channel();
        Self::spawn_ac_upload(instance_name.clone(), ac_client.clone(), ac_upload_rx);
        Self::spawn_cas_upload(instance_name.clone(), cas_client.clone(), cas_upload_rx);
        let mut client = Self {
            instance_name,
            download_dir,
            ac_client,
            cas_client,
            max_batch_total_size_bytes: 4 * 1024 * 1024, // see https://github.com/grpc/grpc-java/issues/1676#issuecomment-229809402
            ac_upload_tx,
            cas_upload_tx,
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
        if cache_capabilities.max_batch_total_size_bytes != 0 {
            self.max_batch_total_size_bytes =
                cache_capabilities.max_batch_total_size_bytes as usize;
        }
        Ok(())
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
                            warn!("Remote cache error in update_action_result(): {:?}", x);
                            break;
                        }
                    }
                }
            }
        });
    }

    /// TODO Use FindMissingBlobsRequest before uploading big files
    /// TODO upload multiple files at once, until max_batch_total_size_bytes
    fn spawn_cas_upload(
        instance_name: String,
        mut client: ContentAddressableStorageClient<Channel>,
        mut rx: UnboundedReceiver<(BlobDigest, PathBuf)>,
    ) {
        tokio::spawn(async move {
            while let Some((digest, path)) = rx.recv().await {
                let data = tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("Read file from local cache: {:?}", path))
                    .unwrap();
                match client
                    .batch_update_blobs(tonic::Request::new(BatchUpdateBlobsRequest {
                        instance_name: instance_name.clone(),
                        requests: vec![batch_update_blobs_request::Request {
                            digest: Some(digest),
                            data,
                            compressor: 0,
                        }],
                    }))
                    .await
                {
                    Ok(_) => {}
                    Err(x) => {
                        if x.code() != Code::Ok {
                            warn!("Remote cache error in batch_update_blobs(): {:?}", x);
                            break;
                        }
                    }
                }
            }
        });
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
                    warn!("Remote cache error in get_action_result(): {:?}", x);
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
                warn!("Remote cache error in batch_read_blobs(): {:?}", x);
                None
            }
        }
    }

    /// TODO replace asserts with proper error handling
    /// TODO limit to max_batch_total_size_bytes?
    pub async fn download_and_store_blobs(
        &self,
        digests: &Vec<BlobDigest>,
    ) -> anyhow::Result<Vec<(BlobDigest, PathBuf)>> {
        assert!(!digests.is_empty());
        let mut downloaded = Vec::with_capacity(digests.len());
        match self
            .cas_client
            .clone()
            .batch_read_blobs(tonic::Request::new(BatchReadBlobsRequest {
                instance_name: self.instance_name.clone(),
                digests: digests.clone(),
                ..Default::default()
            }))
            .await
        {
            Ok(blobs_response) => {
                let responses = blobs_response.into_inner().responses;
                assert_eq!(responses.len(), digests.len());
                for (i, response) in responses.into_iter().enumerate() {
                    if let (Some(digest), Some(status)) = (response.digest, response.status) {
                        assert_eq!(digest, digests[i]);
                        if status.code == Code::Ok as i32 {
                            // TODO validate that hash is a proper basename, does not contain . or /
                            let path = self.get_download_path(&digest);
                            assert_eq!(response.data.len() as i64, digest.size_bytes);
                            if let Err(e) = tokio::fs::write(&path, response.data).await {
                                warn!("Remote cache error in writing {path:?}: {e:?}");
                                continue;
                            }
                            downloaded.push((digest, path));
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
        Ok(downloaded)
    }

    fn get_download_path(&self, digest: &BlobDigest) -> PathBuf {
        static ID: AtomicUsize = AtomicUsize::new(0);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        self.download_dir.join(format!("{}_{id}", digest.hash))
    }

    /// Blob is read from local cache only at upload to avoid keeping too many big files in memory.
    pub fn push_blob(&self, digest: BlobDigest, path: PathBuf) {
        self.cas_upload_tx.send((digest, path)).ok();
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
    use tonic::Code;

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
        println!("{:?}", response);
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
        assert_eq!(response.unwrap_err().code(), tonic::Code::NotFound);
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
    async fn grpc_server_cas() {
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
}
