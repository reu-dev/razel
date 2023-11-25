use crate::bazel_remote_exec::{ActionResult, OutputFile};
use crate::cache::{GrpcRemoteCache, LocalCache, MessageDigest};
use anyhow::{bail, Context};
use log::info;
use std::path::{Path, PathBuf};
use tonic::transport::Uri;

#[derive(Clone)] // TODO is Cache::clone() a good idea?
pub struct Cache {
    local_cache: LocalCache,
    remote_cache: Option<GrpcRemoteCache>,
}

impl Cache {
    pub fn new(workspace_dir: &Path) -> Result<Self, anyhow::Error> {
        Ok(Self {
            local_cache: LocalCache::new(workspace_dir)
                .with_context(|| "Failed to create local cache")?,
            remote_cache: None,
        })
    }

    pub fn dir(&self) -> &PathBuf {
        &self.local_cache.dir
    }

    pub async fn connect_remote_cache(&mut self, urls: &Vec<String>) -> Result<(), anyhow::Error> {
        for url in urls {
            let uri: Uri = url
                .parse()
                .with_context(|| format!("remote cache: {url}"))
                .context(
                    "remote cache should be an URI, e.g. grpc://localhost:9092[/instance_name]",
                )?;
            match uri.scheme_str() {
                Some("grpc") => match GrpcRemoteCache::new(uri, &self.local_cache.dir).await {
                    Ok(x) => {
                        self.remote_cache = Some(x);
                        info!("connected to remote cache: {url}");
                        break;
                    }
                    _ => {
                        info!("failed to connect to remote cache: {url}");
                    }
                },
                _ => bail!("only grpc remote caches are supported: {url}"),
            }
        }
        Ok(())
    }

    pub async fn get_action_result(
        &self,
        digest: &MessageDigest,
        use_remote_cache: bool,
    ) -> Option<ActionResult> {
        let action_result = if let Some(action_result) =
            self.local_cache.get_action_result(digest).await
        {
            action_result
        } else if let Some(remote_cache) = self.remote_cache.as_ref().filter(|_| use_remote_cache) {
            let x = remote_cache.get_action_result(digest.clone()).await?;
            self.local_cache.push_action_result(digest, &x).await.ok()?;
            x
        } else {
            return None;
        };
        let missing_files = self
            .local_cache
            .get_list_of_missing_output_files(&action_result)
            .await;
        match (
            missing_files.is_empty(),
            self.remote_cache.as_ref().filter(|_| use_remote_cache),
        ) {
            (true, _) => Some(action_result),
            (false, None) => None,
            (false, Some(remote_cache)) => {
                let downloaded = remote_cache
                    .download_and_store_blobs(&missing_files)
                    .await
                    .ok()?;
                // store all downloaded files even if incomplete, might be used by other action
                for (digest, path) in &downloaded {
                    self.local_cache
                        .move_file_into_cache(path, digest)
                        .await
                        .ok()?;
                }
                if downloaded.len() == missing_files.len() {
                    Some(action_result)
                } else {
                    None
                }
            }
        }
    }

    pub async fn push_action_result(
        &self,
        digest: &MessageDigest,
        result: &ActionResult,
        use_remote_cache: bool,
    ) -> Result<(), anyhow::Error> {
        if let Some(remote_cache) = self.remote_cache.as_ref().filter(|_| use_remote_cache) {
            remote_cache.push_action_result(digest.clone(), result.clone());
        }
        self.local_cache.push_action_result(digest, result).await
    }

    pub async fn move_output_file_into_cache(
        &self,
        sandbox_dir: Option<&PathBuf>,
        out_dir: &PathBuf,
        file: &OutputFile,
        use_remote_cache: bool,
    ) -> Result<(), anyhow::Error> {
        let cache_path = self
            .local_cache
            .move_output_file_into_cache(sandbox_dir, out_dir, file)
            .await?;
        if let Some(remote_cache) = self.remote_cache.as_ref().filter(|_| use_remote_cache) {
            remote_cache.push_blob(file.digest.clone().unwrap(), cache_path);
        }
        Ok(())
    }

    pub async fn symlink_output_files_into_out_dir(
        &self,
        output_files: &Vec<OutputFile>,
        out_dir: &Path,
    ) -> Result<(), anyhow::Error> {
        self.local_cache
            .symlink_output_files_into_out_dir(output_files, out_dir)
            .await
    }
}
