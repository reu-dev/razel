use crate::bazel_remote_exec::{ActionResult, OutputFile};
use crate::cache::{BlobDigest, GrpcRemoteCache, LocalCache, MessageDigest};
use anyhow::{bail, Context, Error};
use itertools::Itertools;
use log::info;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Uri;

#[derive(Clone)] // TODO is Cache::clone() a good idea?
pub struct Cache {
    out_dir: PathBuf,
    local_cache: LocalCache,
    remote_cache: Option<GrpcRemoteCache>,
    cas_states: Arc<Mutex<HashMap<String, CacheState>>>,
}

impl Cache {
    pub fn new(workspace_dir: &Path, out_dir: PathBuf) -> Result<Self, anyhow::Error> {
        let local_cache =
            LocalCache::new(workspace_dir).with_context(|| "Failed to create local cache")?;
        if out_dir.starts_with(&local_cache.dir) {
            bail!("out_dir should not be within cache dir: {:?}", out_dir);
        }
        Ok(Self {
            out_dir: out_dir.clone(),
            local_cache,
            remote_cache: None,
            cas_states: Arc::new(Mutex::new(Default::default())),
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
        &mut self,
        digest: &MessageDigest,
        use_remote_cache: bool,
    ) -> Option<ActionResult> {
        let remote_cache = self.remote_cache.as_ref().filter(|_| use_remote_cache);
        let action_result = if let Some(x) = self.local_cache.get_action_result(digest).await {
            x
        } else if let Some(remote_cache) = remote_cache {
            let x = remote_cache.get_action_result(digest.clone()).await?;
            self.local_cache.push_action_result(digest, &x).await.ok()?;
            x
        } else {
            return None;
        };
        if action_result.output_files.is_empty() {
            return Some(action_result);
        }
        let to_download = self.get_files_to_download(&action_result).await;
        if to_download.is_empty() {
            return Some(action_result);
        }
        let Some(remote_cache) = self.remote_cache.as_ref().filter(|_| use_remote_cache) else {
            return None;
        };
        let downloaded = remote_cache
            .download_and_store_blobs(&to_download)
            .await
            .ok()?;
        if downloaded.is_empty() {
            return None;
        }
        self.move_downloaded_files_to_cas(&downloaded).await.ok()?;
        (downloaded.len() == to_download.len()).then_some(action_result)
    }

    async fn move_downloaded_files_to_cas(
        &mut self,
        files: &Vec<(BlobDigest, PathBuf)>,
    ) -> Result<(), Error> {
        // store all downloaded files even if incomplete, might be used by other action
        for (_, path) in files {
            self.local_cache.prepare_file_to_move(path).await?;
        }
        let mut cas_states = self.cas_states.lock().await;
        for (digest, path) in files {
            let cas_state = cas_states
                .entry(digest.hash.clone())
                .or_insert(CacheState::New);
            if *cas_state != CacheState::New {
                continue;
            }
            self.local_cache.move_file_into_cache(path, digest).await?;
            *cas_state = CacheState::DownloadedFromRemoteCache;
        }
        Ok(())
    }

    async fn get_files_to_download<'a>(&mut self, result: &'a ActionResult) -> Vec<&'a OutputFile> {
        let mut missing = Vec::with_capacity(result.output_files.len());
        let mut cas_states = self.cas_states.lock().await;
        for file in &result.output_files {
            let Some(digest) = &file.digest else {
                // TODO handle when reading ActionResult
                panic!("OutputFile has no digest: {}", file.path);
            };
            if cas_states.contains_key(&digest.hash) {
                continue;
            }
            if self.local_cache.is_blob_cached(digest).await {
                cas_states.insert(digest.hash.clone(), CacheState::LocallyCached);
            } else {
                missing.push(file);
            }
        }
        missing
    }

    pub async fn push(
        &mut self,
        message_digest: &MessageDigest,
        action_result: &ActionResult,
        sandbox_dir: Option<&PathBuf>,
        use_remote_cache: bool,
    ) -> Result<(), anyhow::Error> {
        let files = self
            .prepare_files_to_push(action_result, sandbox_dir)
            .await?;
        let remote_cache = self.remote_cache.as_ref().filter(|_| use_remote_cache);
        self.local_cache
            .push_action_result(message_digest, action_result)
            .await?;
        if let Some(remote_cache) = remote_cache {
            remote_cache.push_action_result(message_digest.clone(), action_result.clone());
        }
        let mut cas_states = self.cas_states.lock().await;
        for file in files {
            let cas_state = cas_states
                .entry(file.digest.hash.clone())
                .or_insert(CacheState::New);
            Self::push_file(&self.local_cache, remote_cache, file, cas_state).await?;
        }
        Ok(())
    }

    /// To be called before Self::push_file() without mutex lock
    async fn prepare_files_to_push(
        &self,
        action_result: &ActionResult,
        sandbox_dir: Option<&PathBuf>,
    ) -> Result<Vec<PushFileData>, anyhow::Error> {
        let files = action_result
            .output_files
            .iter()
            .map(|file| PushFileData {
                digest: file.digest.as_ref().unwrap().clone(),
                out_path: sandbox_dir
                    .map(|x| x.join(&self.out_dir).join(&file.path))
                    .unwrap_or_else(|| self.out_dir.join(&file.path)),
                cas_path: self.local_cache.cas_path(file.digest.as_ref().unwrap()),
            })
            .collect_vec();
        for file in &files {
            if file.out_path.is_symlink() {
                bail!("output file must not be a symlink: {:?}", file.out_path);
            }
            self.local_cache
                .prepare_file_to_move(&file.out_path)
                .await?;
        }
        Ok(files)
    }

    async fn push_file(
        local_cache: &LocalCache,
        remote_cache: Option<&GrpcRemoteCache>,
        file: PushFileData,
        cas_state: &mut CacheState,
    ) -> Result<(), Error> {
        if *cas_state == CacheState::New {
            local_cache
                .move_file_into_cache(&file.out_path, &file.digest)
                .await
                .context("move_output_file_into_cache()")?;
            *cas_state = CacheState::LocallyCreatedButNotUploaded;
        }
        if cas_state.is_upload_needed() {
            if let Some(remote_cache) = remote_cache {
                remote_cache.push_blob(file.digest, file.cas_path);
                *cas_state = CacheState::LocallyCreatedAndUploaded;
            }
        }
        Ok(())
    }

    // TODO integrate in other functions?
    pub async fn symlink_output_files_into_out_dir(
        &self,
        output_files: &Vec<OutputFile>,
    ) -> Result<(), anyhow::Error> {
        self.local_cache
            .symlink_output_files_into_out_dir(output_files, &self.out_dir)
            .await
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CacheState {
    New,
    /// created or downloaded by another process
    LocallyCached,
    DownloadedFromRemoteCache,
    LocallyCreatedButNotUploaded,
    LocallyCreatedAndUploaded,
}

impl CacheState {
    fn is_upload_needed(&self) -> bool {
        match *self {
            CacheState::New => true,
            CacheState::LocallyCached => false,
            CacheState::DownloadedFromRemoteCache => false,
            CacheState::LocallyCreatedButNotUploaded => true,
            CacheState::LocallyCreatedAndUploaded => false,
        }
    }
}

struct PushFileData {
    digest: BlobDigest,
    out_path: PathBuf,
    cas_path: PathBuf,
}
