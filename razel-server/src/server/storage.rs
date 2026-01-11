use super::*;
use anyhow::{Result, ensure};
use razel::remote_exec::{JobId, ServerToClientMsg};
use razel::types::{DigestHash, File, FileId};
use razel::{force_remove_file_std, set_file_permissions};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::task::spawn_blocking;

pub struct Storage {
    pub path: PathBuf,
    cas_dir: PathBuf,
    download_dir: PathBuf,
    #[allow(dead_code)]
    max_size_gb: Option<usize>,
    locally_cached: HashSet<DigestHash>,
    requested_from_clients: HashMap<DigestHash, Vec<(JobId, FileId)>>,
}

impl Storage {
    pub fn new(path: PathBuf, max_size_gb: Option<usize>) -> Result<Self> {
        let cas_dir = path.join("cache/cas");
        let download_dir = path.join("download");
        std::fs::create_dir_all(&cas_dir)?;
        std::fs::create_dir_all(&download_dir)?;
        Ok(Self {
            path,
            cas_dir,
            download_dir,
            max_size_gb,
            locally_cached: Default::default(),
            requested_from_clients: Default::default(),
        })
    }

    /// Returns true if file is locally cached, or requests the file from distributed cache or client.
    pub fn request_file_from_client(
        &mut self,
        job: JobId,
        file: &File,
        connection: &quinn::Connection,
        tx: &Tx,
    ) -> bool {
        let digest = file.digest.as_ref().unwrap();
        if self.locally_cached.contains(&digest.hash) {
            return true;
        }
        let requests = self
            .requested_from_clients
            .entry(digest.hash.clone())
            .or_default();
        requests.push((job, file.id));
        if requests.len() > 1 {
            return false;
        }
        let file = file.clone();
        let connection = connection.clone();
        let download_dir = self.download_dir.clone();
        let cas_path = self.cas_path(&digest.hash);
        let tx = tx.clone();
        tokio::spawn(async move {
            // TODO check distributed cache first
            match download_file(connection, &file, download_dir, cas_path).await {
                Ok(()) => tx
                    .send(QueueMsg::RequestFileFinished(file.digest.unwrap()))
                    .ok(),
                Err(e) => {
                    tracing::warn!(path=?file.path, "download file from client: {e:?}");
                    tx.send(QueueMsg::RequestFileFailed((
                        file.digest.unwrap(),
                        e.to_string(),
                    )))
                    .ok()
                }
            }
        });
        false
    }

    pub fn handle_request_file_finished(&mut self, hash: DigestHash) -> Vec<(JobId, FileId)> {
        let requests = self.requested_from_clients.remove(&hash).unwrap();
        self.locally_cached.insert(hash);
        requests
    }

    pub fn handle_request_file_failed(&mut self, hash: DigestHash) {
        self.requested_from_clients.remove(&hash).unwrap();
    }

    pub fn cas_path(&self, hash: &DigestHash) -> PathBuf {
        self.cas_dir.join(hash)
    }
}

async fn download_file(
    connection: quinn::Connection,
    file: &File,
    download_dir: PathBuf,
    cas_path: PathBuf,
) -> Result<()> {
    let digest = file.digest.as_ref().unwrap();
    let (mut send, mut recv) = connection.open_bi().await?;
    ServerToClientMsg::UploadFileRequest(file.id)
        .send(&mut send)
        .await?;
    send.finish()?;
    let tmp_path = download_dir.join(&digest.hash);
    let mut tokio_file = tokio::fs::File::create(&tmp_path).await?;
    let received = tokio::io::copy(&mut recv, &mut tokio_file).await?;
    tokio_file.flush().await?;
    let exp_size = digest.size_bytes as u64;
    ensure!(
        received == exp_size,
        "size mismatch: received={received} exp={exp_size}"
    );
    let std_file = tokio_file.into_std().await;
    let executable = file.executable.is_some();
    spawn_blocking(move || {
        set_file_permissions(&std_file, executable, true)?;
        move_file_into_cache(&tmp_path, &cas_path, exp_size)
    })
    .await??;
    Ok(())
}

fn move_file_into_cache(src: &PathBuf, dst: &PathBuf, exp_size: u64) -> Result<()> {
    match std::fs::rename(src, dst) {
        Ok(()) => {}
        Err(e) => {
            if !is_blob_cached(dst, exp_size) {
                return Err(e).with_context(|| format!("mv {src:?} -> {dst:?}"));
            }
            // behave like src was moved
            force_remove_file_std(src)?;
        }
    }
    Ok(())
}

fn is_blob_cached(path: &PathBuf, exp_size: u64) -> bool {
    if let Ok(metadata) = std::fs::metadata(path) {
        if !metadata.permissions().readonly() {
            // readonly flag was removed - assume file was modified
            force_remove_file_std(path).ok();
            return false;
        }
        let act_size = metadata.len();
        if act_size != exp_size {
            tracing::warn!("OutputFile has wrong size (act: {act_size}, exp:{exp_size}): {path:?}");
            force_remove_file_std(path).ok();
            return false;
        }
        true
    } else {
        false
    }
}
