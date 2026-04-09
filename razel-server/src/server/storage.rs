use super::*;
use anyhow::{Result, ensure};
use razel::remote_exec::ServerToClientMsg;
use razel::types::{DigestHash, File, FileId, JobId};
use razel::{force_remove_file, force_remove_file_std, set_file_permissions};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::task::spawn_blocking;
use uuid::Uuid;

struct PendingFileRequest {
    job_id: JobId,
    connection: Option<quinn::Connection>,
    file: File,
}

pub struct Storage {
    pub path: PathBuf,
    ac_dir: PathBuf,
    cas_dir: PathBuf,
    download_dir: PathBuf,
    #[allow(dead_code)]
    max_size_gb: Option<usize>,
    pub bytes: u64,
    locally_cached: HashSet<DigestHash>,
    requested_from_clients: HashMap<DigestHash, Vec<PendingFileRequest>>,
}

impl Storage {
    pub fn new(path: PathBuf, max_size_gb: Option<usize>) -> Result<Self> {
        let ac_dir = path.join("cache/ac");
        let cas_dir = path.join("cache/cas");
        let download_dir = path.join("download");
        std::fs::create_dir_all(&ac_dir)?;
        std::fs::create_dir_all(&cas_dir)?;
        std::fs::create_dir_all(&download_dir)?;
        Ok(Self {
            path,
            ac_dir,
            cas_dir,
            download_dir,
            max_size_gb,
            bytes: 0,
            locally_cached: Default::default(),
            requested_from_clients: Default::default(),
        })
    }

    /// Scans the AC and CAS directories.
    pub fn read(&mut self) -> Result<()> {
        let mut bytes = std::fs::read_dir(&self.ac_dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum::<u64>();
        for entry in std::fs::read_dir(&self.cas_dir)?.filter_map(|e| e.ok()) {
            if let Ok(metadata) = entry.metadata() {
                bytes += metadata.len();
            }
            if let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) {
                self.locally_cached.insert(name);
            }
        }
        self.bytes = bytes;
        Ok(())
    }

    /// Returns true if file is locally cached, or requests the file from distributed cache or client.
    pub fn check_if_file_is_cached_or_request_from_client(
        &mut self,
        job_id: JobId,
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
        requests.push(PendingFileRequest {
            job_id,
            connection: Some(connection.clone()),
            file: file.clone(),
        });
        if requests.len() == 1 {
            let connection = requests[0].connection.take().unwrap();
            self.spawn_download_file(connection, job_id, file.clone(), &digest.hash, tx.clone());
        }
        false
    }

    pub fn handle_request_file_finished(&mut self, digest: Digest) -> Vec<(JobId, FileId)> {
        let requests = self.requested_from_clients.remove(&digest.hash).unwrap();
        self.bytes += digest.size_bytes as u64;
        self.locally_cached.insert(digest.hash);
        requests
            .into_iter()
            .map(|r| (r.job_id, r.file.id))
            .collect()
    }

    pub fn handle_request_file_failed(
        &mut self,
        hash: DigestHash,
        tx: &Tx,
    ) -> Vec<(JobId, FileId)> {
        let requests = self.requested_from_clients.get_mut(&hash).unwrap();
        // Find the next entry with an unused connection to retry the download
        if let Some(req) = requests.iter_mut().find(|r| r.connection.is_some()) {
            let conn = req.connection.take().unwrap();
            let file = req.file.clone();
            let job_id = req.job_id;
            self.spawn_download_file(conn, job_id, file, &hash, tx.clone());
            vec![] // retry in progress — nothing to fail yet
        } else {
            // All connections exhausted — remove and fail every waiting job
            self.requested_from_clients
                .remove(&hash)
                .unwrap()
                .into_iter()
                .map(|r| (r.job_id, r.file.id))
                .collect()
        }
    }

    fn spawn_download_file(
        &self,
        connection: quinn::Connection,
        job_id: JobId,
        file: File,
        hash: &DigestHash,
        tx: Tx,
    ) {
        let download_dir = self.download_dir.clone();
        let cas_path = self.cas_path(hash);
        tokio::spawn(async move {
            // TODO check distributed cache first
            match download_file(connection, &file, download_dir, cas_path).await {
                Ok(()) => tx
                    .send(QueueMsg::RequestFileFinished(file.digest.unwrap()))
                    .ok(),
                Err(e) => {
                    tracing::warn!(?job_id, path=?file.path, "download file from client failed: {e}");
                    tx.send(QueueMsg::RequestFileFailed(file.digest.unwrap(), e))
                        .ok()
                }
            }
        });
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
    let tmp_path = download_dir.join(format!("{}-{}", &digest.hash, Uuid::now_v7()));
    let result = download_to_cas(&mut recv, file, digest, &tmp_path, cas_path).await;
    if result.is_err() {
        force_remove_file(&tmp_path).await.ok();
    }
    result
}

async fn download_to_cas(
    recv: &mut quinn::RecvStream,
    file: &File,
    digest: &Digest,
    tmp_path: &PathBuf,
    cas_path: PathBuf,
) -> Result<()> {
    ensure!(
        tmp_path.parent().is_some_and(|p| p.exists()),
        "missing tmp parent directory: {tmp_path:?}"
    );
    ensure!(
        cas_path.parent().is_some_and(|p| p.exists()),
        "missing CAS parent directory: {cas_path:?}"
    );
    let mut tokio_file = tokio::fs::File::create(tmp_path).await?;
    let received = tokio::io::copy(recv, &mut tokio_file).await?;
    tokio_file.flush().await?;
    let exp_size = digest.size_bytes as u64;
    ensure!(
        received == exp_size,
        "size mismatch: received={received} exp={exp_size}"
    );
    let std_file = tokio_file.into_std().await;
    let executable = file.executable.is_some();
    let tmp_path = tmp_path.clone();
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
        Err(_) if is_blob_cached(dst, exp_size) => {
            // behave like src was moved
            force_remove_file_std(src).ok();
        }
        Err(e) => {
            bail!("mv {src:?} -> {dst:?}: {e}");
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
