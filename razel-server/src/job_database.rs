use crate::webui_types::FinishedJobStats;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
pub struct FinishedJob {
    pub stats: FinishedJobStats,
}

pub struct JobDatabase {
    pub dir: PathBuf,
    pub jobs: Vec<FinishedJob>,
    pub bytes: u64,
}

impl JobDatabase {
    pub fn new(storage_path: &Path) -> Result<Self> {
        let dir = storage_path.join("jobs");
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            jobs: vec![],
            bytes: 0,
        })
    }

    pub fn read_jobs(&mut self) -> Result<()> {
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("postcard") {
                continue;
            }
            let bytes =
                std::fs::read(&path).map_err(|e| anyhow!("failed to read {path:?}: {e}"))?;
            let job: FinishedJob = postcard::from_bytes(&bytes)
                .map_err(|e| anyhow!("failed to deserialize {path:?}: {e}"))?;
            self.bytes += bytes.len() as u64;
            self.jobs.push(job);
        }
        Ok(())
    }

    pub fn push(&mut self, job: FinishedJob) {
        let filename = format!("{}.postcard", job.stats.id);
        let path = self.dir.join(filename);
        match postcard::to_stdvec(&job) {
            Ok(bytes) => {
                self.bytes += bytes.len() as u64;
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::write(&path, &bytes).await {
                        eprintln!("job_database: failed to write {}: {e}", path.display());
                    }
                });
            }
            Err(e) => {
                eprintln!(
                    "job_database: failed to serialize job {}: {e}",
                    job.stats.id
                );
            }
        }
        self.jobs.push(job);
    }
}
