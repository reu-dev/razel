use anyhow::{Context, Result};
use razel::metadata::LogFile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

pub const BENCHES_OUT_DIR: &str = "benches";

#[derive(Debug, Serialize, Deserialize)]
pub struct Bench {
    pub id: String,
    #[serde(skip)]
    pub path: PathBuf,
    pub title: String,
    pub cache_state: CacheState,
    pub timestamp: u128,
    pub duration: f32,
    pub remote_cache_stats_before: Option<Value>,
    pub remote_cache_stats_after: Option<Value>,
}

impl Bench {
    pub fn from_path(path: PathBuf) -> Result<Self> {
        let file = File::open(&path).with_context(|| format!("{path:?}"))?;
        let mut bench: Self = serde_json::from_reader(BufReader::new(file))?;
        bench.path = path;
        Ok(bench)
    }

    pub fn write(&self) -> Result<()> {
        let path = &self.path;
        fs::write(path, serde_json::to_string_pretty(self).unwrap())
            .with_context(|| format!("{path:?}"))?;
        Ok(())
    }

    pub fn log_file(&self) -> Result<LogFile> {
        LogFile::from_path(self.log_file_path())
    }

    pub fn log_file_path(&self) -> PathBuf {
        self.path
            .parent()
            .unwrap()
            .join(format!("bench.{}.log.json", self.id))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum CacheState {
    /// Local execution from scratch
    LocalCold,
    /// Local execution zero-check
    LocalWarm,
    /// CI execution from scratch
    LocalColdRemoteCold,
    /// CI execution on new node
    LocalColdRemoteWarm,
}

impl CacheState {
    pub fn is_remote_cache_used(&self) -> bool {
        match self {
            CacheState::LocalCold | CacheState::LocalWarm => false,
            CacheState::LocalColdRemoteCold | CacheState::LocalColdRemoteWarm => true,
        }
    }
}
