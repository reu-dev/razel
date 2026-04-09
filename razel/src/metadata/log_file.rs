use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::metadata::LogWriter;
use crate::read_json_file;
use crate::types::{CacheHit, Tag, Target};
use anyhow::Result;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Serialize)]
pub struct LogFileItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub status: ExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheHit>,
    /// original execution duration of the target - ignoring cache
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<f32>,
    /// actual duration of processing the target - including caching and overheads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<f32>,
    /// total size of all output files and stdout/stderr [bytes]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub measurements: Map<String, Value>,
}

impl LogFileItem {
    pub fn kilobyte_per_second(&self) -> Option<f32> {
        self.exec
            .map(|exec| self.output_size.unwrap_or_default() as f32 / exec / 1000.0)
    }

    pub fn time_saved_by_cache(&self) -> Option<f32> {
        match (self.cache, self.exec, self.total) {
            (Some(_), Some(exec), Some(total)) => Some(exec - total),
            _ => None,
        }
    }
}

pub struct LogFile {
    pub path: PathBuf,
    pub items: Vec<LogFileItem>,
}

impl LogFile {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            items: vec![],
        }
    }

    pub fn from_path<P: AsRef<Path> + Debug>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let items = read_json_file(path.as_ref())?;
        Ok(Self { path, items })
    }

    /// Write a json file with one item per line
    pub fn write(&self) -> Result<()> {
        let mut writer = BufWriter::new(File::create(&self.path)?);
        writer.write_all(b"[\n")?;
        let mut is_first = true;
        for item in &self.items {
            if is_first {
                is_first = false;
            } else {
                writer.write_all(b",\n")?;
            }
            writer.write_all(&serde_json::to_vec(item)?)?;
        }
        writer.write_all(b"\n]\n")?;
        writer.flush()?;
        Ok(())
    }
}

impl LogWriter for LogFile {
    fn push_target_finished(
        &mut self,
        target: &Target,
        execution_result: &ExecutionResult,
        output_size: Option<u64>,
        measurements: &Map<String, Value>,
    ) {
        debug_assert!(!self.items.iter().any(|x| x.name == target.name));
        self.items.push(LogFileItem {
            name: target.name.clone(),
            tags: custom_tags(target),
            status: execution_result.status,
            error: execution_result.error.clone(),
            cache: execution_result.cache_hit,
            exec: execution_result.exec_duration.map(|x| x.as_secs_f32()),
            total: execution_result.total_duration.map(|x| x.as_secs_f32()),
            output_size: output_size.filter(|&x| x != 0),
            measurements: measurements.clone(),
        });
    }

    fn push_target_not_run(&mut self, target: &Target, status: ExecutionStatus) {
        assert!(status == ExecutionStatus::NotStarted || status == ExecutionStatus::Skipped);
        self.items.push(LogFileItem {
            name: target.name.clone(),
            tags: custom_tags(target),
            status,
            error: None,
            cache: None,
            exec: None,
            total: None,
            output_size: None,
            measurements: Default::default(),
        });
    }

    fn finish(&self) -> Result<()> {
        self.write()
    }
}

fn custom_tags(target: &Target) -> Vec<String> {
    target
        .tags
        .iter()
        .filter_map(|x| match x {
            Tag::Custom(x) => Some(x.clone()),
            _ => None,
        })
        .collect_vec()
}
