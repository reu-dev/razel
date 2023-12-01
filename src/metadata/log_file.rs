use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::metadata::Tag;
use crate::{CacheHit, Command};
use anyhow::Result;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize)]
pub struct LogFileItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub status: ExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheHit>,
    /// original execution duration of the command/task - ignoring cache
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<f32>,
    /// actual duration of processing the command/task - including caching and overheads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<f32>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub measurements: Map<String, Value>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct LogFile {
    pub items: Vec<LogFileItem>,
}

impl LogFile {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = fs::read(path)?;
        let items = serde_json::from_slice(&contents)?;
        Ok(Self { items })
    }

    pub fn push(
        &mut self,
        command: &Command,
        execution_result: &ExecutionResult,
        measurements: Map<String, Value>,
    ) {
        let custom_tags = command
            .tags
            .iter()
            .filter_map(|x| match x {
                Tag::Custom(x) => Some(x.clone()),
                _ => None,
            })
            .collect_vec();
        self.items.push(LogFileItem {
            name: command.name.clone(),
            tags: custom_tags,
            status: execution_result.status,
            cache: execution_result.cache_hit,
            exec: execution_result.exec_duration.map(|x| x.as_secs_f32()),
            total: execution_result.total_duration.map(|x| x.as_secs_f32()),
            measurements,
        });
    }

    pub fn push_not_run(&mut self, command: &Command, status: ExecutionStatus) {
        assert!(status == ExecutionStatus::NotStarted || status == ExecutionStatus::Skipped);
        self.push(
            command,
            &ExecutionResult {
                status,
                ..Default::default()
            },
            Default::default(),
        );
    }

    pub fn write(&self, path: &PathBuf) -> Result<()> {
        let vec = serde_json::to_vec(&self.items)?;
        fs::write(path, vec)?;
        Ok(())
    }
}
