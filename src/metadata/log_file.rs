use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::metadata::Tag;
use crate::Command;
use anyhow::Result;
use itertools::Itertools;
use serde::Serialize;
use serde_json::{Map, Value};
use std::fs;
use std::path::PathBuf;

#[derive(Serialize)]
pub struct LogFileItem {
    pub name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub status: ExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<f32>,
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub measurements: Map<String, Value>,
}

#[derive(Default)]
pub struct LogFile {
    items: Vec<LogFileItem>,
}

impl LogFile {
    pub fn push(
        &mut self,
        command: &Command,
        execution_result: &ExecutionResult,
        measurements: Map<String, Value>,
    ) {
        if execution_result.success() && measurements.is_empty() {
            return;
        }
        self.items.push(LogFileItem {
            name: command.name.clone(),
            tags: command
                .tags
                .iter()
                .filter_map(|x| match x {
                    Tag::Custom(x) => Some(x.clone()),
                    _ => None,
                })
                .collect_vec(),
            status: execution_result.status,
            duration: execution_result.duration.map(|x| x.as_secs_f32()),
            measurements,
        });
    }

    pub fn write(&self, path: &PathBuf) -> Result<()> {
        let vec = serde_json::to_vec(&self.items).unwrap();
        fs::write(path, vec)?;
        Ok(())
    }
}
