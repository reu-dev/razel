use crate::executors::ExecutionResult;
use crate::types::{Tag, Target};
use anyhow::Result;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;

pub struct Profile {
    execution_times: Vec<ExecutionTimesItem>,
}

#[derive(Serialize)]
struct ExecutionTimesItem {
    name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<Tag>,
    time: f32,
}

impl Profile {
    pub fn new() -> Self {
        Self {
            execution_times: vec![],
        }
    }

    pub fn collect(&mut self, target: &Target, execution_result: &ExecutionResult) {
        if let Some(duration) = &execution_result.exec_duration {
            self.execution_times.push(ExecutionTimesItem {
                name: target.name.clone(),
                tags: target.tags.clone(),
                time: duration.as_secs_f32(),
            })
        }
    }

    pub fn write_json(&self, path: &PathBuf) -> Result<()> {
        let vec = serde_json::to_vec(&self.execution_times).unwrap();
        fs::write(path, vec)?;
        Ok(())
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::new()
    }
}
