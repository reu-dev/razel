use crate::types::{RazelJson, TargetId};
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub trait RazelJsonHandler {
    /// Set the directory to resolve relative paths of input/output files
    fn set_workspace_dir(&mut self, dir: &Path);
    fn push_json(&mut self, json: RazelJson) -> Result<TargetId>;
}

impl RazelJson {
    pub fn read(path: &str, handler: &mut impl RazelJsonHandler) -> Result<()> {
        handler.set_workspace_dir(Path::new(path).parent().unwrap());
        let file = BufReader::new(
            fs::File::open(path).with_context(|| anyhow!("failed to open {path:?}"))?,
        );
        for (line_number, line_result) in file.lines().enumerate() {
            let line = line_result?;
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() || line_trimmed.starts_with("//") {
                continue;
            }
            let json: RazelJson = serde_json::from_str(line_trimmed).with_context(|| {
                format!("failed to parse {path}:{}\n{line_trimmed}", line_number + 1)
            })?;
            handler.push_json(json).with_context(|| {
                format!("failed to push {path}:{}\n{line_trimmed}", line_number + 1)
            })?;
        }
        Ok(())
    }
}
