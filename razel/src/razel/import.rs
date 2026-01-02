use super::Razel;
use crate::types::RazelJson;
use anyhow::Result;
use std::path::Path;

impl Razel {
    pub fn write_jsonl(&self, output: &Path) -> Result<()> {
        RazelJson::write(
            &self.dep_graph.targets,
            &self.dep_graph.files,
            &self.out_dir,
            output,
        )
    }
}
