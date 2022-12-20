use std::path::PathBuf;
use std::process;

use anyhow::Context;
use tokio::fs;

use crate::{config, force_symlink};

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct Sandbox {
    pub dir: PathBuf,
}

impl Sandbox {
    pub fn cleanup() {
        let base_dir: PathBuf = [config::SANDBOX_DIR, ".sandbox"].iter().collect();
        std::fs::remove_dir_all(base_dir).ok();
    }

    pub fn new(command_id: &String) -> Self {
        Self {
            dir: [
                config::SANDBOX_DIR,
                ".sandbox",
                &process::id().to_string(),
                command_id,
            ]
            .iter()
            .collect(),
        }
    }

    /// Create tmp dir, symlink inputs and create output directories
    pub async fn create(
        &self,
        inputs: &Vec<PathBuf>,
        outputs: &Vec<PathBuf>,
    ) -> Result<(), anyhow::Error> {
        fs::create_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {:?}", self.dir))?;
        for input in inputs {
            if input.is_absolute() {
                continue;
            }
            let src = fs::canonicalize(&input)
                .await
                .with_context(|| format!("Error in canonicalize({:?})", input))?;
            let dst = self.dir.join(input);
            force_symlink(&src, &dst).await?;
        }
        for output in outputs {
            let output_abs = self.dir.join(output);
            let dir = output_abs.parent().unwrap();
            fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("Failed to create sandbox output dir: {:?}", dir))?;
        }
        Ok(())
    }

    /// Remove tmp dir
    pub async fn destroy(&self) -> Result<(), anyhow::Error> {
        fs::remove_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to remove sandbox dir: {:?}", self.dir))?;
        Ok(())
    }
}
