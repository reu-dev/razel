use std::path::PathBuf;
use std::process;

use tokio::fs;

use crate::{config, force_symlink};

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct Sandbox {
    pub dir: PathBuf,
}

impl Sandbox {
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
        fs::create_dir_all(&self.dir).await?;
        for input in inputs {
            if input.is_absolute() {
                continue;
            }
            let src = fs::canonicalize(&input).await?;
            let dst = self.dir.join(&input);
            force_symlink(&src, &dst).await?;
        }
        for output in outputs {
            fs::create_dir_all(self.dir.join(&output).parent().unwrap()).await?;
        }
        Ok(())
    }

    /// Remove tmp dir
    pub async fn destroy(&self) -> Result<(), anyhow::Error> {
        fs::remove_dir_all(&self.dir).await?;
        Ok(())
    }
}
