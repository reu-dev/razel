use std::path::PathBuf;
use std::process;

use anyhow::Context;
use tokio::fs;

use crate::{config, Arena, Command, File};

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct Sandbox {
    pub dir: PathBuf,
    inputs: Vec<String>,
    outputs: Vec<String>,
}

impl Sandbox {
    pub fn new(command: &Command, files: &Arena<File>) -> Self {
        Self {
            dir: [
                config::SANDBOX_DIR,
                ".sandbox",
                &process::id().to_string(),
                &command.id.to_string(),
            ]
            .iter()
            .collect(),
            inputs: command
                .inputs
                .iter()
                .map(|x| files[*x].path.clone())
                .collect(),
            outputs: command
                .outputs
                .iter()
                .map(|x| files[*x].path.clone())
                .collect(),
        }
    }

    /// Create tmp dir, symlink inputs and create output directories
    pub async fn create_and_provide_inputs(&self) -> Result<(), anyhow::Error> {
        fs::create_dir_all(&self.dir).await?;
        for input in &self.inputs {
            let src = fs::canonicalize(&input).await?;
            let dst = self.dir.join(&input);
            fs::create_dir_all(dst.parent().unwrap()).await?;
            fs::symlink(&src, &dst)
                .await
                .with_context(|| format!("symlink {:?} -> {:?}", src, dst))?;
        }
        for output in &self.outputs {
            fs::create_dir_all(self.dir.join(&output).parent().unwrap()).await?;
        }
        Ok(())
    }

    /// Copy outputs and remove tmp dir
    pub async fn handle_outputs_and_destroy(&self) -> Result<(), anyhow::Error> {
        for output in &self.outputs {
            let dst = PathBuf::from(output);
            let src = self.dir.join(&dst);
            fs::create_dir_all(dst.parent().unwrap()).await?;
            fs::rename(&src, &dst)
                .await
                .with_context(|| format!("mv {:?} -> {:?}", src, dst))?;
        }
        fs::remove_dir_all(&self.dir).await?;
        Ok(())
    }
}
