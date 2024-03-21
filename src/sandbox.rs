use crate::config::LinkType;
use anyhow::Context;
use std::path::{Path, PathBuf};
use tokio::fs;

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct Sandbox {
    pub dir: PathBuf,
}

impl Sandbox {
    pub fn cleanup(base_dir: &Path) {
        std::fs::remove_dir_all(base_dir).ok();
    }

    pub fn new(base_dir: &Path, command_id: &str) -> Self {
        Self {
            dir: base_dir.join(command_id),
        }
    }

    /// Create tmp dir, link inputs and create output directories
    pub async fn create(
        &self,
        inputs: &Vec<PathBuf>,
        outputs: &Vec<PathBuf>,
        link_type: LinkType,
    ) -> Result<&PathBuf, anyhow::Error> {
        fs::create_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {:?}", self.dir))?;
        for input in inputs {
            if input.is_absolute() {
                continue;
            }
            let src = input;
            let dst = self.dir.join(input);
            match link_type {
                LinkType::Hardlink => crate::force_hardlink(src, &dst).await?,
                LinkType::Symlink => crate::force_symlink(src, &dst).await?,
            }
        }
        for output in outputs {
            let output_abs = self.dir.join(output);
            let dir = output_abs.parent().unwrap();
            fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("Failed to create sandbox output dir: {dir:?}"))?;
        }
        Ok(&self.dir)
    }

    pub async fn move_output_files_into_out_dir(
        &self,
        output_paths: &Vec<PathBuf>,
    ) -> Result<(), anyhow::Error> {
        for dst in output_paths {
            let src = self.dir.join(dst);
            tokio::fs::rename(&src, &dst)
                .await
                .with_context(|| format!("move_output_files_into_out_dir {src:?} -> {dst:?}"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use crate::tasks::ensure_equal;
    use std::fs;

    const OUTPUT_FILE_CONTENT: &str = "OUTPUT_FILE_CONTENT";

    #[tokio::test]
    async fn no_parent() {
        let base_dir = new_tmp_dir!();
        test_sandbox(base_dir.dir(), "README.md".into(), "output-file.txt".into()).await;
    }

    #[tokio::test]
    async fn two_parents() {
        let base_dir = new_tmp_dir!();
        test_sandbox(
            base_dir.dir(),
            "examples/data/a.csv".into(),
            "examples/data/output-file.txt".into(),
        )
        .await;
    }

    async fn test_sandbox(base_dir: &Path, input: PathBuf, output: PathBuf) {
        let command_id = "0";
        let sandbox = Sandbox::new(base_dir, command_id);
        let sandbox_dir = sandbox
            .create(
                &vec![input.clone()],
                &vec![output.clone()],
                crate::config::SANDBOX_LINK_TYPE,
            )
            .await
            .unwrap();
        let sandbox_input = sandbox_dir.join(&input);
        let sandbox_output = sandbox_dir.join(&output);
        // check input file
        ensure_equal(input, sandbox_input).unwrap();
        // check output file
        assert!(!sandbox_output.exists());
        fs::write(&sandbox_output, OUTPUT_FILE_CONTENT).unwrap();
        assert_eq!(
            fs::read_to_string(sandbox_output).unwrap(),
            OUTPUT_FILE_CONTENT
        );
        sandbox.destroy().await.unwrap();
    }
}
