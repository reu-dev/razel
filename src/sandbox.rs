use crate::force_symlink;
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
    ) -> Result<&PathBuf, anyhow::Error> {
        fs::create_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {:?}", self.dir))?;
        for input in inputs {
            if input.is_absolute() {
                //todo!("also link absolute paths into sandbox - needed for wasi_ctx: {input:?}");
                continue;
            }
            let src = input;
            let dst = self.dir.join(input);
            force_symlink(src, &dst).await?;
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
    use crate::tasks::ensure_equal;
    use crate::{new_tmp_dir, unique_test_name};
    use std::fs;

    const OUTPUT_FILE_CONTENT: &str = "OUTPUT_FILE_CONTENT";

    #[tokio::test]
    async fn no_parent() {
        test_sandbox(
            unique_test_name!(),
            "README.md".into(),
            "output-file.txt".into(),
        )
        .await;
    }

    #[tokio::test]
    async fn two_parents() {
        test_sandbox(
            unique_test_name!(),
            "test/data/a.csv".into(),
            "test/data/output-file.txt".into(),
        )
        .await;
    }

    async fn test_sandbox(command_id: String, input: PathBuf, output: PathBuf) {
        let base_dir = new_tmp_dir!();
        let sandbox = Sandbox::new(&base_dir.join("sandbox"), &command_id);
        let sandbox_dir = sandbox
            .create(&vec![input.clone()], &vec![output.clone()])
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
