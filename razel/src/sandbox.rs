use crate::config::LinkType;
use anyhow::bail;
use anyhow::{Context, Error};
use async_trait::async_trait;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use tokio::fs;

pub type BoxedSandbox = Box<dyn Sandbox + Send>;

#[async_trait]
pub trait Sandbox {
    fn dir(&self) -> SandboxDir;

    /// Create tmp dir, link inputs and create output directories
    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf, anyhow::Error>;

    async fn move_output_files_into_out_dir(
        &self,
        output_paths: &[PathBuf],
    ) -> Result<(), anyhow::Error>;

    /// Remove tmp dir
    async fn destroy(&self) -> Result<(), anyhow::Error>;
}

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct TmpDirSandbox {
    dir: PathBuf,
    inputs: Vec<PathBuf>,
}

impl TmpDirSandbox {
    pub fn cleanup(base_dir: &Path) {
        std::fs::remove_dir_all(base_dir).ok();
    }

    pub fn new(base_dir: &Path, command_id: &str, inputs: Vec<PathBuf>) -> Self {
        Self {
            dir: base_dir.join(command_id),
            inputs,
        }
    }
}

#[async_trait]
impl Sandbox for TmpDirSandbox {
    fn dir(&self) -> SandboxDir {
        SandboxDir::new(Some(self.dir.clone()))
    }

    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf, anyhow::Error> {
        fs::create_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {:?}", self.dir))?;
        for input in &self.inputs {
            if input.starts_with("..") {
                bail!("input file must be inside of workspace: {input:?}");
            }
            let src = input;
            let dst = self.dir.join(input);
            match crate::config::SANDBOX_LINK_TYPE {
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

    async fn move_output_files_into_out_dir(
        &self,
        output_paths: &[PathBuf],
    ) -> Result<(), anyhow::Error> {
        for dst in output_paths {
            let src = self.dir.join(dst);
            tokio::fs::rename(&src, &dst)
                .await
                .with_context(|| format!("move_output_files_into_out_dir {src:?} -> {dst:?}"))?;
        }
        Ok(())
    }

    async fn destroy(&self) -> Result<(), anyhow::Error> {
        fs::remove_dir_all(&self.dir)
            .await
            .with_context(|| format!("Failed to remove sandbox dir: {:?}", self.dir))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct WasiSandbox {
    tmp_dir_sandbox: TmpDirSandbox,
    inputs: Vec<(PathBuf, Option<PathBuf>)>,
}

impl WasiSandbox {
    pub fn new(base_dir: &Path, command_id: &str, inputs: Vec<(PathBuf, Option<PathBuf>)>) -> Self {
        Self {
            tmp_dir_sandbox: TmpDirSandbox::new(base_dir, command_id, vec![]),
            inputs,
        }
    }
}

#[async_trait]
impl Sandbox for WasiSandbox {
    fn dir(&self) -> SandboxDir {
        self.tmp_dir_sandbox.dir()
    }

    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf, anyhow::Error> {
        let dir = &self.tmp_dir_sandbox.dir;
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {dir:?}"))?;
        for (input, cas_path) in &self.inputs {
            if input.starts_with("..") {
                bail!("input file must be inside of workspace: {input:?}");
            }
            let src = cas_path.as_ref().unwrap_or(input);
            crate::force_hardlink(src, &self.dir().join(input)).await?;
        }
        for output in outputs {
            let output_abs = self.dir().join(output);
            let dir = output_abs.parent().unwrap();
            fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("Failed to create sandbox output dir: {dir:?}"))?;
        }
        Ok(dir)
    }

    async fn move_output_files_into_out_dir(&self, output_paths: &[PathBuf]) -> Result<(), Error> {
        self.tmp_dir_sandbox
            .move_output_files_into_out_dir(output_paths)
            .await
    }

    async fn destroy(&self) -> Result<(), Error> {
        self.tmp_dir_sandbox.destroy().await
    }
}

pub struct SandboxDir {
    pub dir: Option<PathBuf>,
}

impl SandboxDir {
    pub fn new(dir: Option<PathBuf>) -> Self {
        Self { dir }
    }

    pub fn join<S: AsRef<OsStr>>(&self, path: &S) -> PathBuf {
        let path = Path::new(&path);
        self.dir
            .as_ref()
            .map_or_else(|| PathBuf::from(path), |s| s.join(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use crate::test_utils::ensure_files_are_equal;
    use std::fs;

    const OUTPUT_FILE_CONTENT: &str = "OUTPUT_FILE_CONTENT";

    #[tokio::test]
    async fn no_parent() {
        let base_dir = new_tmp_dir!();
        test_sandbox(base_dir.dir(), "Cargo.toml".into(), "output.txt".into()).await;
    }

    #[tokio::test]
    async fn two_parents() {
        let base_dir = new_tmp_dir!();
        test_sandbox(
            base_dir.dir(),
            "src/utils/tui.rs".into(),
            "src/utils/output.rs".into(),
        )
        .await;
    }

    async fn test_sandbox(base_dir: &Path, input: PathBuf, output: PathBuf) {
        let command_id = "0";
        let sandbox = TmpDirSandbox::new(base_dir, command_id, vec![input.clone()]);
        let sandbox_dir = sandbox.create(std::slice::from_ref(&output)).await.unwrap();
        let sandbox_input = sandbox_dir.join(&input);
        let sandbox_output = sandbox_dir.join(&output);
        // check input file
        ensure_files_are_equal(input, sandbox_input).unwrap();
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
