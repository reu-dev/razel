use crate::config::LinkType;
use anyhow::bail;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use tokio::fs;

pub type BoxedSandbox = Box<dyn Sandbox + Send>;

#[async_trait]
pub trait Sandbox {
    fn dir(&self) -> SandboxDir;

    /// Create tmp dir, link inputs and create output directories
    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf>;

    async fn move_output_files_into_out_dir(&self, output_paths: &[PathBuf]) -> Result<()>;

    /// Remove tmp dir
    async fn destroy(&self) -> Result<()>;
}

/// TODO sandbox does not stop writing to input files
#[derive(Debug)]
pub struct TmpDirSandbox {
    ws_dir: PathBuf,
    tmp_dir: PathBuf,
    inputs: Vec<PathBuf>,
}

impl TmpDirSandbox {
    pub fn cleanup(base_dir: &Path) {
        std::fs::remove_dir_all(base_dir).ok();
    }

    pub fn new(ws_dir: PathBuf, base_dir: &Path, command_id: &str, inputs: Vec<PathBuf>) -> Self {
        Self {
            ws_dir,
            tmp_dir: base_dir.join(command_id),
            inputs,
        }
    }
}

#[async_trait]
impl Sandbox for TmpDirSandbox {
    fn dir(&self) -> SandboxDir {
        SandboxDir::new(Some(self.tmp_dir.clone()))
    }

    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf> {
        fs::create_dir_all(&self.tmp_dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {:?}", self.tmp_dir))?;
        for input in &self.inputs {
            if input.is_absolute() || input.starts_with("..") {
                bail!("TmpDirSandbox input file has scary path: {input:?}");
            }
            let ws_path = self.ws_dir.join(input);
            let tmp_path = self.tmp_dir.join(input);
            match crate::config::SANDBOX_LINK_TYPE {
                LinkType::Hardlink => crate::force_hardlink(&ws_path, &tmp_path).await?,
                LinkType::Symlink => crate::force_symlink(&ws_path, &tmp_path).await?,
            }
        }
        for output in outputs {
            let output_abs = self.tmp_dir.join(output);
            let dir = output_abs.parent().unwrap();
            fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("Failed to create sandbox output dir: {dir:?}"))?;
        }
        Ok(&self.tmp_dir)
    }

    async fn move_output_files_into_out_dir(&self, output_paths: &[PathBuf]) -> Result<()> {
        for output in output_paths {
            let ws_path = self.ws_dir.join(output);
            let tmp_path = self.tmp_dir.join(output);
            tokio::fs::rename(&tmp_path, &ws_path)
                .await
                .with_context(|| {
                    format!("move_output_files_into_out_dir {tmp_path:?} -> {output:?}")
                })?;
        }
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        fs::remove_dir_all(&self.tmp_dir)
            .await
            .with_context(|| format!("Failed to remove sandbox dir: {:?}", self.tmp_dir))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct WasiSandbox {
    tmp_dir_sandbox: TmpDirSandbox,
    inputs: Vec<(PathBuf, Option<PathBuf>)>,
}

impl WasiSandbox {
    pub fn new(
        ws_dir: PathBuf,
        base_dir: &Path,
        command_id: &str,
        inputs: Vec<(PathBuf, Option<PathBuf>)>,
    ) -> Self {
        Self {
            tmp_dir_sandbox: TmpDirSandbox::new(ws_dir, base_dir, command_id, vec![]),
            inputs,
        }
    }
}

#[async_trait]
impl Sandbox for WasiSandbox {
    fn dir(&self) -> SandboxDir {
        self.tmp_dir_sandbox.dir()
    }

    async fn create(&self, outputs: &[PathBuf]) -> Result<&PathBuf> {
        let tmp_dir = &self.tmp_dir_sandbox.tmp_dir;
        fs::create_dir_all(&tmp_dir)
            .await
            .with_context(|| format!("Failed to create sandbox dir: {tmp_dir:?}"))?;
        for (input, cas_path) in &self.inputs {
            if input.is_absolute() || input.starts_with("..") {
                bail!("WasiSandbox input file has scary path: {input:?}");
            }
            let src = if let Some(cas_path) = cas_path {
                cas_path.to_path_buf()
            } else {
                self.tmp_dir_sandbox.ws_dir.join(input)
            };
            let tmp_path = self.dir().join(input);
            crate::force_hardlink(&src, &tmp_path).await?;
        }
        for output in outputs {
            let output_abs = self.dir().join(output);
            let dir = output_abs.parent().unwrap();
            fs::create_dir_all(&dir)
                .await
                .with_context(|| format!("Failed to create sandbox output dir: {dir:?}"))?;
        }
        Ok(tmp_dir)
    }

    async fn move_output_files_into_out_dir(&self, output_paths: &[PathBuf]) -> Result<()> {
        self.tmp_dir_sandbox
            .move_output_files_into_out_dir(output_paths)
            .await
    }

    async fn destroy(&self) -> Result<()> {
        self.tmp_dir_sandbox.destroy().await
    }
}

#[derive(Debug)]
pub struct WorkspaceDirSandbox {
    dir: PathBuf,
}

impl WorkspaceDirSandbox {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
}

#[async_trait]
impl Sandbox for WorkspaceDirSandbox {
    fn dir(&self) -> SandboxDir {
        SandboxDir::new(Some(self.dir.clone()))
    }

    async fn create(&self, _outputs: &[PathBuf]) -> Result<&PathBuf> {
        Ok(&self.dir)
    }

    async fn move_output_files_into_out_dir(&self, _output_paths: &[PathBuf]) -> Result<()> {
        Ok(())
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct SandboxDir {
    pub dir: Option<PathBuf>,
}

impl SandboxDir {
    pub fn new(dir: Option<PathBuf>) -> Self {
        Self { dir }
    }

    pub fn join<S: AsRef<OsStr> + ?Sized>(&self, path: &S) -> PathBuf {
        let path = Path::new(&path);
        self.dir
            .as_ref()
            .map_or_else(|| PathBuf::from(path), |s| s.join(path))
    }
}

impl From<Option<PathBuf>> for SandboxDir {
    fn from(dir: Option<PathBuf>) -> Self {
        Self::new(dir)
    }
}

impl From<PathBuf> for SandboxDir {
    fn from(dir: PathBuf) -> Self {
        Self::new(Some(dir))
    }
}

impl From<&PathBuf> for SandboxDir {
    fn from(dir: &PathBuf) -> Self {
        Self::new(Some(dir.clone()))
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
        let sandbox = TmpDirSandbox::new(
            PathBuf::from("."),
            base_dir,
            command_id,
            vec![input.clone()],
        );
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
