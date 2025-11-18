use crate::executors::AsyncTask;
use crate::types::{CaptureRegexTask, EnsureEqualTask, EnsureNotEqualTask, WriteFileTask};
use crate::SandboxDir;
use anyhow::{anyhow, bail, Result};
use regex::Regex;
use tokio::task::spawn_blocking;

impl AsyncTask for CaptureRegexTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let regex = Regex::new(&self.regex)?;
        let input = sandbox_dir.join(&self.input);
        let output = sandbox_dir.join(&self.output);
        spawn_blocking(move || {
            let text = std::fs::read_to_string(input)?;
            let captures = regex
                .captures(&text)
                .ok_or(anyhow!("Regex did not match"))?;
            // first group is whole match
            if captures.len() != 2 {
                bail!("Regex should capture a single group: {captures:?}");
            }
            let capture = &captures[1];
            std::fs::write(output, capture)?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

impl AsyncTask for WriteFileTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let output = sandbox_dir.join(&self.file);
        let mut text = self.lines.join("\n");
        text.push('\n');
        tokio::fs::write(output, text).await?;
        Ok(())
    }
}

impl AsyncTask for EnsureEqualTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        assert!(sandbox_dir.dir.is_none());
        let file1 = self.file1.clone();
        let file2 = self.file2.clone();
        spawn_blocking(move || {
            let file1_bytes = std::fs::read(&file1)?;
            let file2_bytes = std::fs::read(&file2)?;
            if file1_bytes != file2_bytes {
                bail!("Files {file1:?} and {file2:?} differ!");
            }
            Ok(())
        })
        .await??;
        Ok(())
    }
}

impl AsyncTask for EnsureNotEqualTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        assert!(sandbox_dir.dir.is_none());
        let file1 = self.file1.clone();
        let file2 = self.file2.clone();
        spawn_blocking(move || {
            let file1_bytes = std::fs::read(&file1)?;
            let file2_bytes = std::fs::read(&file2)?;
            if file1_bytes == file2_bytes {
                bail!("Files {file1:?} and {file2:?} are equal!");
            }
            Ok(())
        })
        .await??;
        Ok(())
    }
}
