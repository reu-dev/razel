use crate::SandboxDir;
use crate::types::{CaptureRegexTask, EnsureEqualTask, EnsureNotEqualTask, WriteFileTask};
use anyhow::{Result, anyhow, bail};
use regex::Regex;
use tokio::task::spawn_blocking;

impl CaptureRegexTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
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

impl WriteFileTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let output = sandbox_dir.join(&self.file);
        let mut text = self.lines.join("\n");
        text.push('\n');
        tokio::fs::write(output, text).await?;
        Ok(())
    }
}

impl EnsureEqualTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let file1 = sandbox_dir.join(&self.file1);
        let file2 = sandbox_dir.join(&self.file2);
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

impl EnsureNotEqualTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let file1 = sandbox_dir.join(&self.file1);
        let file2 = sandbox_dir.join(&self.file2);
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
