use crate::types::DownloadFileTask;
use crate::{SandboxDir, make_file_executable};
use anyhow::Result;
use futures_util::StreamExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;

impl DownloadFileTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let mut stream = reqwest::get(&self.url)
            .await?
            .error_for_status()?
            .bytes_stream();
        let path = sandbox_dir.join(&self.output);
        let mut file = fs::File::create(&path).await?;
        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        if self.executable {
            make_file_executable(&file).await?;
        }
        file.sync_all().await?;
        drop(file.into_std().await); // close now instead of deferred in background
        Ok(())
    }
}
