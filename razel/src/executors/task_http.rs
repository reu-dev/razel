use crate::executors::AsyncTask;
use crate::types::DownloadFileTask;
use crate::{make_file_executable, SandboxDir};
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[async_trait]
impl AsyncTask for DownloadFileTask {
    async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<(), anyhow::Error> {
        let mut stream = reqwest::get(&self.url)
            .await?
            .error_for_status()?
            .bytes_stream();
        let mut file = fs::File::create(sandbox_dir.join(&self.output)).await?;
        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
        }
        if self.executable {
            make_file_executable(&file).await?;
        }
        file.sync_all().await?;
        Ok(())
    }
}
