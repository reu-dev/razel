use crate::executors::AsyncTask;
use crate::make_file_executable;
use async_trait::async_trait;
use futures_util::StreamExt;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct DownloadFileTask {
    pub url: String,
    pub output: PathBuf,
    pub executable: bool,
}

#[async_trait]
impl AsyncTask for DownloadFileTask {
    async fn exec(&self, sandbox_dir: Option<PathBuf>) -> Result<(), anyhow::Error> {
        let mut stream = reqwest::get(&self.url).await?.bytes_stream();
        let mut file = File::create(
            sandbox_dir
                .map(|x| x.join(&self.output))
                .unwrap_or_else(|| PathBuf::from(&self.output)),
        )
        .await?;
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
