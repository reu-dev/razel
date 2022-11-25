use crate::executors::AsyncTask;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::copy;

pub struct DownloadFileTask {
    pub url: String,
    pub output: PathBuf,
}

#[async_trait]
impl AsyncTask for DownloadFileTask {
    async fn exec(&self, sandbox_dir: Option<PathBuf>) -> Result<(), anyhow::Error> {
        let response = reqwest::get(&self.url).await?;
        let mut file = File::create(
            sandbox_dir
                .map(|x| x.join(&self.output))
                .unwrap_or_else(|| PathBuf::from(&self.output)),
        )
        .await?;
        let content = response.text().await?;
        copy(&mut content.as_bytes(), &mut file).await?;
        Ok(())
    }
}
