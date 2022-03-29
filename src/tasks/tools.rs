use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn write(file_name: String, lines: Vec<String>) -> Result<(), anyhow::Error> {
    let mut file = File::create(file_name).await?;
    file.write_all(lines.join("\n").as_bytes()).await?;
    file.sync_all().await?;
    Ok(())
}

/*
pub async fn ensure_equal(file1: String, file2: String) -> Result<(), anyhow::Error> {
    todo!();
}

pub async fn ensure_not_equal(file1: String, file2: String) -> Result<(), anyhow::Error> {
    todo!();
}

 */
