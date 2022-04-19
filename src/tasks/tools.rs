use std::fs::File;
use std::io::Write;

pub fn write(file_name: String, lines: Vec<String>) -> Result<(), anyhow::Error> {
    let mut file = File::create(file_name)?;
    let mut text = lines.join("\n");
    text.push('\n');
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
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
