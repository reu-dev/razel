use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;

pub fn write_file(file_name: PathBuf, lines: Vec<String>) -> Result<(), anyhow::Error> {
    let mut file = File::create(file_name)?;
    let mut text = lines.join("\n");
    text.push('\n');
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

pub fn ensure_equal(file1: PathBuf, file2: PathBuf) -> Result<(), anyhow::Error> {
    let file1_bytes = std::fs::read(&file1)?;
    let file2_bytes = std::fs::read(&file2)?;
    if file1_bytes != file2_bytes {
        bail!("Files {:?} and {:?} differ!", file1, file2);
    }
    Ok(())
}

pub fn ensure_not_equal(file1: PathBuf, file2: PathBuf) -> Result<(), anyhow::Error> {
    let file1_bytes = std::fs::read(&file1)?;
    let file2_bytes = std::fs::read(&file2)?;
    if file1_bytes == file2_bytes {
        bail!("Files {:?} and {:?} are equal!", file1, file2);
    }
    Ok(())
}
