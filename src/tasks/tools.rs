use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{anyhow, bail};
use regex::Regex;

pub fn capture_regex(input: PathBuf, output: PathBuf, re: String) -> Result<(), anyhow::Error> {
    let regex = Regex::new(&re)?;
    let bytes = std::fs::read(input)?;
    let text = String::from_utf8_lossy(&bytes);
    let captures = regex
        .captures(&text)
        .ok_or(anyhow!("Regex did not match"))?;
    // first group is whole match
    if captures.len() != 2 {
        bail!("Regex should capture a single group: {captures:?}");
    }
    let capture = &captures[1];
    let mut file = File::create(output)?;
    file.write_all(capture.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

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
