use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::Path;
use std::process::Command;

/// CTest JSON Object Model parser.
///
/// Requires ctest >= 3.14. See https://cmake.org/cmake/help/latest/manual/ctest.1.html#show-as-json-object-model
///
/// Note: all paths use unix line separators, even on Windows.
#[derive(Deserialize)]
pub struct CTestJson {
    pub kind: String,
    pub tests: Vec<CTestJsonTest>,
    pub version: CTestJsonVersion,
}

impl CTestJson {
    /// Use `ctest --test-dir <dir> -C <build_type> --show-only=json-v1` to create the JSON Object Model and read it.
    pub fn read(dir: &Path, build_type: &str) -> Result<Self> {
        let command = &[
            "ctest",
            "--test-dir",
            dir.to_str().unwrap(),
            "-C",
            build_type,
            "--show-only=json-v1",
        ];
        let output = Command::new(command[0])
            .args(&command[1..])
            .output()
            .with_context(|| format!("Failed to execute {command:?}"))?;
        if !output.status.success() {
            bail!(
                "Failed to execute {command:?}\nstatus: {}\nstderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Self::from_slice(&output.stdout)
    }

    fn from_slice(text: &[u8]) -> Result<Self> {
        let json: CTestJson = serde_json::from_slice(text)?;
        if json.kind != "ctestInfo" {
            bail!("unknown kind: {:?}", json.kind);
        }
        if json.version.major != 1 {
            bail!("unsupported file version: {:?}", json.version);
        }
        Ok(json)
    }
}

#[derive(Debug, Deserialize)]
pub struct CTestJsonVersion {
    pub major: i32,
    pub minor: i32,
}

#[derive(Deserialize)]
pub struct CTestJsonProperty {
    pub name: String,
    pub value: serde_json::Value,
}

#[derive(Deserialize)]
pub struct CTestJsonTest {
    #[serde(default)]
    pub command: Vec<String>,
    pub name: String,
    pub properties: Vec<CTestJsonProperty>,
}

impl CTestJsonTest {
    /// REQUIRED_FILES: absolute or relative to WORKING_DIRECTORY
    pub fn required_files(&self) -> Option<Vec<&str>> {
        self.properties
            .iter()
            .find(|p| p.name == "REQUIRED_FILES")?
            .value
            .as_array()
            .map(|a| a.iter().map(|v| v.as_str().unwrap()).collect())
    }

    pub fn timeout(&self) -> Option<f64> {
        self.properties
            .iter()
            .find(|p| p.name == "TIMEOUT")
            .and_then(|p| p.value.as_f64())
    }

    pub fn working_dir(&self) -> Option<&str> {
        self.properties
            .iter()
            .find(|p| p.name == "WORKING_DIRECTORY")
            .and_then(|p| p.value.as_str())
    }
}
