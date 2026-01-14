use crate::read_json_file;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{info, instrument};

/// CTest JSON Object Model parser.
///
/// Requires ctest >= 3.14. See https://cmake.org/cmake/help/latest/manual/ctest.1.html#show-as-json-object-model
#[derive(Deserialize)]
pub struct CTestJson {
    pub kind: String,
    pub tests: Vec<CTestJsonTest>,
    pub version: CTestJsonVersion,
}

impl CTestJson {
    /// Read a file generated with `ctest --show-only=json-v1`
    pub fn read(path: &Path) -> Result<Self> {
        read_json_file(path)
    }

    /// Use `ctest --test-dir <dir> --show-only=json-v1` to create the JSON Object Model and read it.
    pub fn create(dir: &Path) -> Result<Self> {
        let command = &[
            "ctest",
            "--test-dir",
            dir.to_str().unwrap(),
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

    #[instrument(skip_all)]
    pub fn collect_input_files(&self) -> Result<HashSet<PathBuf>> {
        let mut inputs: HashSet<PathBuf> = Default::default();
        for test in &self.tests {
            if let Some(required_files) = test.required_files() {
                inputs.extend(required_files);
            }
        }
        info!(tests = self.tests.len(), inputs = inputs.len());
        Ok(inputs)
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
    pub fn required_files(&self) -> Option<Vec<PathBuf>> {
        let prop = self
            .properties
            .iter()
            .find(|p| p.name == "REQUIRED_FILES")?;
        let working_dir = self.working_dir();
        Some(
            prop.value
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    let p = Path::new(v.as_str().unwrap());
                    if p.is_relative() {
                        working_dir.as_ref().unwrap().join(p)
                    } else {
                        p.to_path_buf()
                    }
                })
                .collect(),
        )
    }

    pub fn timeout(&self) -> Option<f64> {
        self.properties
            .iter()
            .find(|p| p.name == "TIMEOUT")
            .and_then(|p| p.value.as_f64())
    }

    pub fn working_dir(&self) -> Option<PathBuf> {
        self.properties
            .iter()
            .find(|p| p.name == "WORKING_DIRECTORY")
            .map(|p| PathBuf::from(p.value.to_string()))
    }
}
