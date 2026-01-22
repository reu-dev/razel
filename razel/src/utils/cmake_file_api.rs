use crate::read_json_file;
use anyhow::{Context, Result, anyhow, bail};
use itertools::Itertools;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::warn;

/// CMake file API parser.
///
/// See https://cmake.org/cmake/help/latest/manual/cmake-file-api.7.html
///
/// Note: all paths use unix line separators, even on Windows.
#[derive(Deserialize)]
pub struct CMakeFileApi {
    pub reply_dir: PathBuf,
    pub codemodel: Codemodel,
}

impl CMakeFileApi {
    /// Instruct CMake to create file-based API. To be called before cmake.
    ///
    /// Write v1 Client Stateless Query File.
    pub fn write_query(cmake_binary_dir: &Path) -> Result<()> {
        let query_dir = cmake_binary_dir.join(".cmake/api/v1/query/client-razel");
        fs::create_dir_all(&query_dir)?;
        let path = query_dir.join("codemodel-v2");
        fs::write(path, "")?;
        Ok(())
    }

    pub fn read(cmake_binary_dir: &Path) -> Result<Self> {
        let reply_dir = reply_dir(cmake_binary_dir);
        let index = read_index(&reply_dir)?;
        let codemodel_file = get_codemodel_file_from_index(&index)
            .ok_or(anyhow!("get_codemodel_file_from_index"))?;
        let codemodel = read_json_file(&reply_dir.join(codemodel_file))?;
        Ok(Self {
            reply_dir,
            codemodel,
        })
    }

    pub fn collect_input_files(&self, cmake_build_type: &str) -> Result<HashSet<PathBuf>> {
        self.warn_on_outdated_version();
        let src_dir = &self.codemodel.paths.source;
        let bin_dir = &self.codemodel.paths.build;
        let mut inputs: HashSet<PathBuf> = Default::default();
        let configuration = if self.codemodel.configurations.len() == 1 {
            &self.codemodel.configurations[0]
        } else {
            self.codemodel
                .configurations
                .iter()
                .find(|c| c.name.to_lowercase() == cmake_build_type.to_lowercase())
                .ok_or_else(|| anyhow!("configuration to found: {cmake_build_type}"))?
        };
        for target in configuration
            .abstractTargets
            .iter()
            .chain(configuration.targets.iter())
        {
            let target = target.read(&self.reply_dir)?;
            if target.imported {
                inputs.extend(
                    target
                        .artifacts
                        .into_iter()
                        .map(|a| a.path)
                        .filter(|p| p.starts_with(src_dir))
                        .map(PathBuf::from),
                );
            }
            inputs.extend(
                target
                    .sources
                    .into_iter()
                    .map(|a| a.path)
                    .map(|p| {
                        if Path::new(&p).is_relative() {
                            format!("{src_dir}/{p}")
                        } else {
                            p
                        }
                    })
                    .filter(|p| !p.starts_with(bin_dir))
                    .map(PathBuf::from),
            );
        }
        Ok(inputs)
    }

    fn warn_on_outdated_version(&self) {
        let version = &self.codemodel.version;
        match (version.major, version.minor) {
            // abstract targets were introduced in codemodel version 2.9 (CMake 4.2)
            (..2, _) | (2, ..9) => warn!(
                "Outdated CMake version. Abstract targets (e.g. imported libraries) require >= 4.2"
            ),
            _ => {}
        }
    }
}

#[derive(Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}

#[derive(Deserialize)]
pub struct Codemodel {
    pub configurations: Vec<CodemodelConfiguration>,
    pub paths: CodemodelPaths,
    pub version: Version,
}

#[allow(nonstandard_style)]
#[derive(Deserialize)]
pub struct CodemodelPaths {
    pub build: String,
    pub source: String,
}

#[allow(nonstandard_style)]
#[derive(Deserialize)]
pub struct CodemodelConfiguration {
    /// e.g. Release
    pub name: String,
    #[serde(default)]
    pub abstractTargets: Vec<CodemodelTarget>,
    #[serde(default)]
    pub targets: Vec<CodemodelTarget>,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct CodemodelTarget {
    pub jsonFile: PathBuf,
}

impl CodemodelTarget {
    pub fn read(&self, query_dir: &Path) -> Result<Target> {
        read_json_file(&query_dir.join(&self.jsonFile))
    }
}

#[derive(Deserialize)]
pub struct Target {
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
    #[serde(default)]
    pub imported: bool,
    pub sources: Vec<Source>,
    /// MODULE_LIBRARY, SHARED_LIBRARY
    #[serde(rename = "type")]
    pub r_type: String,
}

#[derive(Deserialize)]
pub struct Artifact {
    pub path: String,
}

#[derive(Deserialize)]
pub struct Source {
    pub path: String,
}

fn reply_dir(cmake_binary_dir: &Path) -> PathBuf {
    cmake_binary_dir.join(".cmake/api/v1/reply")
}

/// Read latest file matching index-*.json
fn read_index(reply_dir: &Path) -> Result<serde_json::Value> {
    let Some(path) = fs::read_dir(reply_dir)
        .with_context(|| format!("Failed to read directory: {:?}", reply_dir))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with("index-") && name.ends_with(".json"))
                .unwrap_or(false)
        })
        .sorted()
        .last()
    else {
        bail!("No index-*.json file found in {reply_dir:?}");
    };
    read_json_file(&path)
}

fn get_codemodel_file_from_index(index: &Value) -> Option<PathBuf> {
    index
        .as_object()?
        .get("reply")?
        .as_object()?
        .get("client-razel")?
        .as_object()?
        .get("codemodel-v2")?
        .as_object()?
        .get("jsonFile")?
        .as_str()
        .map(PathBuf::from)
}
