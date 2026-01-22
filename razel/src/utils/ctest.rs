use crate::cmake_file_api::CMakeFileApi;
use crate::ctest_json::CTestJson;
use anyhow::{Result, anyhow};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{info, instrument};

pub struct CTest {
    pub cmake: CMakeFileApi,
    pub ctest: CTestJson,
}

impl CTest {
    pub fn read(cmake_binary_dir: &Path, cmake_build_type: &str) -> Result<Self> {
        let cmake = CMakeFileApi::read(cmake_binary_dir)?;
        let ctest = CTestJson::read(cmake_binary_dir, cmake_build_type)?;
        Ok(Self { cmake, ctest })
    }

    #[instrument(skip_all)]
    pub fn collect_input_files(&self) -> Result<HashSet<PathBuf>> {
        let bin_dir = &self.cmake.codemodel.paths.build;
        let mut inputs: HashSet<PathBuf> = Default::default();
        let mut push_input = |s: &str, working_dir: &str| {
            let abs = if Path::new(s).is_relative() {
                if working_dir.starts_with(bin_dir) {
                    return;
                }
                PathBuf::from(format!("{working_dir}/{s}"))
            } else {
                PathBuf::from(s)
            };
            inputs.insert(abs);
        };
        for test in &self.ctest.tests {
            let working_dir = test
                .working_dir()
                .ok_or_else(|| anyhow!("WORKING_DIRECTORY is not set: {}", test.name))?;
            if let Some(required_files) = test.required_files() {
                for file in required_files
                    .into_iter()
                    .filter(|f| !f.starts_with(bin_dir))
                {
                    push_input(file, working_dir);
                }
            } else {
                for arg in test.command.iter().filter(|a| !a.starts_with(bin_dir)) {
                    push_input(arg, working_dir);
                }
            }
        }
        info!(tests = self.ctest.tests.len(), inputs = inputs.len());
        Ok(inputs)
    }
}
