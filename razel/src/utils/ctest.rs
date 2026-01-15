use crate::cmake_file_api::CMakeFileApi;
use crate::ctest_json::CTestJson;
use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{info, instrument};

pub struct CTest {
    pub cmake: CMakeFileApi,
    pub ctest: CTestJson,
}

impl CTest {
    pub fn read(cmake_binary_dir: &Path) -> Result<Self> {
        let cmake = CMakeFileApi::read(cmake_binary_dir)?;
        let ctest = CTestJson::read(cmake_binary_dir)?;
        Ok(Self { cmake, ctest })
    }

    #[instrument(skip_all)]
    pub fn collect_input_files(&self) -> Result<HashSet<PathBuf>> {
        let src_dir = &self.cmake.codemodel.paths.source;
        let bin_dir = &self.cmake.codemodel.paths.build;
        let src_dir_str = src_dir.to_string_lossy().to_string();
        let bin_dir_str = bin_dir.to_string_lossy().to_string();
        let mut inputs: HashSet<PathBuf> = Default::default();
        for test in &self.ctest.tests {
            for arg in test
                .command
                .iter()
                .filter(|a| !a.starts_with(&bin_dir_str) && a.starts_with(&src_dir_str))
            {
                inputs.insert(PathBuf::from(arg));
            }
            if let Some(required_files) = test.required_files() {
                for p in required_files
                    .into_iter()
                    .filter(|p| !p.starts_with(bin_dir))
                {
                    inputs.insert(p);
                }
            }
        }
        info!(tests = self.ctest.tests.len(), inputs = inputs.len());
        Ok(inputs)
    }
}
