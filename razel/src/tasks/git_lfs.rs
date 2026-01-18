use crate::cmake_file_api::CMakeFileApi;
use crate::ctest::CTest;
use crate::git_lfs::pull_paths;
use crate::types::{GitLfsPullCmakeDepsTask, GitLfsPullCtestDepsTask, GitLfsPullTask};
use anyhow::Result;
use tokio::task::spawn_blocking;

impl GitLfsPullTask {
    pub async fn exec(&self) -> Result<()> {
        pull_paths(self.paths.clone()).await
    }
}

impl GitLfsPullCmakeDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let cmake_binary_dir = self.cmake_binary_dir.clone();
        let cmake_build_type = self.cmake_build_type.clone();
        let inputs = spawn_blocking(move || {
            CMakeFileApi::read(&cmake_binary_dir)?.collect_input_files(&cmake_build_type)
        })
        .await??;
        pull_paths(inputs).await
    }
}

impl GitLfsPullCtestDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let ctest_dir = self.cmake_binary_dir.clone();
        let cmake_build_type = self.cmake_build_type.clone();
        let inputs = spawn_blocking(move || {
            CTest::read(&ctest_dir, &cmake_build_type)?.collect_input_files()
        })
        .await??;
        pull_paths(inputs).await
    }
}
