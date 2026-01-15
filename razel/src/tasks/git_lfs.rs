use crate::cmake_file_api::CMakeFileApi;
use crate::ctest::CTest;
use crate::git_lfs::pull_files;
use crate::types::{GitLfsPullCmakeDepsTask, GitLfsPullCtestDepsTask};
use anyhow::Result;
use itertools::Itertools;
use tokio::task::spawn_blocking;

impl GitLfsPullCmakeDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let cmake_binary_dir = self.cmake_binary_dir.clone();
        let inputs =
            spawn_blocking(move || CMakeFileApi::read(&cmake_binary_dir)?.collect_input_files())
                .await??;
        pull_files(&inputs.into_iter().collect_vec()).await
    }
}

impl GitLfsPullCtestDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let ctest_dir = self.cmake_binary_dir.clone();
        let inputs =
            spawn_blocking(move || CTest::read(&ctest_dir)?.collect_input_files()).await??;
        pull_files(&inputs.into_iter().collect_vec()).await
    }
}
