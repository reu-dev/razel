use crate::cmake_file_api::CMakeFileApi;
use crate::ctest::CTest;
use crate::git_lfs::pull_paths;
use crate::types::{GitLfsPullCmakeDepsTask, GitLfsPullCtestDepsTask, GitLfsPullTask};
use anyhow::Result;
use itertools::Itertools;
use tokio::task::spawn_blocking;

impl GitLfsPullTask {
    pub async fn exec(&self) -> Result<()> {
        pull_paths(&self.paths).await
    }
}

impl GitLfsPullCmakeDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let cmake_binary_dir = self.cmake_binary_dir.clone();
        let inputs =
            spawn_blocking(move || CMakeFileApi::read(&cmake_binary_dir)?.collect_input_files())
                .await??;
        pull_paths(&inputs.into_iter().collect_vec()).await
    }
}

impl GitLfsPullCtestDepsTask {
    pub async fn exec(&self) -> Result<()> {
        let ctest_dir = self.cmake_binary_dir.clone();
        let inputs =
            spawn_blocking(move || CTest::read(&ctest_dir)?.collect_input_files()).await??;
        pull_paths(&inputs.into_iter().collect_vec()).await
    }
}
