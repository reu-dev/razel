use crate::cmake_file_api::CMakeFileApi;
use crate::types::CmakeEnableApiTask;
use anyhow::Result;
use tokio::task::spawn_blocking;

impl CmakeEnableApiTask {
    pub async fn exec(&self) -> Result<()> {
        let cmake_binary_dir = self.cmake_binary_dir.clone();
        spawn_blocking(move || CMakeFileApi::write_query(&cmake_binary_dir)).await?
    }
}
