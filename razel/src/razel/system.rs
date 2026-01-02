use super::Razel;
use crate::cache::Cache;
use crate::select_cache_dir;
use anyhow::{bail, Result};
use std::path::PathBuf;

impl Razel {
    pub async fn check_remote_cache_servers(&self, urls: Vec<String>) -> Result<()> {
        let mut failed = 0;
        let workspace_dir = &self.targets_builder.as_ref().unwrap().workspace_dir;
        let cache_dir = select_cache_dir(workspace_dir)?;
        let mut cache = Cache::new(cache_dir, PathBuf::new())?;
        for url in urls.iter().filter(|x| !x.is_empty()) {
            match cache
                .connect_remote_cache(std::slice::from_ref(url), None)
                .await
            {
                Ok(true) => println!("{url} ok"),
                Ok(_) => {
                    println!("{url} failed");
                    failed += 1;
                }
                Err(x) => {
                    println!("{url} {x:?}");
                    failed += 1;
                }
            }
        }
        match failed {
            0 => Ok(()),
            1 => bail!("{failed} remote cache is not available"),
            _ => bail!("{failed} remote caches are not available"),
        }
    }
}
