use super::Razel;
use crate::cache::Cache;
use crate::config::select_cache_dir;
use anyhow::{bail, Result};

impl Razel {
    pub async fn check_remote_cache(&self, urls: Vec<String>) -> Result<()> {
        let mut failed = 0;
        let cache_dir = select_cache_dir(&self.workspace_dir)?;
        let mut cache = Cache::new(cache_dir, self.out_dir.clone())?;
        for url in urls.iter().filter(|x| !x.is_empty()) {
            match cache.connect_remote_cache(&[url.clone()], None).await {
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
