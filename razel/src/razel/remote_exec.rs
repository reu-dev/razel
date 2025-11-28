use super::Razel;
use crate::remote_exec::Client;
use crate::SchedulerStats;
use anyhow::Result;
use std::path::PathBuf;
use url::Url;

impl Razel {
    pub(crate) async fn run_remotely(
        &mut self,
        _keep_going: bool,
        _verbose: bool,
        _group_by_tag: &str,
        _cache_dir: Option<PathBuf>,
        remote_exec: Vec<Url>,
    ) -> Result<SchedulerStats> {
        let client = Client::new(remote_exec).await?;
        let job = client.create_job().await?;
        tracing::info!(job = ?job.id);
        todo!()
    }
}
