use super::Razel;
use crate::cache::Cache;
use crate::remote_exec::{Client, ClientChannelMsg, CreateJobResponse};
use crate::{
    select_cache_dir, select_sandbox_dir, SchedulerExecStats, SchedulerStats, TmpDirSandbox,
};
use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::debug;
use url::Url;

impl Razel {
    pub async fn check_remote_exec_servers(&self, urls: Vec<Url>) -> Result<()> {
        let mut set = JoinSet::new();
        for url in urls {
            set.spawn(async move {
                match Client::new(vec![url.clone()]).await {
                    Ok(_) => {
                        println!("{url} ok");
                        true
                    }
                    Err(x) => {
                        println!("{url} {x:?}");
                        false
                    }
                }
            });
        }
        let failed = set.join_all().await.into_iter().filter(|x| !x).count();
        match failed {
            0 => Ok(()),
            1 => bail!("{failed} remote execution server is not available"),
            _ => bail!("{failed} remote execution servers are not available"),
        }
    }

    pub(crate) async fn run_remotely(
        &mut self,
        keep_going: bool,
        group_by_tag: &str,
        cache_dir: Option<PathBuf>,
        remote_exec: Vec<Url>,
    ) -> Result<SchedulerStats> {
        let preparation_start = Instant::now();
        let client = self.prepare_run_remotely(cache_dir, remote_exec).await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut interval = tokio::time::interval(self.tui.get_update_interval());
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let execution_start = Instant::now();
        client.spawn_exec(
            self.dep_graph.targets.clone(),
            self.dep_graph.files.clone(),
            keep_going,
            tx,
        );
        let mut remote_exec_finished = false;
        while !remote_exec_finished || self.scheduler.running() != 0 {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(ClientChannelMsg::Result(r)) => {
                            let output_files = vec![]; // TODO
                            let output_files_cached = false; // TODO
                            self.on_command_finished(r.target_id, &r.result, output_files, output_files_cached);
                        }
                        Some(ClientChannelMsg::Stats(s)) => {
                            self.running_remotely = s.running;
                            self.tui_dirty = true;
                        }
                        Some(ClientChannelMsg::Finished) | None => remote_exec_finished = true,
                    }
                },
                _ = interval.tick() => self.update_status(),
            }
        }
        client.close("done").await;
        self.remove_outputs_of_not_run_actions_from_out_dir();
        TmpDirSandbox::cleanup(self.sandbox_dir.as_ref().unwrap());
        self.push_logs_for_not_started_targets();
        let stats = SchedulerStats {
            exec: SchedulerExecStats {
                succeeded: self.succeeded.len(),
                failed: self.failed.len(),
                skipped: self.dep_graph.skipped.len(),
                not_run: self.dep_graph.waiting.len() + self.scheduler.ready(),
            },
            cache_hits: self.cache_hits,
            preparation_duration: execution_start.duration_since(preparation_start),
            execution_duration: execution_start.elapsed(),
        };
        self.tui.finished(&stats);
        self.write_metadata(group_by_tag)
            .context("Failed to write metadata")?;
        Ok(stats)
    }

    async fn prepare_run_remotely(
        &mut self,
        cache_dir: Option<PathBuf>,
        remote_exec: Vec<Url>,
    ) -> Result<Client> {
        let client_handle = tokio::spawn(async {
            let mut client = Client::new(remote_exec).await?;
            let response = client.create_job().await?;
            Result::<(Client, CreateJobResponse)>::Ok((client, response))
        });
        let builder = self.targets_builder.as_ref().unwrap();
        let output_directory = self.current_dir.join(&self.out_dir);
        debug!("current dir:       {:?}", self.current_dir);
        debug!("workspace dir:     {:?}", builder.workspace_dir);
        debug!("output directory:  {output_directory:?}");
        let cache_dir = match cache_dir {
            Some(x) => x,
            _ => select_cache_dir(&builder.workspace_dir)?,
        };
        debug!("cache directory:   {cache_dir:?}");
        let sandbox_dir = select_sandbox_dir(&cache_dir)?;
        let cache = Cache::new(cache_dir, self.out_dir.clone())?;
        debug!("sandbox directory: {sandbox_dir:?}");
        debug!("worker threads:    {}", self.worker_threads);
        TmpDirSandbox::cleanup(&sandbox_dir);
        self.cache = Some(cache);
        self.sandbox_dir = Some(sandbox_dir);
        self.create_dependency_graph();
        self.remove_unknown_or_excluded_files_from_out_dir(&self.out_dir)
            .ok();
        self.digest_input_files().await?;
        self.create_output_dirs()?;
        let (client, response) = client_handle.await??;
        debug!("remote executor:   {}", client.url.as_str());
        debug!("job link:          {}", response.url);
        Ok(client)
    }
}
