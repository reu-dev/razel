use super::*;
use crate::job_database::{FinishedJob, JobDatabase};
use crate::webui_types::{FinishedJobStats, JobStatus, NodeStats, RunningJobStats};
use anyhow::{Error, Result, anyhow, ensure};
use itertools::{Itertools, chain};
use quinn::SendStream;
use razel::remote_exec::{
    CreateJobRequest, CreateJobResponse, ExecuteTargetsRequest, ServerToClientMsg,
};
use razel::types::*;
use std::collections::{HashSet, VecDeque};
use tracing::{debug, info, warn};
use uuid::Uuid;

type CpuSlots = f32;

pub struct Scheduler {
    node: String,
    max_cpu_slots: CpuSlots,
    jobs: Vec<JobData>,
    targets: usize,
    cpu_slots: CpuSlots,
    locks: HashSet<String>,
    job_db: JobDatabase,
}

impl Scheduler {
    pub fn new(max_cpu_slots: usize, node: String, job_db: JobDatabase) -> Self {
        // reserve one core for server/scheduler tasks
        let max_cpu_slots = max_cpu_slots as CpuSlots - 1.0;
        assert!(max_cpu_slots >= 1.0);
        Self {
            node,
            max_cpu_slots,
            cpu_slots: 0.0,
            jobs: Default::default(),
            targets: 0,
            locks: Default::default(),
            job_db,
        }
    }

    pub fn collect_stats(
        &self,
        node_stats: &mut NodeStats,
        running_jobs: &mut Vec<RunningJobStats>,
        finished_jobs: &mut Vec<FinishedJobStats>,
    ) {
        node_stats.storage_used += self.job_db.bytes;
        node_stats.jobs_running = self.jobs.iter().filter(|j| j.running != 0).count();
        node_stats.jobs_pending = self.jobs.len() - node_stats.jobs_running;
        node_stats.cpu_slots = self.cpu_slots;
        running_jobs.extend(self.jobs.iter().map(|j| j.collect_stats(&self.node)));
        finished_jobs.extend(self.job_db.jobs.iter().map(|j| j.stats.clone()));
    }

    pub fn handle_client_connection_lost(&mut self, client_id: ClientId) {
        let finished = self
            .jobs
            .extract_if(.., |job| {
                if job.client_id == client_id {
                    job.connection.take();
                    job.running == 0
                } else {
                    false
                }
            })
            .collect_vec();
        for job in finished {
            self.finish_job(job);
        }
    }

    fn finish_job(&mut self, job: JobData) {
        info!(client_id=job.client_id, job_id=?job.id, "job finished");
        let status = if job.is_finished() {
            if job.failed.is_empty() {
                JobStatus::Success
            } else {
                JobStatus::Failed
            }
        } else {
            JobStatus::Canceled
        };
        self.job_db.push(FinishedJob {
            stats: FinishedJobStats {
                id: job.id,
                job: job.job,
                node: self.node.clone(),
                status,
                succeeded: job.succeeded.len(),
                cached: job.cached,
                failed: job.failed.len(),
                skipped: job.dep_graph.skipped.len(),
                exec_cpu_secs: job.exec_cpu_secs,
                total_cpu_secs: job.total_cpu_secs,
                output_size_bytes: job.output_size_bytes,
            },
        });
    }
}

struct JobData {
    client_id: ClientId,
    connection: Option<quinn::Connection>,
    id: JobId,
    job: Job,
    dep_graph: DependencyGraph,
    targets_for_input_file: HashMap<FileId, Vec<TargetId>>,
    requested_files: HashSet<FileId>,
    requested_files_for_ready_target: HashMap<TargetId, Vec<FileId>>,
    ready: VecDeque<TargetId>,
    running: usize,
    cpu_slots: CpuSlots,
    succeeded: Vec<TargetId>,
    cached: usize,
    failed: Vec<TargetId>,
    keep_going: bool,
    worker: JobWorker,
    exec_cpu_secs: f64,
    total_cpu_secs: f64,
    output_size_bytes: u64,
}

impl JobData {
    pub fn new(
        client_id: ClientId,
        connection: quinn::Connection,
        id: JobId,
        job: Job,
        worker: JobWorker,
    ) -> Self {
        Self {
            client_id,
            connection: Some(connection),
            id,
            job,
            dep_graph: Default::default(),
            targets_for_input_file: Default::default(),
            requested_files: Default::default(),
            requested_files_for_ready_target: Default::default(),
            ready: Default::default(),
            running: 0,
            cpu_slots: 0.0,
            succeeded: vec![],
            cached: 0,
            failed: vec![],
            keep_going: false,
            worker,
            exec_cpu_secs: 0.0,
            total_cpu_secs: 0.0,
            output_size_bytes: 0,
        }
    }

    // TODO drop async
    pub async fn push_targets(
        &mut self,
        targets: Vec<Target>,
        files: Vec<File>,
        stored_inputs: Vec<FileId>,
        requested_files: Vec<FileId>,
    ) {
        for target in &targets {
            for file in chain!(&target.executables, &target.inputs).copied() {
                self.targets_for_input_file
                    .entry(file)
                    .or_default()
                    .push(target.id);
            }
        }
        for file in stored_inputs.into_iter().map(|id| &files[id]) {
            self.worker
                .link_input_file_into_ws_dir(file.digest.as_ref().unwrap(), &file.path)
                .await
                .unwrap();
        }
        self.dep_graph.push_targets(targets, files);
        self.requested_files.extend(&requested_files);
        let ready = self.dep_graph.ready.clone();
        self.push_ready_from_dep_graph(ready);
    }

    pub async fn set_file_received(&mut self, file: FileId, digest: &Digest) {
        self.worker
            .link_input_file_into_ws_dir(digest, &self.dep_graph.files[file].path)
            .await
            .unwrap();
        self.requested_files.remove(&file);
        for target_id in self.targets_for_input_file[&file].iter().copied() {
            let Some(requested_files) = self.requested_files_for_ready_target.get_mut(&target_id)
            else {
                continue;
            };
            requested_files.swap_remove(requested_files.iter().position(|x| *x == file).unwrap());
            if requested_files.is_empty() {
                self.requested_files_for_ready_target.remove(&target_id);
                tracing::trace!(target_id, "target is ready after requesting files");
                Self::push_ready(&self.dep_graph.targets[target_id], &mut self.ready);
            }
        }
    }

    fn push_ready_from_dep_graph(&mut self, targets: Vec<TargetId>) {
        for target_id in targets {
            let target = &self.dep_graph.targets[target_id];
            let requested_files = chain!(&target.executables, &target.inputs)
                .copied()
                .filter(|x| self.requested_files.contains(x))
                .collect_vec();
            if requested_files.is_empty() {
                tracing::trace!(target_id, "target is ready");
                Self::push_ready(target, &mut self.ready);
            } else {
                tracing::trace!(target_id, ?requested_files, "target is waiting for files");
                self.requested_files_for_ready_target
                    .insert(target_id, requested_files);
            }
        }
    }

    fn push_ready(target: &Target, ready: &mut VecDeque<TargetId>) {
        if target.cpus() > 1.0 || target.locks().next().is_some() {
            // schedule with higher priority
            ready.push_front(target.id);
        } else {
            ready.push_back(target.id);
        }
    }

    pub fn handle_execute_target_result(&mut self, msg: &ExecuteTargetResult) {
        let target_id = msg.target_id;
        tracing::debug!(job_id=?self.id, target_id, result=?msg.result, output_files=?msg.output_files.iter().map(|x| x.id).collect_vec());
        let cpus = self.dep_graph.targets[target_id].cpus() as f64;
        if let Some(d) = msg.result.exec_duration {
            self.exec_cpu_secs += d.as_secs_f64() * cpus;
        }
        if let Some(d) = msg.result.total_duration {
            self.total_cpu_secs += d.as_secs_f64() * cpus;
        }
        self.output_size_bytes += msg.result.output_size(&msg.output_files);
        if msg.result.success() {
            self.succeeded.push(target_id);
            if msg.result.cache_hit.is_some() {
                self.cached += 1;
            }
            self.set_output_file_digests(&msg.output_files);
            let ready = self.dep_graph.set_succeeded(target_id);
            self.push_ready_from_dep_graph(ready);
        } else {
            self.dep_graph.set_failed(target_id);
            if !self.dep_graph.targets[target_id]
                .tags
                .contains(&Tag::Condition)
            {
                self.failed.push(target_id);
            }
        }
    }

    fn set_output_file_digests(&mut self, files: &Vec<File>) {
        for file in files {
            assert!(file.digest.is_some());
            assert!(self.dep_graph.files[file.id].digest.is_none());
            self.dep_graph.files[file.id].digest = file.digest.clone();
        }
    }

    fn is_finished(&self) -> bool {
        self.dep_graph.waiting.is_empty() && self.ready.is_empty() && self.running == 0
    }

    fn collect_stats(&self, node: &str) -> RunningJobStats {
        let status = if self.is_finished() {
            if self.connection.is_none() {
                JobStatus::Canceled
            } else if self.failed.is_empty() {
                JobStatus::Success
            } else {
                JobStatus::Failed
            }
        } else if self.running > 0 {
            JobStatus::Running
        } else {
            JobStatus::Pending
        };
        RunningJobStats {
            id: self.id,
            job: self.job.clone(),
            node: node.to_string(),
            status,
            waiting: self.dep_graph.waiting.len(),
            ready: self.ready.len(),
            running: self.running,
            succeeded: self.succeeded.len(),
            cached: self.cached,
            failed: self.failed.len(),
            skipped: self.dep_graph.skipped.len(),
            exec_cpu_secs: self.exec_cpu_secs,
            total_cpu_secs: self.total_cpu_secs,
            output_size_bytes: self.output_size_bytes,
        }
    }
}

impl Server {
    pub fn handle_create_job_request(
        &mut self,
        client_id: ClientId,
        send: SendStream,
        request: CreateJobRequest,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let job_id = Uuid::now_v7();
        info!(client_id, ?job_id, "CreateJobRequest");
        let worker = JobWorker::new(job_id, &self.storage.path)?;
        scheduler.jobs.push(JobData::new(
            client_id,
            self.clients[&client_id].connection.clone(),
            job_id,
            request.job,
            worker,
        ));
        ServerToClientMsg::CreateJobResponse(CreateJobResponse {
            job_id,
            url: self.webui_job_url(&job_id),
        })
        .spawn_send(send)?;
        Ok(())
    }

    // TODO drop async
    #[instrument(skip_all)]
    pub async fn handle_execute_targets_request(
        &mut self,
        mut msg: ExecuteTargetsRequest,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            return Ok(());
        };
        if job.connection.is_none() {
            return Ok(());
        }
        let mut stored_inputs: Vec<FileId> = Default::default();
        let mut requested_inputs: Vec<FileId> = Default::default();
        for file in &mut msg.files {
            if file
                .executable
                .is_some_and(|x| x == ExecutableType::SystemExecutable)
            {
                file.path = file.path.file_name().unwrap().into();
                file.executable = Some(ExecutableType::ExecutableInWorkspace);
            }
            if file.path.is_absolute() || file.path.starts_with("..") {
                bail!("file has scary path: {file:?}");
            }
            if let Some(digest) = &file.digest {
                ensure!(
                    digest.is_valid(),
                    "invalid digest for file {:?}: {digest:?}",
                    file.path,
                );
                if self.storage.check_if_file_is_cached_or_request_from_client(
                    job.id,
                    file,
                    job.connection.as_ref().unwrap(),
                    &self.tx,
                ) {
                    stored_inputs.push(file.id);
                } else {
                    requested_inputs.push(file.id);
                }
            }
        }
        job.push_targets(msg.targets, msg.files, stored_inputs, requested_inputs)
            .await;
        job.keep_going = msg.keep_going;
        debug!(job_id=?job.id, ready=job.dep_graph.ready.len(), waiting=job.dep_graph.waiting.len(), "ExecuteTargetsRequest");
        self.start_ready_targets();
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn handle_execute_target_result(&mut self, msg: ExecuteTargetResult) {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            tracing::error!(job_id=?msg.job_id, target_id=msg.target_id, "Job not found in handle_execute_target_result()");
            return;
        };
        let target = &job.dep_graph.targets[msg.target_id];
        let cpu_slots = target.cpus();
        scheduler.targets -= 1;
        scheduler.cpu_slots -= cpu_slots;
        assert!(scheduler.cpu_slots > -0.01);
        for lock in target.locks() {
            scheduler.locks.remove(lock);
        }
        job.running -= 1;
        job.cpu_slots -= cpu_slots;
        assert!(job.cpu_slots > -0.01);
        job.handle_execute_target_result(&msg);
        if let Some(connection) = job.connection.as_ref() {
            ServerToClientMsg::ExecuteTargetResult(msg)
                .spawn_send_uni(connection.clone())
                .unwrap();
            self.start_ready_targets();
        } else if job.running == 0 {
            let pos = scheduler
                .jobs
                .iter()
                .position(|x| x.id == msg.job_id)
                .unwrap();
            let job = scheduler.jobs.remove(pos);
            scheduler.finish_job(job);
        }
    }

    fn start_ready_targets(&mut self) {
        let scheduler = self.scheduler.as_mut().unwrap();
        if scheduler.cpu_slots + 1.0 > scheduler.max_cpu_slots {
            return;
        }
        for job in &mut scheduler.jobs {
            let mut i = 0;
            while i < job.ready.len() {
                let target_id = job.ready[i];
                let target = &job.dep_graph.targets[target_id];
                if target.cpus() > scheduler.max_cpu_slots - scheduler.cpu_slots
                    || target.locks().any(|lock| scheduler.locks.contains(lock))
                {
                    i += 1;
                    continue;
                }
                job.ready.swap_remove_back(i);
                job.running += 1;
                job.cpu_slots += target.cpus();
                job.worker
                    .push_target(target, &job.dep_graph.files, self.tx.clone());
                scheduler.targets += 1;
                scheduler.cpu_slots += target.cpus();
                scheduler.locks.extend(target.locks().map(String::from));
                if scheduler.cpu_slots + 1.0 > scheduler.max_cpu_slots {
                    return;
                }
            }
        }
    }

    fn webui_job_url(&self, job_id: &Uuid) -> String {
        let host = &self.node.host;
        let port = self.node.client_port.as_ref().unwrap();
        let job_id = job_id.as_simple();
        format!("http://{host}:{port}/job/{job_id}")
    }

    pub async fn handle_request_file_finished(&mut self, digest: Digest) {
        let scheduler = self.scheduler.as_mut().unwrap();
        for (job_id, file) in self.storage.handle_request_file_finished(digest.clone()) {
            let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == job_id) else {
                continue;
            };
            job.set_file_received(file, &digest).await;
        }
        self.start_ready_targets();
    }

    pub fn handle_request_file_failed(&mut self, digest: Digest, error: Error) {
        let failed = self
            .storage
            .handle_request_file_failed(digest.hash, &self.tx);
        if failed.is_empty() {
            return; // retry already in flight
        }
        let error = format!("{error:?}");
        let scheduler = self.scheduler.as_mut().unwrap();
        for (job_id, _file_id) in failed {
            let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == job_id) else {
                continue;
            };
            if let Some(c) = job.connection.take() {
                close_connection_on_error(c, ConnectionCloseCode::JobError, anyhow!(error.clone()));
            }
        }
    }
}
