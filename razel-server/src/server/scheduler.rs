use super::*;
use crate::auth::{AuthId, AuthState};
use crate::job_database::FinishedJob;
use crate::project::Project;
use crate::webui_types::{FinishedJobStats, JobStatus, NodeStats, RunningJobStats};
use anyhow::{Error, Result, anyhow, bail, ensure};
use itertools::{Itertools, chain};
use quinn::SendStream;
use razel::remote_exec::{
    CreateJobRequest, CreateJobResponse, ExecuteTargetsRequest, GitLabJobRequest,
    InteractiveJobRequest, JobId, JobRequestKind, ServerToClientMsg,
};
use razel::types::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
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
    auth: Arc<AuthState>,
    pub projects: HashMap<AuthId, Project>,
}

impl Scheduler {
    pub fn new(max_cpu_slots: usize, node: String, auth: Arc<AuthState>) -> Self {
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
            auth,
            projects: Default::default(),
        }
    }

    pub fn load_existing_projects(&mut self, storage_root: &Path) -> Result<()> {
        let jwt_root = storage_root.join("jwt");
        std::fs::create_dir_all(&jwt_root)?;
        for iss_entry in std::fs::read_dir(&jwt_root)?
            .flatten()
            .filter(|x| x.file_type().is_ok_and(|t| t.is_dir()))
        {
            let iss = iss_entry.file_name().to_string_lossy().into_owned();
            self.auth.push_gitlab_ci_instance(&iss);
            for proj_entry in std::fs::read_dir(iss_entry.path())?
                .flatten()
                .filter(|x| x.file_type().is_ok_and(|t| t.is_dir()))
            {
                let proj = proj_entry.file_name().to_string_lossy().into_owned();
                let id: AuthId = format!("jwt/{iss}/{proj}");
                let mut project = Project::new(storage_root, id.clone())?;
                project.read()?;
                self.projects.insert(id, project);
            }
        }
        let user_root = storage_root.join("user");
        std::fs::create_dir_all(&user_root)?;
        for user_entry in std::fs::read_dir(&user_root)?
            .flatten()
            .filter(|x| x.file_type().is_ok_and(|t| t.is_dir()))
        {
            let user = user_entry.file_name().to_string_lossy().into_owned();
            for hash_entry in std::fs::read_dir(user_entry.path())?
                .flatten()
                .filter(|x| x.file_type().is_ok_and(|t| t.is_dir()))
            {
                let hash = hash_entry.file_name().to_string_lossy().into_owned();
                let id: AuthId = format!("user/{user}/{hash}");
                let mut project = Project::new(storage_root, id.clone())?;
                project.read()?;
                self.projects.insert(id, project);
                self.auth.push_interactive_user(&user, &hash);
            }
        }
        Ok(())
    }

    pub fn collect_stats(
        &self,
        node_stats: &mut NodeStats,
        running_jobs: &mut Vec<RunningJobStats>,
        finished_jobs: &mut Vec<FinishedJobStats>,
    ) {
        node_stats.storage_used = self.projects.values().map(|p| p.bytes()).sum::<u64>();
        node_stats.jobs_running = self.jobs.iter().filter(|j| j.running != 0).count();
        node_stats.jobs_pending = self.jobs.len() - node_stats.jobs_running;
        node_stats.cpu_slots = self.cpu_slots;
        running_jobs.extend(self.jobs.iter().map(|j| j.collect_stats(&self.node)));
        for project in self.projects.values() {
            finished_jobs.extend(project.job_db.jobs.iter().map(|j| j.stats.clone()));
        }
        finished_jobs.sort_unstable_by_key(|x| std::cmp::Reverse(x.id));
    }

    pub fn handle_client_connection_lost(&mut self, client_id: ClientId) {
        let finished = self
            .jobs
            .extract_if(.., |job| {
                if job.client_id == client_id {
                    job.connection.take();
                    job.client_tx.take();
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
        info!(client_id=job.client_id, job_id=?job.job_id, "job finished");
        let project = self.projects.get_mut(&job.project_id).unwrap();
        let status = if job.is_finished() {
            if job.failed.is_empty() {
                JobStatus::Success
            } else {
                JobStatus::Failed
            }
        } else {
            JobStatus::Canceled
        };
        project.job_db.push(FinishedJob {
            stats: FinishedJobStats {
                id: job.job_id,
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
        job.worker.spawn_rm_job_dir();
    }
}

struct JobData {
    client_id: ClientId,
    connection: Option<quinn::Connection>,
    client_tx: Option<mpsc::UnboundedSender<ServerToClientMsg>>,
    project_id: AuthId,
    job_id: JobId,
    job: Job,
    #[allow(dead_code)]
    docker_pull_credentials: Option<(String, String)>,
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
        project_id: AuthId,
        job_id: JobId,
        job: Job,
        worker: JobWorker,
        docker_pull_credentials: Option<(String, String)>,
    ) -> Self {
        Self {
            client_id,
            connection: Some(connection),
            client_tx: None,
            project_id,
            job_id,
            job,
            docker_pull_credentials,
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
        tracing::debug!(job_id=?self.job_id, target_id, result=?msg.result, output_files=?msg.output_files.iter().map(|x| x.id).collect_vec());
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
            id: self.job_id,
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
    /// TODO drop async - needed to fetch JWKS (only the *first* time a issuer is seen)
    pub async fn handle_create_job_request(
        &mut self,
        client_id: ClientId,
        send: SendStream,
        request: CreateJobRequest,
    ) -> Result<()> {
        let CreateJobRequest {
            token,
            kind,
            junit_classname,
            default_tags,
            docker_image,
            docker_pull_credentials,
        } = request;
        let scheduler = self.scheduler.as_mut().unwrap();
        let (auth_id, kind, user, job_project) = match kind {
            JobRequestKind::GitLabCi(GitLabJobRequest { job_name, job_url }) => {
                let (auth_id, data) = scheduler
                    .auth
                    .verify_gitlab_ci_id_token(&token)
                    .await
                    .map_err(|e| anyhow!("auth failed: {e}"))?;
                (
                    auth_id,
                    JobKind::GitLabCi(GitLabCiJob {
                        instance: data.iss,
                        pipeline_id: data.pipeline_id,
                        job_id: data.job_id,
                        job_name,
                        job_url,
                    }),
                    data.user_login,
                    Some(data.project_path),
                )
            }
            JobRequestKind::Interactive(InteractiveJobRequest { user, project }) => {
                let auth_id = scheduler
                    .auth
                    .verify_interactive_user(&user, &token)
                    .map_err(|e| anyhow!("auth failed: {e}"))?;
                (auth_id, JobKind::Interactive, user, project)
            }
        };
        let job_id = Uuid::now_v7();
        info!(client_id, ?job_id, auth_id, "CreateJobRequest");
        let storage_path = match scheduler.projects.get(&auth_id) {
            Some(p) => p.path.clone(),
            _ => {
                let project = Project::new(&self.storage_root, auth_id.clone())?;
                let storage_path = project.path.clone();
                scheduler.projects.insert(auth_id.clone(), project);
                storage_path
            }
        };
        let worker = JobWorker::new(job_id, &storage_path)?;
        let job = Job {
            ts: chrono::Utc::now(),
            kind,
            user,
            project: job_project,
            junit_classname,
            default_tags,
            docker_image,
        };
        scheduler.jobs.push(JobData::new(
            client_id,
            self.clients[&client_id].connection.clone(),
            auth_id,
            job_id,
            job,
            worker,
            docker_pull_credentials,
        ));
        ServerToClientMsg::CreateJobResponse(CreateJobResponse {
            job_id,
            url: self.webui_job_url(&job_id),
        })
        .spawn_send(send)?;
        Ok(())
    }

    /// Handles an `ExecuteTargetsRequest` for a job and reuses the request's bi stream as the
    /// ordered reply channel for `ExecuteTargetResult` / `ExecuteStats`. All results travel
    /// back over a single QUIC stream, so the client receives them in submission order - which
    /// is what `DependencyGraph::set_succeeded` requires.
    ///
    /// Concurrent `ExecuteTargetsRequest`s for the same job are not supported: a second
    /// request replaces `client_tx`, dropping (and finishing) the previous reply stream, so
    /// any results still in flight for earlier targets are routed onto the new stream.
    ///
    /// TODO drop async
    #[instrument(skip_all)]
    pub async fn handle_execute_targets_request(
        &mut self,
        send: quinn::SendStream,
        mut msg: ExecuteTargetsRequest,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.job_id == msg.job_id) else {
            return Ok(());
        };
        let Some(connection) = job.connection.as_ref() else {
            return Ok(());
        };
        job.client_tx = Some(spawn_server_to_client_msg_sender(send));
        let project = scheduler.projects.get_mut(&job.project_id).unwrap();
        let mut stored_inputs: Vec<FileId> = Default::default();
        let mut requested_inputs: Vec<FileId> = Default::default();
        for file in &mut msg.files {
            match file.executable {
                Some(ExecutableType::SystemExecutable) => continue,
                Some(ExecutableType::ExecutableOutsideWorkspace) => {
                    bail!(
                        "ExecutableOutsideWorkspace is not supported for remote exec: {:?}",
                        file.path
                    );
                }
                _ => {}
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
                if project.check_if_file_is_cached_or_request_from_client(
                    job.job_id, file, connection, &self.tx,
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
        debug!(job_id=?job.job_id, ready=job.dep_graph.ready.len(), waiting=job.dep_graph.waiting.len(), "ExecuteTargetsRequest");
        self.start_ready_targets();
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn handle_execute_target_result(&mut self, msg: ExecuteTargetResult) {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.job_id == msg.job_id) else {
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
        if let Some(tx) = job.client_tx.as_ref() {
            tx.send(ServerToClientMsg::ExecuteTargetResult(msg)).ok();
            self.start_ready_targets();
        } else if job.running == 0 {
            let pos = scheduler
                .jobs
                .iter()
                .position(|x| x.job_id == msg.job_id)
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

    pub async fn handle_request_file_finished(&mut self, project_id: &AuthId, digest: Digest) {
        let scheduler = self.scheduler.as_mut().unwrap();
        let project = scheduler.projects.get_mut(project_id).unwrap();
        for (job_id, file) in project.handle_request_file_finished(&digest) {
            let Some(job) = scheduler.jobs.iter_mut().find(|x| x.job_id == job_id) else {
                continue;
            };
            job.set_file_received(file, &digest).await;
        }
        self.start_ready_targets();
    }

    pub fn handle_request_file_failed(
        &mut self,
        project_id: &AuthId,
        digest: Digest,
        error: Error,
    ) {
        let scheduler = self.scheduler.as_mut().unwrap();
        let project = scheduler.projects.get_mut(project_id).unwrap();
        let failed = project.handle_request_file_failed(&digest.hash, &self.tx);
        if failed.is_empty() {
            return; // retry already in flight
        }
        let error = format!("{error:?}");
        for (job_id, _file_id) in failed {
            let Some(job) = scheduler.jobs.iter_mut().find(|x| x.job_id == job_id) else {
                continue;
            };
            if let Some(c) = job.connection.take() {
                job.client_tx.take();
                close_connection_on_error(c, ConnectionCloseCode::JobError, anyhow!(error.clone()));
            }
        }
    }
}

fn spawn_server_to_client_msg_sender(
    mut stream: SendStream,
) -> mpsc::UnboundedSender<ServerToClientMsg> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerToClientMsg>();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = msg.send(&mut stream).await {
                warn!("sending ServerToClientMsg failed: {e}");
                break;
            }
        }
        stream.finish().ok();
    });
    tx
}
