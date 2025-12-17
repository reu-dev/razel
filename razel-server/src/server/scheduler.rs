use super::*;
use anyhow::Result;
use itertools::{chain, Itertools};
use quinn::SendStream;
use razel::remote_exec::{
    CreateJobRequest, CreateJobResponse, ExecuteTargetsRequest, Job, JobId, ServerToClientMsg,
};
use razel::types::{DependencyGraph, ExecutableType, File, FileId, Tag, Target, TargetId};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use tracing::{debug, info};
use uuid::Uuid;

pub struct Scheduler {
    jobs: Vec<JobData>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            jobs: Default::default(),
        }
    }
}

struct JobData {
    client: quinn::Connection,
    id: JobId,
    #[allow(dead_code)]
    job: Job,
    dep_graph: DependencyGraph,
    targets_for_input_file: HashMap<FileId, Vec<TargetId>>,
    requested_files: HashSet<FileId>,
    requested_files_for_ready_target: HashMap<TargetId, Vec<FileId>>,
    ready: VecDeque<TargetId>,
    succeeded: Vec<TargetId>,
    failed: Vec<TargetId>,
    keep_going: bool,
    worker: JobWorker,
}

impl JobData {
    pub fn new(client: quinn::Connection, id: JobId, job: Job, worker: JobWorker) -> Self {
        Self {
            client,
            id,
            job,
            dep_graph: Default::default(),
            targets_for_input_file: Default::default(),
            requested_files: Default::default(),
            requested_files_for_ready_target: Default::default(),
            ready: Default::default(),
            succeeded: vec![],
            failed: vec![],
            keep_going: false,
            worker,
        }
    }

    pub fn push_targets(
        &mut self,
        targets: Vec<Target>,
        files: Vec<File>,
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
        self.dep_graph.push_targets(targets, files);
        self.set_files_requested(requested_files);
        let ready = self.dep_graph.ready.clone();
        self.push_ready_from_dep_graph(ready);
    }

    fn set_files_requested(&mut self, files: Vec<FileId>) {
        self.requested_files.extend(&files);
    }

    pub async fn set_file_received(&mut self, file: FileId, cas_path: &PathBuf) {
        self.worker
            .link_input_file_into_ws_dir(cas_path, &self.dep_graph.files[file].path)
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
                self.ready.push_back(target_id);
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
                self.ready.push_back(target_id);
            } else {
                tracing::trace!(target_id, ?requested_files, "target is waiting for files");
                self.requested_files_for_ready_target
                    .insert(target_id, requested_files);
            }
        }
    }

    pub fn handle_execute_target_result(&mut self, msg: &ExecuteTargetResult) {
        let target_id = msg.target_id;
        tracing::debug!(job_id=?self.id, target_id, result=?msg.result, output_files=?msg.output_files.iter().map(|x| x.id).collect_vec());
        if msg.result.success() {
            self.succeeded.push(target_id);
            for file in &msg.output_files {
                assert!(file.digest.is_some());
                assert!(self.dep_graph.files[file.id].digest.is_none());
                self.dep_graph.files[file.id].digest = file.digest.clone();
            }
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
}

impl Server {
    pub fn handle_create_job_request(
        &mut self,
        client: ClientId,
        send: SendStream,
        request: CreateJobRequest,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let job_id = Uuid::now_v7();
        info!(?job_id, "CreateJobRequest");
        let worker = JobWorker::new(job_id, self.node.max_parallelism, &self.storage.path)?;
        scheduler.jobs.push(JobData::new(
            self.clients[&client].connection.clone(),
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

    #[instrument(skip_all)]
    pub fn handle_execute_targets_request(&mut self, msg: ExecuteTargetsRequest) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            return Ok(());
        };
        let mut requested_files: Vec<FileId> = Default::default();
        for file in &msg.files {
            if file.digest.is_some()
                && !self
                    .storage
                    .request_file_from_client(job.id, file, &job.client, &self.tx)
            {
                requested_files.push(file.id);
            }
        }
        job.push_targets(msg.targets, msg.files, requested_files);
        job.keep_going = msg.keep_going;
        debug!(job_id=?job.id, ready=job.dep_graph.ready.len(), waiting=job.dep_graph.waiting.len(), "ExecuteTargetsRequest");
        self.start_ready_targets();
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn handle_execute_target_result(&mut self, msg: ExecuteTargetResult) {
        let scheduler = self.scheduler.as_mut().unwrap();
        let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            return;
        };
        job.handle_execute_target_result(&msg);
        ServerToClientMsg::ExecuteTargetResult(msg)
            .spawn_send_uni(job.client.clone())
            .unwrap();
        self.start_ready_targets();
    }

    fn start_ready_targets(&mut self) {
        let scheduler = self.scheduler.as_mut().unwrap();
        for job in &mut scheduler.jobs {
            while let Some(target) = job.ready.pop_front().map(|x| &job.dep_graph.targets[x]) {
                job.worker
                    .push_target(target, &job.dep_graph.files, self.tx.clone());
            }
        }
    }

    fn webui_job_url(&self, job_id: &Uuid) -> String {
        let host = &self.node.host;
        let port = self.node.client_port.as_ref().unwrap();
        let job_id = job_id.as_simple();
        format!("http://{host}:{port}/job/{job_id}")
    }

    pub async fn handle_request_file_finished(&mut self, hash: DigestHash) {
        let cas_path = self.storage.cas_path(&hash);
        let scheduler = self.scheduler.as_mut().unwrap();
        for (job_id, file) in self.storage.handle_request_file_finished(hash) {
            let Some(job) = scheduler.jobs.iter_mut().find(|x| x.id == job_id) else {
                continue;
            };
            job.set_file_received(file, &cas_path).await;
        }
        self.start_ready_targets();
    }

    pub fn handle_request_file_failed(&mut self, hash: DigestHash, err: String) {
        todo!(
            "handle_request_file_failed: {err:?} files={:?}",
            self.storage.handle_request_file_failed(hash)
        );
    }
}
