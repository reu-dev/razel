use super::*;
use anyhow::Result;
use quinn::SendStream;
use razel::remote_exec::{
    CreateJobRequest, CreateJobResponse, ExecuteTargetsRequest, Job, JobId, ServerToClientMsg,
};
use razel::types::{DependencyGraph, Tag, TargetId};
use std::collections::VecDeque;
use tracing::info;
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

    pub fn handle_execute_targets_request(&mut self, msg: ExecuteTargetsRequest) -> Result<()> {
        let Some(job) = self.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            return Ok(());
        };
        job.dep_graph.push_targets(msg.targets, msg.files);
        job.keep_going = msg.keep_going;
        job.ready.extend(&job.dep_graph.ready);
        Ok(())
    }

    pub fn handle_execute_target_result(&mut self, msg: ExecuteTargetResult) {
        let Some(job) = self.jobs.iter_mut().find(|x| x.id == msg.job_id) else {
            return;
        };
        let target_id = msg.target_id;
        if msg.result.success() {
            job.succeeded.push(target_id);
            // TODO  self.set_output_file_digests(output_files, output_files_cached);
            let ready = job.dep_graph.set_succeeded(target_id);
            job.ready.extend(&ready);
        } else if job.dep_graph.targets[target_id]
            .tags
            .contains(&Tag::Condition)
        {
            job.dep_graph.set_failed(target_id);
        } else {
            job.failed.push(target_id);
        }
    }
}

struct JobData {
    id: JobId,
    #[allow(dead_code)]
    job: Job,
    dep_graph: DependencyGraph,
    keep_going: bool,
    ready: VecDeque<TargetId>,
    succeeded: Vec<TargetId>,
    failed: Vec<TargetId>,
    worker: JobWorker,
}

impl Server {
    pub fn handle_create_job_request(
        &mut self,
        send: SendStream,
        request: CreateJobRequest,
    ) -> Result<()> {
        let scheduler = self.scheduler.as_mut().unwrap();
        let job_id = Uuid::now_v7();
        info!(?job_id, "CreateJobRequest");
        let worker = JobWorker::new(
            job_id,
            self.node.max_parallelism,
            &self.storage.first().unwrap().path,
        )?;
        scheduler.jobs.push(JobData {
            id: job_id,
            job: request.job,
            dep_graph: Default::default(),
            keep_going: false,
            ready: Default::default(),
            succeeded: vec![],
            failed: vec![],
            worker,
        });
        ServerToClientMsg::CreateJobResponse(CreateJobResponse {
            job_id,
            url: self.webui_job_url(&job_id),
        })
        .spawn_send(send)?;
        Ok(())
    }

    pub fn handle_execute_targets_request(
        &mut self,
        _send: SendStream,
        request: ExecuteTargetsRequest,
    ) -> Result<()> {
        self.scheduler
            .as_mut()
            .unwrap()
            .handle_execute_targets_request(request)?;
        self.start_ready_targets();
        Ok(())
    }

    pub fn handle_execute_target_result(&mut self, msg: ExecuteTargetResult) {
        self.scheduler
            .as_mut()
            .unwrap()
            .handle_execute_target_result(msg);
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
}
