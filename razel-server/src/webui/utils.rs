use crate::webui_types::{Job, JobKind};
use leptos::prelude::*;

pub fn auth_from_job(job: &Job) -> String {
    match &job.kind {
        JobKind::GitLabCi(g) => g.instance.clone(),
        JobKind::Interactive => job.user.clone(),
    }
}

pub fn linked_job_name_from_job_kind(kind: &JobKind) -> impl IntoView + use<> {
    match kind {
        JobKind::GitLabCi(g) => view! {
            <a href={g.job_url.clone()} target="_blank" rel="noopener">
                {g.job_name.clone()}
            </a>
        }
        .into_any(),
        JobKind::Interactive => ().into_any(),
    }
}
