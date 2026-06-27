use crate::webui_types::{Job, JobKind};
use leptos::prelude::*;

pub fn auth_from_job(job: &Job) -> String {
    match &job.kind {
        JobKind::GitLabCi(g) => g.instance.clone(),
        JobKind::Interactive => job.user.clone(),
    }
}

pub fn linked_job_name(kind: &JobKind, junit_classname: Option<&str>) -> impl IntoView + use<> {
    let suffix = junit_classname.map(|c| format!(" {c}")).unwrap_or_default();
    match kind {
        JobKind::GitLabCi(g) => view! {
            <a href={g.job_url.clone()} target="_blank" rel="noopener">
                {g.job_name.clone()}
            </a>
            {suffix}
        }
        .into_any(),
        JobKind::Interactive => suffix.into_any(),
    }
}
