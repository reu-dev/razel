use crate::webui_types::{JobStatus, Stats};
use leptos::prelude::*;

#[component]
pub fn TopbarStats(stats: RwSignal<Stats>) -> impl IntoView {
    let jobs_running = Signal::derive(move || {
        stats
            .get()
            .running_jobs
            .iter()
            .filter(|j| j.status == JobStatus::Running)
            .count()
    });
    let jobs_pending = Signal::derive(move || {
        stats
            .get()
            .running_jobs
            .iter()
            .filter(|j| j.status == JobStatus::Pending)
            .count()
    });
    let cpu_load = Signal::derive(move || {
        let s = stats.get();
        let used: f32 = s
            .nodes
            .iter()
            .filter_map(|(_, ns)| ns.as_ref())
            .map(|ns| ns.cpu_slots)
            .sum();
        let total: f32 = s.nodes.iter().map(|(n, _)| n.max_cpu_slots as f32).sum();
        if total == 0.0 {
            0.0
        } else {
            used / total * 100.0
        }
    });

    view! {
        <div class="topbar__stats">
            <div class="stat">
                <span class="stat__label">"Jobs"<br/>"running"</span>
                <span class="stat__value">{jobs_running}</span>
            </div>
            <div class="stat">
                <span class="stat__label">"Jobs"<br/>"pending"</span>
                <span class="stat__value">{jobs_pending}</span>
            </div>
            <div class="stat">
                <span class="stat__label">"slot"<br/>"load"</span>
                <span class="stat__value">{move || format!("{:.0} %", cpu_load.get())}</span>
            </div>
        </div>
    }
}
