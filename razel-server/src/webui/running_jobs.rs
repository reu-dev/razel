use crate::webui::human_format::{format_bytes, format_secs};
use crate::webui_types::RunningJobStats;
use leptos::prelude::*;

#[component]
pub fn RunningJobs(jobs: Signal<Vec<RunningJobStats>>) -> impl IntoView {
    view! {
        <section>
            <h2>"Running Jobs"</h2>
            <table>
                <thead>
                    <tr>
                        <th>"ID"</th>
                        <th>"Project"</th>
                        <th>"Node"</th>
                        <th>"Status"</th>
                        <th class="num">"Waiting"</th>
                        <th class="num">"Ready"</th>
                        <th class="num">"Running"</th>
                        <th class="num"><abbr title="Succeeded">"Succ."</abbr></th>
                        <th class="num">"Failed"</th>
                        <th class="num">"Skipped"</th>
                        <th class="num">"Cache Hits"</th>
                        <th class="num"><abbr title="sum of exec_duration * cpus over all targets">"Exec CPU"</abbr></th>
                        <th class="num"><abbr title="sum of total_duration * cpus over all targets">"Total CPU"</abbr></th>
                        <th class="num"><abbr title="total size of all output files and stdout/stderr">"Output"</abbr></th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        jobs.get()
                            .into_iter()
                            .map(|j| {
                                let cache_hit = format!("{:.0} %", j.cache_hit_rate() * 100.0);
                                view! {
                                    <tr>
                                        <td>{j.id.as_simple().to_string()[..8].to_string()}</td>
                                        <td>{j.job.project}</td>
                                        <td>{j.node}</td>
                                        <td>{format!("{:?}", j.status)}</td>
                                        <td class="num">{j.waiting}</td>
                                        <td class="num">{j.ready}</td>
                                        <td class="num">{j.running}</td>
                                        <td class="num">{j.succeeded}</td>
                                        <td class="num">{j.failed}</td>
                                        <td class="num">{j.skipped}</td>
                                        <td class="num">{cache_hit}</td>
                                        <td class="num">{format_secs(j.exec_cpu_secs as u64)}</td>
                                        <td class="num">{format_secs(j.total_cpu_secs as u64)}</td>
                                        <td class="num">{format_bytes(j.output_size_bytes)}</td>
                                    </tr>
                                }
                            })
                            .collect_view()
                    }}
                </tbody>
            </table>
        </section>
    }
}
