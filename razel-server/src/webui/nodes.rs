use crate::webui::human_format::format_bytes;
use crate::webui_types::{Node, NodeStats};
use itertools::Itertools;
use leptos::prelude::*;

#[component]
pub fn Nodes(servers: Signal<Vec<(Node, Option<NodeStats>)>>) -> impl IntoView {
    view! {
        <section>
        <h2>"Nodes"</h2>
        <table>
            <thead>
                <tr>
                    <th>"Host"</th>
                    <th><abbr title="Physical Machine">"Phys. M."</abbr></th>
                    <th class="num">"CPU Slots"</th>
                    <th>"Tags"</th>
                    <th class="num">"Max Storage"</th>
                    <th>"Status"</th>
                    <th class="num">"Server Conn"</th>
                    <th class="num">"Client Conn"</th>
                    <th class="num">"Storage Used"</th>
                    <th class="num">"Jobs Running"</th>
                    <th class="num">"Jobs Pending"</th>
                    <th class="num">"Slot Load"</th>
                </tr>
            </thead>
            <tbody>
                {move || {
                    servers
                        .get()
                        .into_iter()
                        .map(|(n, s)| {
                            let tags = n.tags.iter()
                                .map(|t| format!("{:?}", t))
                                .collect_vec()
                                .join(", ");
                            let storage_max = n.storage_max_size_gb
                                .map(|s| format_bytes(s as u64 * 1_000_000_000))
                                .unwrap_or_default();
                            let stats_cells = match s {
                                Some(s) => {
                                    let status = format!("{:?}", s.status);
                                    let storage_used = format_bytes(s.storage_used);
                                    let load = format!("{:.0} %",
                                        s.cpu_slots / n.max_cpu_slots as f32 * 100.0);
                                    view! {
                                        <td>{status}</td>
                                        <td class="num">{s.server_connections}</td>
                                        <td class="num">{s.client_connections}</td>
                                        <td class="num">{storage_used}</td>
                                        <td class="num">{s.jobs_running}</td>
                                        <td class="num">{s.jobs_pending}</td>
                                        <td class="num">{load}</td>
                                    }.into_any()
                                }
                                None => view! { <td colspan="8"></td> }.into_any(),
                            };
                            view! {
                                <tr>
                                    <td>{n.host}</td>
                                    <td>{n.physical_machine}</td>
                                    <td class="num">{n.max_cpu_slots}</td>
                                    <td>{tags}</td>
                                    <td class="num">{storage_max}</td>
                                    {stats_cells}
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
