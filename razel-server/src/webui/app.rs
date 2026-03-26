use super::finished_jobs::FinishedJobs;
use super::nodes::Nodes;
use super::running_jobs::RunningJobs;
use super::stats_ws::WsStatus;
use super::topbar_stats::TopbarStats;
use crate::webui_types::Stats;
use leptos::prelude::*;
use leptos_meta::{MetaTags, Stylesheet, Title, provide_meta_context};
use leptos_router::{
    StaticSegment,
    components::{Route, Router, Routes},
};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <AutoReload options=options.clone() />
                <HydrationScripts options/>
                <MetaTags/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    view! {
        <Stylesheet id="leptos" href="/pkg/webui.css"/>
        <Title text="razel"/>
        <Router>
            <Routes fallback=|| "Page not found.".into_view()>
                <Route path=StaticSegment("") view=HomePage/>
            </Routes>
        </Router>
    }
}

#[component]
pub fn HomePage() -> impl IntoView {
    let stats = RwSignal::new(Stats::default());
    let ws_status = RwSignal::new(WsStatus::Connecting);

    #[cfg(feature = "hydrate")]
    leptos::task::spawn_local(super::stats_ws::ws_loop(stats, ws_status));

    view! {
        <header class="topbar">
            <div class="topbar__title">"razel"</div>
            <TopbarStats stats/>
            <div class="topbar__ws">
                <span class=move || format!("ws-dot ws-dot--{:?}", ws_status.get())></span>
                <span>{move || format!("{:?}", ws_status.get())}</span>
            </div>
        </header>

        <div class="content">
            <Nodes servers=Signal::derive(move || stats.get().nodes)/>
            <RunningJobs jobs=Signal::derive(move || stats.get().running_jobs)/>
            <FinishedJobs jobs=Signal::derive(move || stats.get().finished_jobs)/>
        </div>
    }
}
