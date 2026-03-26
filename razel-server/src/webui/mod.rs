pub mod app;
mod finished_jobs;
mod human_format;
mod nodes;
mod running_jobs;
mod stats_ws;
mod topbar_stats;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    use app::*;
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}
