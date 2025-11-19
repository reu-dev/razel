#![allow(clippy::assigning_clones)] // makes code less readable

#[cfg(feature = "full")]
pub use crate::razel::*;
#[cfg(feature = "full")]
pub use file::*;
#[cfg(feature = "full")]
pub use parse_batch::*;
pub use rules::*;
#[cfg(feature = "full")]
pub use sandbox::*;
#[cfg(feature = "full")]
pub use scheduler::*;
#[cfg(feature = "full")]
pub use utils::*;

#[cfg(feature = "full")]
mod bazel_remote_exec;
#[cfg(feature = "full")]
pub mod cli;
pub mod config;
#[cfg(feature = "full")]
mod file;
#[cfg(feature = "full")]
mod parse_batch;
#[cfg(feature = "full")]
mod razel;
mod rules;
#[cfg(feature = "full")]
mod sandbox;
#[cfg(feature = "full")]
mod scheduler;
pub mod targets_builder;

#[cfg(feature = "full")]
pub mod cache {
    pub use cache::*;
    pub use digest::*;
    pub use local_cache::*;
    pub use remote_cache::*;

    #[allow(clippy::module_inception)]
    mod cache;
    mod digest;
    mod local_cache;
    mod remote_cache;
}

#[cfg(feature = "full")]
pub mod executors {
    pub use custom_command::*;
    pub use execution_result::*;
    pub use executor::*;
    pub use task::*;
    pub use task_http_remote_exec::*;
    pub use wasi::*;

    mod custom_command;
    mod execution_result;
    mod executor;
    mod task;
    mod task_csv;
    mod task_http;
    mod task_http_remote_exec;
    mod task_tools;
    mod wasi;
}

#[cfg(feature = "full")]
pub mod metadata {
    pub use log_file::*;
    pub use measurements::*;
    pub use profile::*;
    pub use report::*;

    mod graphs;
    mod log_file;
    mod measurements;
    mod profile;
    mod report;
}

#[cfg(feature = "full")]
pub mod utils {
    pub mod test_utils;
    pub use directories::*;
    pub use file_permissions::*;
    pub use hardlink::*;
    pub use helpers::*;
    pub use resources::*;
    pub use symlink::*;

    mod directories;
    mod file_permissions;
    mod hardlink;
    mod helpers;
    #[cfg_attr(target_os = "linux", path = "resources_linux.rs")]
    #[cfg_attr(not(target_os = "linux"), path = "resources_unimplemented.rs")]
    mod resources;
    mod symlink;
    pub mod tui;
}

pub mod types {
    pub use dependency_graph::*;
    pub use razel_jsonl::*;
    pub use result::*;
    pub use tags::*;
    pub use target::*;
    pub use tasks::*;

    mod dependency_graph;
    mod razel_jsonl;
    mod result;
    mod tags;
    mod target;
    mod tasks;
}
