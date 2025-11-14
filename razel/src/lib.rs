#![allow(clippy::assigning_clones)] // makes code less readable

pub use crate::razel::*;
pub use cli::*;
pub use command::*;
pub use file::*;
pub use parse_batch::*;
pub use razel_jsonl::*;
pub use rules::*;
pub use sandbox::*;
pub use scheduler::*;
pub use utils::*;

mod bazel_remote_exec;
mod cli;
mod command;
pub mod config;
mod file;
mod parse_batch;
mod razel;
mod razel_jsonl;
mod rules;
mod sandbox;
mod scheduler;
pub mod targets_builder;

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

pub mod executors {
    pub use custom_command::*;
    pub use execution_result::*;
    pub use executor::*;
    pub use http_remote::*;
    pub use task::*;
    pub use wasi::*;

    mod custom_command;
    mod execution_result;
    mod executor;
    mod http_remote;
    mod task;
    mod wasi;
}

pub mod metadata {
    pub use graphs::*;
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

pub mod utils {
    pub mod test_utils;

    pub use arena::*;
    pub use file_permissions::*;
    pub use hardlink::*;
    pub use helpers::*;
    pub use resources::*;
    pub use symlink::*;

    mod arena;
    mod file_permissions;
    mod hardlink;
    mod helpers;
    #[cfg_attr(target_os = "linux", path = "resources_linux.rs")]
    #[cfg_attr(not(target_os = "linux"), path = "resources_unimplemented.rs")]
    mod resources;
    mod symlink;
    pub mod tui;
}

pub mod tasks {
    pub use self::csv::*;
    pub use http::*;
    pub use tools::*;

    mod csv;
    mod http;
    mod tools;
}

pub mod types {
    pub use dependency_graph::*;
    pub use result::*;
    pub use tags::*;
    pub use target::*;

    mod dependency_graph;
    mod result;
    mod tags;
    mod target;
}
