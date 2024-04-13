pub use crate::razel::*;
pub use cli::*;
pub use command::*;
pub use file::*;
pub use parse_batch::*;
pub use parse_jsonl::*;
pub use rules::*;
pub use sandbox::*;
pub use scheduler::*;
pub use types::*;
pub use utils::*;

mod cli;
mod command;
pub mod config;
mod file;
mod parse_batch;
mod parse_jsonl;
mod razel;
mod rules;
mod sandbox;
mod scheduler;
mod types;

#[allow(clippy::all)]
pub mod bazel_remote_exec {
    pub use build::bazel::remote::execution::v2::*;

    mod google {
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }

        pub mod longrunning {
            tonic::include_proto!("google.longrunning");
        }

        #[cfg(not(doctest))]
        mod api {
            tonic::include_proto!("google.api");
        }
    }

    mod build {
        pub mod bazel {
            mod semver {
                tonic::include_proto!("build.bazel.semver");
            }

            pub mod remote {
                pub mod execution {
                    pub mod v2 {
                        tonic::include_proto!("build.bazel.remote.execution.v2");
                    }
                }
            }
        }
    }
}

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
    pub use task::*;
    pub use wasi::*;

    mod custom_command;
    mod execution_result;
    mod executor;
    mod task;
    mod wasi;
}

pub mod metadata {
    pub use graphs::*;
    pub use log_file::*;
    pub use measurements::*;
    pub use profile::*;
    pub use report::*;
    pub use tags::*;

    mod graphs;
    mod log_file;
    mod measurements;
    mod profile;
    mod report;
    mod tags;
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
