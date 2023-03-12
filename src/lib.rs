pub use crate::razel::*;
pub use cli::*;
pub use command::*;
pub use file::*;
pub use measurements::*;
pub use parse_batch::*;
pub use rules::*;
pub use sandbox::*;
pub use scheduler::*;
pub use utils::*;

mod cli;
mod command;
pub mod config;
mod file;
mod measurements;
mod parse_batch;
mod parse_jsonl;
mod razel;
mod rules;
mod sandbox;
mod scheduler;

#[allow(clippy::all)]
pub mod bazel_remote_exec {
    pub use build::bazel::remote::execution::v2::*;

    mod google {
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }

        mod longrunning {
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
    pub use local_cache::*;

    #[allow(clippy::module_inception)]
    mod cache;
    mod local_cache;
}

pub mod executors {
    pub use custom_command::*;
    pub use executor::*;
    pub use task::*;
    pub use wasi::*;

    mod custom_command;
    mod executor;
    mod task;
    mod wasi;
}

pub mod utils {
    pub use arena::*;
    pub use file_permissions::*;
    pub use resources::*;
    pub use symlink::*;
    pub use tui::*;

    mod arena;
    mod file_permissions;
    #[cfg_attr(target_os = "linux", path = "resources_linux.rs")]
    #[cfg_attr(not(target_os = "linux"), path = "resources_unimplemented.rs")]
    mod resources;
    mod symlink;
    mod tui;
}

pub mod tasks {
    pub use self::csv::*;
    pub use http::*;
    pub use tools::*;

    mod csv;
    mod http;
    mod tools;
}
