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
        mod protobuf {
            include!("bazel_remote_exec/gen/google.protobuf.rs");
        }

        pub mod rpc {
            include!("bazel_remote_exec/gen/google.rpc.rs");
        }

        mod longrunning {
            include!("bazel_remote_exec/gen/google.longrunning.rs");
        }

        #[cfg(not(doctest))]
        mod api {
            include!("bazel_remote_exec/gen/google.api.rs");
        }
    }

    mod build {
        pub mod bazel {
            mod semver {
                include!("bazel_remote_exec/gen/build.bazel.semver.rs");
            }

            pub mod remote {
                pub mod execution {
                    pub mod v2 {
                        include!("bazel_remote_exec/gen/build.bazel.remote.execution.v2.rs");
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

    mod custom_command;
    mod executor;
    mod task;
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
