pub use cli::*;
pub use command::*;
pub use file::*;
pub use parse_batch::*;
pub use rules::*;
pub use sandbox::*;
pub use scheduler::*;
pub use utils::*;

mod cache;
mod cli;
mod command;
pub mod config;
mod file;
mod hash;
mod parse_batch;
mod parse_jsonl;
mod rules;
mod sandbox;
mod scheduler;

pub mod bazel_remote_exec {
    mod google {
        mod protobuf {
            include!("bazel_remote_exec/gen/google.protobuf.rs");
        }
        pub(crate) mod rpc {
            include!("bazel_remote_exec/gen/google.rpc.rs");
        }
        mod longrunning {
            include!("bazel_remote_exec/gen/google.longrunning.rs");
        }
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

    pub use build::bazel::remote::execution::v2::*;
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

    mod arena;
}

pub mod tasks {
    pub use tools::*;

    pub use self::csv::*;

    mod csv;
    mod tools;
}
