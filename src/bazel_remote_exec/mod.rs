#![allow(clippy::all, dead_code)]

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
