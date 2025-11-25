#[cfg(feature = "full")]
fn build_bazel_remote_execution_protos() {
    let files =
        vec!["src/bazel_remote_exec/proto/build/bazel/remote/execution/v2/remote_execution.proto"];
    for x in &files {
        println!("cargo:rerun-if-changed={x}");
    }
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&files, &["src/bazel_remote_exec/proto"])
        .unwrap();
}

fn main() {
    #[cfg(feature = "full")]
    build_bazel_remote_execution_protos();
}
