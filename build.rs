// build script to generate code from bazel remote execution protobuf files

fn main() {
    let files =
        vec!["src/bazel_remote_exec/proto/build/bazel/remote/execution/v2/remote_execution.proto"];
    for x in &files {
        println!("cargo:rerun-if-changed={}", x);
    }
    let config = prost_build::Config::new();
    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .disable_package_emission()
        .out_dir("src/bazel_remote_exec/gen")
        .compile_with_config(config, &files, &["src/bazel_remote_exec/proto"])
        .unwrap();
}
