# razel cache

command execution with cache:

1. if action is not completely cached: execute action and push to cache
2. symlink output files from local cache to `out_dir`

read cache for `Action`:

1. create `ActionDigest` on `Action` serialized to pb
2. get `ActionResult` from local ac cache (read pb file)
    * if exists and all `ActionResult::output_files` exist in local cas cache => cache hit
3. request `ActionResult` from remote ac cache
    * if received, query missing blobs from `ActionResult::output_files`
    * store `ActionResult` and received blobs in local cache

## Remote Cache implementations

* [bazel-remote-cache](https://github.com/buchgr/bazel-remote)
    - run with `docker run -p 9092:9092 buchgr/bazel-remote-cache --max_size 1`
    - call razel with `RAZEL_REMOTE_CACHE=grpc://localhost:9092`
* [nativelink](https://github.com/TraceMachina/nativelink)
    - run with instance_name `main` on port 50051:
        ```
        mkdir -p nativelink-config
        curl https://raw.githubusercontent.com/TraceMachina/nativelink/main/nativelink-config/examples/basic_cas.json --output nativelink-config/basic_cas.json
        podman run -p 50051:50051 -v $PWD/nativelink-config:/nativelink-config:ro ghcr.io/tracemachina/nativelink:v0.2.0 /nativelink-config/basic_cas.json
        ```
    - call razel with `RAZEL_REMOTE_CACHE=grpc://localhost:50051/main`
