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
* [turbo-cache](https://github.com/TraceMachina/turbo-cache)
  - configure with instance_name e.g. `main`
  - call razel with `RAZEL_REMOTE_CACHE=grpc://localhost:50051/main`
