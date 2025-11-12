# razel implementation details

### Terminology

* build file: `razel.jsonl`
* workspace: parent of `razel.jsonl`, used to resolve relative paths
* command/task (bazel: rule)
* input (bazel: source/data file)
* output (bazel: generated file)

### Dependency graph

dependencies:

* command depends on executable (special data file)
* command can depend on data files
* command can depend on output files of other command
* command can depend on other commands w/o using input files, e.g. deploy after running tests

### Build stages

1. create command and file instances from build file
    * check that output files are within workspace (all outputs should be put in `razel-out` dir)
    * check that a file is not used as output of multiple commands
2. create dependency graph
    * check for circular dependencies (TODO)
3. check inputs
    * data files must be readable
    * executables must be readable and executable
4. cleanup `razel-out`
    * remove all files which are no outputs, e.g. created by previous build with different build file
5. execute commands/tasks
    1. create `ActionDigest` on `Action` serialized to pb
    2. get `ActionResult` from local ac cache (read pb file)
        * if exists and all `ActionResult::output_files` exist in local cas cache => cache hit
    3. request `ActionResult` from remote ac cache
        * if received, query missing blobs from `ActionResult::output_files`
        * store `ActionResult` and received blobs in local cache
        * if blobs are all received => cache hit
    4. if cache miss: execute action and push to cache
    5. link output files from local cache to `razel-out`
