# razel implementation details

### Terminology

TODO: sync with bazel?

* workspace
* build file
* command/task (bazel: rule)
* input: (bazel: source/data file)
* output: (bazel: generated file)

### Paths

* `razel-out`: create in cwd
* workspace dir: used to resolve relative paths
    * cwd?
    * or commands file dir? could be multiple?

* paths used to execute commands: should equal original command?
* paths shown to user for debugging, should be easy to reproduce

| Executor      | cwd                      | executable | data              | non-data inputs                 | outputs                   |
|---------------|--------------------------|------------|-------------------|---------------------------------|---------------------------|
| CustomCommand | command specific tmp dir | symlink    | rel path, symlink | `razel-out/`, rel path, symlink | `razel-out/`, copied back |
| Task          | -                        | -          | rel path          | `razel-out/`, rel path          | `razel-out/`              |

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
    * check for circular dependencies
3. check inputs
    * data files must be readable
    * executables must be readable and executable
4. cleanup `razel-out`
    * remove all files which are no outputs, e.g. created by previous build with different build file
5. execute commands/tasks

### Cache

cache key:

* command: executable
* task: razel version
* command/task: command line and input files

### Thread pool

options:

* [rayon](https://github.com/rayon-rs/rayon)
    * work stealing won't work for blocking I/O: https://github.com/rayon-rs/rayon/issues/779
