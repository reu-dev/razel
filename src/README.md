# razel implementation details

### Terminology

TODO: sync with bazel?

* workspace
* build file
* command/task (bazel: rule)
* input: (bazel: source/data file)
* output: (bazel: generated file)

### Dependency graph

dependencies:

* command depends on executable (special data file)
* command can depend on data files
* command can depend on output files of other command
* command can depend on other commands w/o using input files, e.g. deploy after running tests

### Build stages

1. create command and file instances from build file
    * check that output files are within workspace (all outputs should be put in `razel-bin` dir)
    * check that a file is not used as output of multiple commands
2. create dependency graph
    * check for circular dependencies
3. check inputs
    * data files must be readable
    * executables must be readable and executable
4. cleanup `razel-bin`
    * remove all files which are no outputs, e.g. created by previous build with different build file
5. execute commands/tasks

### Command executor Sandbox

paths used to execute commands:

| Executor      | cwd                      | executable | data              | non-data inputs                 | outputs                   |
|---------------|--------------------------|------------|-------------------|---------------------------------|---------------------------|
| CustomCommand | command specific tmp dir | symlink    | rel path, symlink | `razel-bin/`, rel path, symlink | `razel-bin/`, copied back |
| Task          | -                        | -          | rel path          | `razel-bin/`, rel path          | `razel-bin/`              |

### Cache

cache key:

* command: executable
* task: razel version
* command/task: command line and input files

### Thread pool

options:

* [rayon](https://github.com/rayon-rs/rayon)
    * work stealing won't work for blocking I/O: https://github.com/rayon-rs/rayon/issues/779
