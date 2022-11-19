# Razel

A command executor with caching. It is:

* Fast: caching avoids repeated execution of commands which haven't changed
* Reliable: commands are executed in a sandbox to detect missing dependencies
* Easy to use: commands are specified using a high-level TypeScript API and convenience functions/tasks are built-in
* Made for: data processing pipelines with executables working on files and many dependencies between those

Razel is not the best choice for building software, especially there's no built-in support for compiler setup and header
dependencies.

[![CI](https://github.com/reu-dev/razel/actions/workflows/ci.yml/badge.svg)](https://github.com/reu-dev/razel/actions/workflows/ci.yml)

## Getting Started

Use [rustup](https://rustup.rs/) to install Rust. Clone and build Razel:

```bash
git clone https://github.com/reu-dev/razel.git
cd razel/
cargo install --locked --path .
```

### Example: TypeScript API

Install [Deno](https://deno.land/) to use the TypeScript API. [TypeScript example file](test/deno.ts)

```bash
# create razel.jsonl from test/deno.ts 
deno run --allow-write=. test/deno.ts

# execute commands from razel.jsonl
razel exec -f test/razel.jsonl
```

Instead of TypeScript, your favorite scripting language could be used to create a `razel.jsonl` file.

### Example: Batch file

Razel can directly execute a file containing commands. Input and output files need to be specified, which is WIP.
[Batch example file](test/batch.sh)

```bash
razel exec -f test/batch.sh
```

## Project Status

Razel is in active development and **not** ready for production. CLI and format of `razel.jsonl` will likely change.

| OS      | Status | Note                               |
|---------|--------|------------------------------------|
| Linux   | ✓      | stable, main development platform  |
| Mac     | (✓)    | tested in CI                       |
| Windows | ✘      | not yet tested, likely not working |

| Feature                                   | Status  | Note                                                       |
|-------------------------------------------|---------|------------------------------------------------------------|
| command execution in sandbox              | ✓       |                                                            |
| multithreaded execution                   | ✓       |                                                            |
| local caching                             | ✓       |                                                            |
| remote caching                            | ✘       | WIP                                                        |
| remote execution                          | ✘       | TODO                                                       |
| OOM handling: retry with less concurrency | ✓ Linux | requires `sudo cgcreate -a $USER -t $USER -g memory:razel` |

## Why not ...?

* [Bazel](https://bazel.build/) is a multi-language build tool. However, for the use case Razel targets, there are some
  issues:
    * additional launcher script required for some simple tasks
        * using stdout of action as input for another action
        * parsing measurements from stdout of action
        * CTest features like FAIL_REGULAR_EXPRESSION, WILL_FAIL
    * difficult to get command lines for debugging
    * no automatic disk usage limit/cleanup for local cache - all temp output needs to fit on disk
    * no native support for response files
    * resources cannot be reserved to run real-time critical tests
    * content of bazel-bin/out directories is not defined (contains mixture of current build and cache)
* [CTest](https://cmake.org/cmake/help/latest/manual/ctest.1.html) is nice for building C/C++ code and CTest can be used
  for testing,
  but it does not support caching and managing dependencies between tests is difficult.

## Acknowledgements

The idea to build fast and correct is based on [Bazel](https://bazel.build/). Razel uses data structures from
the [Bazel Remote Execution API](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)
for caching.

## Features

### Measurements

Razel parses the stdout of executed commands to capture runtime measurements and writes them to `razel-out/measurements.csv`.
Currently, the `<CTestMeasurement>` and `<DartMeasurement>` tags as used by [CTest/CDash](https://cmake.org/cmake/help/latest/command/ctest_test.html#additional-test-measurements) are supported:
```
<CTestMeasurement type="numeric/double" name="score">12.3</CTestMeasurement>
<CTestMeasurement type="text/string" name="result">ok</CTestMeasurement>
```
Supporting custom formats is planned.

## Goals / Ideas

* built-in convenience functions/tasks
    * parse measurement from stdout of processes, aggregate them for reports
    * optionally replace outputs on errors instead of bailing out
    * concat output files, e.g. jsonl, csv
    * summary grouped by custom process labels to provide better overview of errors
    * JUnit test output, e.g. for GitLab CI
    * simple query language to filter processes
* process scheduling and caching depending on resources
    * automatic disk cleanup locally and for cache
    * measure/predict task execution time and output size
    * consider disk usage, RAM, network speed
    * schedule network bound tasks in addition to CPU bound ones
    * allow limiting parallel instances of external tools
* transparent remote execution
* data/results down/upload to storage, e.g. Git LFS, MinIO
    * local access to important outputs of remotely executed tasks
* support wasi
    * no need to compile tools for Linux, Windows, Apple x64, Apple M1, ...
    * bit-exact output on all platforms?
    * wasm provides sandbox
    * integrate wasi executor to avoid dependency on additional tool
* integrate building source code with CMake (low prio)
    * specify source like `Bazel new_local_repository(), new_git_repository()`
    * run CMake configure, parse commands to build targets and execute those
        * CMake can create [JSON Compilation Database](https://clang.llvm.org/docs/JSONCompilationDatabase.html), but
          that does not include link commands
        * [CMake File API](https://cmake.org/cmake/help/latest/manual/cmake-file-api.7.html) contains raw data, but not
          complete command lines
        * `cmake -DCMAKE_RULE_MESSAGES:BOOL=OFF -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON . && make --no-print-directory` lists
          all commands but is difficult to parse
        * `ninja -t commands` looks ok
* execute CTest files (low prio)
    * CTest allows specifying input files, but not output files
* ensure correctness
    * UB check for modified executables: run first commands multiple times to verify that the outputs are consistent
    * avoid cache poisoning when disk full: missing fwrite/fclose checks in an executable would break cache
* tools for users to debug errors
    * show command lines ready for c&p into debugger
    * UB check of chain until first failing command
    * if source code of executables available: rebuild with debug and sanitizers and run with those executables
    * for command with long list of inputs: bisect inputs to create minimal reproducible example
