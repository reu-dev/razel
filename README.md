# Razel

![Rust](https://img.shields.io/badge/language-rust-blue.svg)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/reu-dev/razel/blob/main/LICENSE.md)
[![CI](https://github.com/reu-dev/razel/actions/workflows/ci.yml/badge.svg)](https://github.com/reu-dev/razel/actions/workflows/ci.yml)

[![Deno module](https://shield.deno.dev/x/razel)](https://deno.land/x/razel)
[![Python module](https://img.shields.io/pypi/v/razel.svg)](https://pypi.org/pypi/razel)
[![Rust crate](https://img.shields.io/crates/v/razel.svg)](https://crates.io/crates/razel)

A command executor with caching. It is:

* Fast: caching avoids repeated execution of commands which haven't changed
* Reliable: commands are executed in a sandbox to detect missing dependencies
* Easy to use: commands are specified using a high-level TypeScript or Python API and convenience functions/tasks are
  built-in
* Made for: data processing pipelines with executables working on files and many dependencies between those

Razel is not the best choice for building software, especially there's no built-in support for compiler setup and header
dependencies.

## Getting Started

The native input format for Razel is a `razel.jsonl` file, see the example [test/razel.jsonl](test/razel.jsonl).
It can be run with `razel exec -f test/razel.jsonl`.

The preferred way is to use one of the high-level APIs. Both allow specifying the commands in an object-oriented style
and provide a `run()` function which creates the `razel.jsonl` file, downloads the native `razel` binary
and uses it to execute the commands.

Paths of inputs files are relative to the workspace (directory of `razel.jsonl`). Output files are created in `<cwd>/razel-out`.

### TypeScript API

Install [Deno](https://deno.land/) to use the [TypeScript API](include/deno/razel.ts).
Run the [example Deno script](test/deno.ts):

```bash
deno run -A --check test/deno.ts -- -v
```

### Python API

The [Python API](include/python/razel.py) requires Python >= 3.8.
Install the package and run the [example Python script](test/python.py):

```bash
pip install --upgrade razel
python test/python.py -v
```

### Batch file (experimental)

In addition to `razel.jsonl`, Razel can directly execute a batch file containing commands.
Input and output files need to be specified, which is WIP.

Execute the example [test/batch.sh](test/batch.sh) with Razel:

```bash
razel exec -f test/batch.sh
```

### Running in Docker/Podman container

The workspace directory can be mounted into a container:
```bash
podman run -t -v $PWD:$PWD -w $PWD denoland/deno deno run -A test/deno.ts
```

### Building Razel from source

Use [rustup](https://rustup.rs/) to install Rust. Install `protobuf-compiler`. Then run `cargo install razel`.


## Project Status

Razel is in active development and **not** ready for production. CLI and format of `razel.jsonl` will likely change.

| OS      | Status | Note                              |
|---------|--------|-----------------------------------|
| Linux   | ✓      | stable, main development platform |
| Mac     | ✓      | used and tested in CI             |
| Windows | (✓)    | tested in CI only                 |

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

## Features

### Measurements

Razel parses the stdout of executed commands to capture runtime measurements and writes them to `razel-out/razel-metadata/measurements.csv`.
Currently, the `<CTestMeasurement>` and `<DartMeasurement>` tags as used by [CTest/CDash](https://cmake.org/cmake/help/latest/command/ctest_test.html#additional-test-measurements) are supported:
```
<CTestMeasurement type="numeric/double" name="score">12.3</CTestMeasurement>
<CTestMeasurement type="text/string" name="result">ok</CTestMeasurement>
```
Supporting custom formats is planned.

### Tags

Tags can be set on commands. Any custom string can be used as tag, a colon should be used for grouping.
The tags are added to `razel-out/razel-metadata/execution_times.json`.
Using tags for filtering commands and creating reports is planned. 

Tags with `razel:` prefix are reserved and have special meaning:
- `razel:quiet`: don't be verbose if command succeeded
- `razel:verbose`: always show verbose output
- `razel:condition`: keep running and don't be verbose if command failed        
- `razel:no-cache`: always execute a command without caching
- `razel:no-remote-cache`: don't use remote cache
- `razel:no-sandbox`: disable sandbox and also cache - for commands with unspecified input/output files

### Conditional execution / Skipping command

Commands can be skipped based on the execution result of another command. Set the `razel:condition` tag on a command
and use that one as dependency for other commands. 

### Param/Response files

Commands with huge number of arguments might result in command lines which are too long to be executed by the OS.
Razel detects those cases and replaces the arguments with a response file. The filename starts with @.

## Acknowledgements

The idea to build fast and correct is based on [Bazel](https://bazel.build/). Razel uses data structures from
the [Bazel Remote Execution API](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)
for caching.
