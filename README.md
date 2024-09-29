# Razel

![Rust](https://img.shields.io/badge/language-rust-blue.svg)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/reu-dev/razel/blob/main/LICENSE.md)
[![CI](https://github.com/reu-dev/razel/actions/workflows/ci.yml/badge.svg)](https://github.com/reu-dev/razel/actions/workflows/ci.yml)

[![Deno module](https://shield.deno.dev/x/razel)](https://deno.land/x/razel)
[![Python module](https://img.shields.io/pypi/v/razel.svg)](https://pypi.org/pypi/razel)
[![Rust crate](https://img.shields.io/crates/v/razel.svg)](https://crates.io/crates/razel)

A command executor with caching. It is:

* Fast: commands are executed multithreaded and local caching speeds up trial and error development (avoids repeated
  execution of commands which have been processed before)
* Scalable: optional remote caching allows sharing results between CI jobs
* Reliable: commands are executed in a sandbox to detect missing dependencies
* Easy to use: commands are specified using a high-level TypeScript or Python API and convenience functions/tasks are
  built-in
* Made for: data processing pipelines with executables working on files and many dependencies between those

Razel is not the best choice for building software, especially there's no built-in support for compiler setup and header
dependencies.

## Getting Started

The native input format for Razel is a `razel.jsonl` file, see the example [examples/razel.jsonl](examples/razel.jsonl).
It can be run with `razel exec -f examples/razel.jsonl`.

The preferred way is to use one of the high-level APIs. Both allow specifying the commands in an object-oriented style
and provide a `run()` function which creates the `razel.jsonl` file, downloads the native `razel` binary
and uses it to execute the commands.

Paths of inputs files are relative to the workspace (directory of `razel.jsonl`). Output files are created
in `<cwd>/razel-out`. Additional metadata is written to `<cwd>/razel-out/razel-metadata`.

### TypeScript API

Install [Deno](https://deno.land/) to use the [TypeScript API](apis/deno/razel.ts).
Run the [example Deno script](examples/deno.ts):

```bash
deno run -A --check examples/deno.ts -- -v
```

### Python API

The [Python API](apis/python/razel.py) requires Python >= 3.8.
Install the package and run the [example Python script](examples/python.py):

```bash
pip install --upgrade razel
python examples/python.py -v
```

### Batch file (experimental)

In addition to `razel.jsonl`, Razel can directly execute a batch file containing commands.
Input and output files need to be specified, which is WIP.

Execute the example [examples/batch.sh](examples/batch.sh) with Razel:

```bash
razel exec -f examples/batch.sh
```

### Running in Docker/Podman container

The workspace directory can be mounted into a container:

```bash
podman run -t -v $PWD:$PWD -w $PWD denoland/deno deno run -A examples/deno.ts
```

### Building Razel from source

Use [rustup](https://rustup.rs/) to install Rust. Install `protobuf-compiler`. Then run `cargo install --locked razel`.

## Project Status

Razel is in active development and used in production.

CLI and format of `razel.jsonl` will likely change, same for output in `razel-out/razel-metadata`.
While Linux is the main development platform, Razel is also tested on Mac and Windows.

## Features

### Measurements

Razel parses the stdout of executed commands to capture runtime measurements and writes them
to `razel-out/razel-metadata/log.json` and `razel-out/razel-metadata/measurements.csv`.
Currently, the `<CTestMeasurement>` and `<DartMeasurement>` tags as used
by [CTest/CDash](https://cmake.org/cmake/help/latest/command/ctest_test.html#additional-test-measurements) are
supported:

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
- `razel:timeout:<seconds>`: kill command after the specified number of seconds
- `razel:no-cache`: always execute a command without caching
- `razel:no-remote-cache`: don't use remote cache
- `razel:no-sandbox`: disable sandbox and also cache - for commands with unspecified input/output files

### Conditional execution / Skipping commands

Commands can be skipped based on the execution result of another command. Set the `razel:condition` tag on a command
and use that one as dependency for other commands.

### WebAssembly

Razel has a WebAssembly runtime integrated and can directly execute WASM modules
using [WebAssembly System Interface (WASI)](https://wasi.dev/).

WebAssembly is a perfect fit to create portable data processing pipelines with Razel.
Just a single WebAssembly module is needed to run - and create bit-exact output - on all platforms.
WebAssembly execution is slower than native binaries, but startup time might be faster (no process overhead).

### Param/Response files

Commands with huge number of arguments might result in command lines which are too long to be executed by the OS.
Razel detects those cases and replaces the arguments with a response file. The filename starts with @.

### Out of memory (OOM) handling

If a process is killed by the OS, the command and similar ones will be retried with less concurrency to reduce the
total memory usage. (Doesn't work in K8s because the whole pod is killed.)

### Sandbox

Commands are executed in a temporary directory which contains symlinks to the input files specific to one command.
This allows detecting unspecified dependencies which would break caching.

The sandbox is not meant for executing untrusted code.

### Local Caching

The local cache is enabled by default and stores information about previously executed commands and output files.
The output directory `razel-out` contains symlinks to files stored in the local cache.

Use `razel exec --info` to get the default cache directory and `--cache-dir` (env: `RAZEL_CACHE_DIR`) to move it.

### Remote Caching

Razel supports remote caching compatible to
[Bazel Remote Execution API](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto).
Remote execution is not yet implemented.

Use `--remote-cache` (env: `RAZEL_REMOTE_CACHE`) to specify a comma seperated list of remote cache URLs.
The first available one will be used.
Optionally `--remote-cache-threshold` (`REMOTE_CACHE_THRESHOLD`) can be set to only cache commands with
`outputSize / execTime < threshold [kilobyte / s]`. If your remote cache doesn't have unlimited storage capacity,
this can drastically speed up execution because quick commands with large output files will no longer be cached,
providing more storage for expensive commands.

The following remote cache implementations are tested with Razel:

* [bazel-remote-cache](https://github.com/buchgr/bazel-remote)
    - run with `podman run -p 9092:9092 buchgr/bazel-remote-cache --max_size 10`
    - call razel with `RAZEL_REMOTE_CACHE=grpc://localhost:9092`
* [nativelink](https://github.com/TraceMachina/nativelink)
    - run with instance_name `main` on port 50051:
        ```
        mkdir -p nativelink-config
        curl https://raw.githubusercontent.com/TraceMachina/nativelink/main/nativelink-config/examples/basic_cas.json --output nativelink-config/basic_cas.json
        podman run -p 50051:50051 -v $PWD/nativelink-config:/nativelink-config:ro ghcr.io/tracemachina/nativelink:v0.2.0 /nativelink-config/basic_cas.json
        ```
    - call razel with `RAZEL_REMOTE_CACHE=grpc://localhost:50051/main`

## Configuration

Use `razel exec -h` to list the configuration options for execution.
Some options can also be set as environment variables and those are loaded from `.env` files.

The following sources are used in order, overwriting previous values:

- `.env` file in current directory or its parents
- `.env.local` file in current directory or its parents
- environment variable
- command line option

## Acknowledgements

The idea to build fast and correct is based on [Bazel](https://bazel.build/). 
