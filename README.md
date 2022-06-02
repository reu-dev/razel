# razel: rusty bazel, reu's bazel

## Modes

* execute single command
    * for developing/debugging or even for low-level integration in (Python) scripts
* execute single task (=built-in convenience function)
* execute batch files containing command lines
    * input and output files are not specified
* execute CTest files (low prio)
    * CTest allows specifying input files, but not output files
* high level language APIs (Deno, Python)
    * user defines command chains using his favorite scripting language
    * razel executes processes
    * directly or maybe using a custom language/json file in between

## Goals / Ideas

* fast and correct, like Bazel
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
* execute processes in sandbox, like Bazel
    * provide specified inputs, check expected outputs
    * in ramdisk?
    * in docker/podman?
    * exec with own uid?
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
* ensure correctness
    * UB check for modified executables: run first commands multiple times to verify that the outputs are consistent
    * avoid cache poisoning when disk full: missing fwrite/fclose checks in an executable would break cache
* tools for users to debug errors
    * show command lines ready for c&p into debugger
    * UB check of chain until first failing command
    * if source code of executables available: rebuild with debug and sanitizers and run with those executables
    * for command with long list of inputs: bisect inputs to create minimal reproducible example

## Why not ...?

* Bazel
    * additional launcher script required for some simple tasks
        * using stdout of action as input for another action
        * parsing measurements from stdout of action
        * CTest features like FAIL_REGULAR_EXPRESSION, WILL_FAIL
    * difficult to get command lines for debugging
    * no automatic disk usage limit/cleanup for local cache - all temp output needs to fit on disk
    * no native support for response files
    * resources cannot be reserved to run real-time critical tests
    * content of bazel-bin/out directories is not defined (contains mixture of current build and cache)
* CTest
    * no caching / remote execution
