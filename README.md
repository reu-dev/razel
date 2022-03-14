# chained commands executor (cce)
or chained tasks executor (cte)

## Goals
* fast and correct, like Bazel
* built-in convenience functions
  * parse measurement from stdout of task, aggregate them
  * optionally replace outputs on errors
  * summary grouped by tasks to make tasks errors understandable
  * JUnit test output, e.g. for GitLab CI
* simple query language to filter tasks
* task scheduling and caching depending on resources 
  * automatic disk cleanup locally and for cache
  * disk usage, RAM, network speed and parallel instances of external tools might be limited
  * measure/predict task execution time and output size
* transparent remote execution
* data/results down/upload to storage, e.g. Git LFS, MinIO
  * local access to important outputs of remotely executed tasks

## Ideas
* integrate wasi executor

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
* CTest
  * no caching / remote execution
