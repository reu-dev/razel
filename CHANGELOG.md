# Changelog

## [v0.5.6](https://github.com/reu-dev/razel/releases/tag/v0.5.6) - 2025-11-11

### Added

- apis: main function to download/run native binary
- binary for macos-14 aarch64
- binary for ubuntu-24.04 aarch64

### Fixed

- finish tui status before printing report

### Changed

- update macos x86_64 binary from macos-13 to macos-14
- also use stdout for improving error message

## [v0.5.5](https://github.com/reu-dev/razel/releases/tag/v0.5.5) - 2025-08-03

### Fixed

- error handling for download-file task
- http-remote-exec: disable host after server error

### Changed

- api: improve env handling
- update Windows release to windows-2022

## [v0.5.4](https://github.com/reu-dev/razel/releases/tag/v0.5.4) - 2025-04-17

### Fixed

- api: use full paths in ensureEqual/ensureNotEqual to fix command filtering
- fix filtering commands with explicit deps

### Changed

- update Linux release to Ubuntu 22.04
- update deps

## [v0.5.3](https://github.com/reu-dev/razel/releases/tag/v0.5.3) - 2025-02-21

### Fixed

- handling of non UTF-8 stdout/stderr

### Changed

- deno api: replace deprecated Deno.run() with Deno.Command()
- deno api: use deno fmt
- update deps [#49](https://github.com/reu-dev/razel/pull/49)

## [v0.5.2](https://github.com/reu-dev/razel/releases/tag/v0.5.2) - 2024-12-09

### Added

- changelog
- `system check-remote-cache` subcommand
- error message in log file items [#48](https://github.com/reu-dev/razel/pull/48)

### Changed

- Python API speed up [#47](https://github.com/reu-dev/razel/pull/47) thanks [@phenyque](https://github.com/phenyque)
- improve error message by parsing C/C++ assert and Rust panic from
  stderr [#48](https://github.com/reu-dev/razel/pull/48)
- improve rules to specify in/out files in batch files
- update deps

## [v0.5.1](https://github.com/reu-dev/razel/releases/tag/v0.5.1) - 2024-11-07

### Added

- target filter: positional args, `--filter-regex` and `--filter-regex-all`
- timeout tag
- `import` subcommand to create `razel.jsonl` from batch files
- improve parsing batch files

### Fixed

- retry on oom kill

### Changed

- improve retry printout
- update deps

## [v0.5.0](https://github.com/reu-dev/razel/releases/tag/v0.5.0) - 2024-06-19

### Added

- http remote exec [#46](https://github.com/reu-dev/razel/pull/46)
- apis: add function to read log file

### Changed

- improve log file readability
- update rust-version
