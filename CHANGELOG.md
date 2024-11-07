# Changelog

## Unreleased

### Added

- changelog

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
