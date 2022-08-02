# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [UNRELEASED]

## Changed

- CHANGE FOR PR CREATION

## [0.2.2] - 2022-04-14

### Added

- Added unit tests for the custom executor template.

## [0.2.1] - 2022-04-13

### Fixed

- The executor no longer tries to manipulate the return value of the function if function execution failed.

## [0.2.0] - 2022-04-13

### Changed

- Slight refactor for the Covalent microservices refactor.

## [0.1.0] - 2022-03-31

### Changed

- Fixed package structure, so that the plugin is found by Covalent after installing the plugin.
- Added global variable _EXECUTOR_PLUGIN_DEFAULTS, which is now needed by Covalent.
- Changed global variable executor_plugin_name -> EXECUTOR_PLUGIN_NAME in executors to conform with PEP8.

## [0.0.1] - 2022-03-02

### Added

- Core files for this repo.
- CHANGELOG.md to track changes (this file).
- Semantic versioning in VERSION.
- CI pipeline job to enforce versioning.
