# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Automatic scaling for grouping operations when working in the environment with less than 16GB of memory
- More detailed steps names

### Changed

- Locked minimal Shapely version
- Modified ways grouping logic to be faster
- Split filtered and required ways to be parsed separately

### Fixed

- Increased speed estimation period for Rich time progress

## [0.2.0] - 2023-12-29

### Added

- CLI based on Typer for converting PBF files into GeoParquet

## [0.1.0] - 2023-12-29

### Added

- Created QuackOSM repository
- Implemented PbfFileReader

[Unreleased]: https://github.com/kraina-ai/quackosm/compare/0.2.0...HEAD

[0.2.0]: https://github.com/kraina-ai/quackosm/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/kraina-ai/quackosm/releases/tag/0.1.0
