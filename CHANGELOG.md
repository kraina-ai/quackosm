# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1] - 2024-01-31

### Changed

- Removed additional redundancy of GeoParquet result files when only one extract covers whole area [#35](https://github.com/kraina-ai/quackosm/issues/35)

### Fixed

- Added missing `requests` dependency

## [0.4.0] - 2024-01-31

### Added

- Option to automatically download PBF files for geometries [#32](https://github.com/kraina-ai/quackosm/issues/32)
- Filtering data using 3 global grid systems: Geohash, H3 and S2 [#30](https://github.com/kraina-ai/quackosm/issues/30)

### Changed

- Filter OSM IDs are now expected to be passed after comma instead of repeating `--filter-osm-id` every time [#30](https://github.com/kraina-ai/quackosm/issues/30)

### Fixed

- Remove duplicated features when parsing multiple PBF files
- Geometry orienting to eliminate hash differences between operating systems and different equal versions of the same geometry

## [0.3.3] - 2024-01-16

### Added

- Option to pass OSM tags filter in the form of JSON file to the CLI
- Option to keep all tags when filtering with the OSM tags [#25](https://github.com/kraina-ai/quackosm/issues/25)

### Changed

- Logic for `explode_tags` parameter when filtering based on tags, but still keeping them all

### Fixed

- Typos in the CLI docs

## [0.3.2] - 2024-01-10

### Added

- Option to pass `parquet_compression` parameter to DuckDB
- Bigger PBF parsing test as a benchmark

### Changed

- Increased number of rows per group for environments with more than 24 GB of memory
- Simplified temporal directory path propagation within `PbfFileReader` class
- Reduced disk spillage by removing more files during operation
- Optimized final geometries concatenation by removing `UNION` operation
- Tests execution order

## [0.3.1] - 2024-01-06

### Added

- Speed column for Rich progress bar

### Changed

- Simplified ways grouping logic by removing some steps

## [0.3.0] - 2024-01-02

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

[Unreleased]: https://github.com/kraina-ai/quackosm/compare/0.4.1...HEAD

[0.4.1]: https://github.com/kraina-ai/quackosm/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/kraina-ai/quackosm/compare/0.3.3...0.4.0

[0.3.3]: https://github.com/kraina-ai/quackosm/compare/0.3.2...0.3.3

[0.3.2]: https://github.com/kraina-ai/quackosm/compare/0.3.1...0.3.2

[0.3.1]: https://github.com/kraina-ai/quackosm/compare/0.3.0...0.3.1

[0.3.0]: https://github.com/kraina-ai/quackosm/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/kraina-ai/quackosm/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/kraina-ai/quackosm/releases/tag/0.1.0
