# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.15.1] - 2025-06-28

### Fixed

- Missing working directory when compressing value columns for multiple files at once

## [0.15.0] - 2025-06-16

### Changed

- Removed `h3` and `s2` dependencies from the `cli` group and replaced them with `h3` DuckDB extension and `s2sphere` Python library
- Applied changes required for the `conda` release [#142](https://github.com/kraina-ai/quackosm/issues/142) [#217](https://github.com/kraina-ai/quackosm/issues/217)

### Removed

- `geoarrow-pandas` from dependencies

## [0.14.2] - 2025-06-14

### Added

- `MissingOsmCacheWarning` for the user to inform that OSM index has to be built and the source can be switched to Geofabrik [#213](https://github.com/kraina-ai/quackosm/issues/213)
- `OldOsmCacheWarning` for the user to inform that currently saved cache is over 1 year old and it can be outdated [#213](https://github.com/kraina-ai/quackosm/issues/213)

### Changed

- Bumped minimal version of the `h3` library to `4.1.0`

## [0.14.1] - 2025-05-23

### Added

- Option to skip metadata tags filtering, based on a default GDAL configuration

## [0.14.0] - 2025-05-17

### Added

- Option to sort result files by geometry to reduce file size
- Additional `_sorted` suffix used in the result file name
- Option to define final parquet file compression with level and number of rows per group
- Typing stubs for mypy

### Changed

- Default result parquet file compression from `snappy` to `zstd` with level 3
- Number of rows in a parquet row group to `100000`
- Bumped minimal version of DuckDB to `1.1.2` and polars to `1.9`
- Refactored internal logic by exporting it to external `rq_geo_toolkit` library
- Changed multiple files merging logic

### Fixed

- Replace geo metadata in final geoparquet with proper bounding box size and geometry types
- Changed polars LazyFrame execution for newer versions
- Remove wrong empty geometries from relations
- Steps numbering with complex OSM tags filtering

### Deprecated

- Replaced `parquet_compression` parameter in `PbfFileReader` class with `compression`

## [0.13.0] - 2025-02-26

### Changed

- Shortened the cache file paths hashes from default 64 characters to 8 [#188](https://github.com/kraina-ai/quackosm/issues/188)

## [0.12.1] - 2025-01-03

### Added

- Automatic download progress bar hiding when verbosity is set to `silent`.
- Cached nominatim geocoding results to speed up tests

## [0.12.0] - 2024-11-03

### Added

- Option to pass custom SQL filters with `custom_sql_filter` (and `--custom-sql-filter`) parameter [#67](https://github.com/kraina-ai/quackosm/issues/67)

### Fixed

- Delayed progress bar appearing during nodes intersection step

## [0.11.4] - 2024-10-28

### Changed

- Improved multiprocessing intersection algorithm by early stopping processes start-up if finished quicker than expected

## [0.11.3] - 2024-10-25

### Changed

- Moved location of the OSM extracts providers to the global cache [#173](https://github.com/kraina-ai/quackosm/issues/173)

## [0.11.2] - 2024-10-14

### Added

- Option to pass a bounding box as a geometry filter in CLI [#169](https://github.com/kraina-ai/quackosm/issues/169)

### Changed

- Modified CLI descriptions and hid unnecessary default values [#169](https://github.com/kraina-ai/quackosm/issues/169)

## [0.11.1] - 2024-10-09

### Added

- Option to export to DuckDB database [#94](https://github.com/kraina-ai/quackosm/issues/119) (implemented by [@mwip](https://github.com/mwip))

## [0.11.0] - 2024-09-24

### Changed

- Bumped minimal DuckDB version to `1.1.0`
- Refactored geoparquet operations for compatibility with new DuckDB version
- Excluded `conftest.py` file from the final library build
- Replaced `unary_union` calls with `union_all()` on all GeoDataFrames
- Silenced `pooch` library warnings regarding empty SHA hash

## [0.10.0] - 2024-09-23

### Changed

- **BREAKING** Changed required minimal number of points in polygon from 3 to 4
- Added removal of repeated points in linestrings

### Fixed

- Removed support for yanked polars version `1.7.0`

## [0.9.4] - 2024-09-11

### Changed

- Excluded DuckDB `1.1.0` version from dependencies

## [0.9.3] - 2024-09-10

### Removed

- `geoarrow-rust-core` from dependencies

## [0.9.2] - 2024-08-28

### Changed

- Removed `pyarrow-ops` dependency and replaced it with simpler implementation
- Removed `srai` dependency from tests
- Set minimal `numpy` version

## [0.9.1] - 2024-08-28

### Fix

- Changed `geopy` dependency to required, to fix missing import for `quackosm.geocode_to_geometry` function

## [0.9.0] - 2024-08-12

### Added

- Functions `convert_osm_extract_to_parquet` and `convert_osm_extract_to_geodataframe` with option to search and download OSM extracts by text query [#119](https://github.com/kraina-ai/quackosm/issues/119)
- Function for downloading an OSM extract PBF file using a text query (`quackosm.osm_extracts.download_extract_by_query`)
- Function for displaying available OSM extracts from multiple sources (`quackosm.osm_extracts.display_available_extracts` and `--show-extracts` / `--show-osm-extracts` in cli) in the form of a tree
- New parameter `geometry_coverage_iou_threshold` (and `--iou-threshold` in cli) to enable configuration of the Intersection over Union metric value sensitivity for covering the geometry with OSM extracts
- Two new notebook examples for documentation purposes - basic usage and OSM extracts deep dive
- Improved tests configuration by downloading precalculated extracts indexes from a dedicated repository

### Changed

- Refactored searching OSM extracts for a given geometry filter to utilize Intersection over Union metric [#110](https://github.com/kraina-ai/quackosm/issues/110) [#115](https://github.com/kraina-ai/quackosm/issues/115)
- Moved multiple modules imports inside certain functions to speed up CLI responsiveness
- Replaced default `Geofabrik` OSM extract download source with `any` to include all available resources
- Refactored OSM extracts sources cache files to calculate area in kilometers squared and added `parent` and `file_name` fields

### Deprecated

- Function `find_smallest_containing_extract` from `quackosm.osm_extracts` have been deprecated in favor of `find_smallest_containing_extracts`

## [0.8.3] - 2024-07-25

### Added

- New function `quackosm.geocode_to_geometry` for quick geocoding of the text query to a geometry

### Changed

- Replaced `OSMnx` dependency with `GeoPy` for geometry geocoding [#135](https://github.com/kraina-ai/quackosm/issues/135)

## [0.8.2] - 2024-06-04

### Added

- `geoarrow-rust-core` library to the main dependencies
- Test for hashing geometry filter with mixed order
- Test for parquet multiprocessing logic
- Test for new intersection step
- Option to pass URL directly as PBF path [#114](https://github.com/kraina-ai/quackosm/issues/114)
- Dedicated `MultiprocessingRuntimeError` for multiprocessing errors

### Changed

- Added new internal parquet dataset processing logic using multiprocessing
- Refactored nodes intersection step from `ST_Intersects` in DuckDB to Shapely's `STRtree` [#112](https://github.com/kraina-ai/quackosm/issues/112)
- `PbfFileReader`'s internal `geometry_filter` is additionally clipped by PBF extract geometry to speed up intersections [#116](https://github.com/kraina-ai/quackosm/issues/116)
- `OsmTagsFilter` and `GroupedOsmTagsFilter` type from `dict` to `Mapping` to make it covariant
- Tqdm's `disable` parameter for non-TTY environments from `None` to `False`
- Refactored final GeoParquet file saving logic to greatly reduce memory usage
- Bumped minimal `pyarrow` version to 16.0
- Default `multiprocessing.Pool` initialization mode from `fork` to `spawn`

## [0.8.1] - 2024-05-11

### Added

- Option to convert multiple `*.osm.pbf` files to a single `parquet` file

### Changed

- Names of the functions have been unified to all start with `convert_` prefix
- Simplified internal conversion API

### Deprecated

- Functions `convert_pbf_to_gpq`, `convert_geometry_to_gpq`/`convert_geometry_filter_to_gpq`, `get_features_gdf` and `get_features_gdf_from_geometry` have been deprecated in favor of `convert_pbf_to_parquet`, `convert_geometry_to_parquet`, `convert_pbf_to_geodataframe` and `convert_geometry_to_geodataframe`
- Parameter `file_paths` has been replaced with `pbf_path`

### Fixed

- Removed the `parquet` extension installation step after opening the DuckDB connection

## [0.8.0] - 2024-05-08

### Added

- Polars library to the main dependencies

### Changed

- Refactored ways grouping logic from duckdb to polars `LazyFrame` API for faster operations
- Default result file extension from `geoparquet` to `parquet` [#99](https://github.com/kraina-ai/quackosm/issues/99)
- Moved `rich` to the main dependencies [#95](https://github.com/kraina-ai/quackosm/issues/95)
- Set minimal versions of multiple dependencies
- Added tests for minimal dependencies versions

### Fixed

- Steps numbering after encountering `MemoryError`

### Removed

- `h3ronpy` from dependencies and replaced logic with pure `h3` calls

### Deprecated

- Reusing existing `geoparquet` files from cache will be supported, but will result in deprecation warning [#99](https://github.com/kraina-ai/quackosm/issues/99)

## [0.7.3] - 2024-05-07

### Added

- Debug mode that keeps all temporary files for further inspection, activated with `debug` flag

### Changed

- Refactored parsing native `LINESTRING_2D` types when reading them from saved parquet file

## [0.7.2] - 2024-04-28

### Changed

- Refactored geometry fixing by utilizing `ST_MakeValid` function added in DuckDB `0.10.0` version

## [0.7.1] - 2024-04-25

### Changed

- Simplified GDAL parity tests by precalculating result files and uploading them to additional repository

### Fixed

- Added exception if parts of provided geometry have no area [#85](https://github.com/kraina-ai/quackosm/issues/85)

## [0.7.0] - 2024-04-24

### Added

- Transient mode of reporting progress with output being removed after operation [#77](https://github.com/kraina-ai/quackosm/issues/77)
- Tracking for multiple files within single operation
- New tests for all 3 methods of combining result files together with duplicated features removal

### Changed

- Refactored internal Rich progress reporting process
- Replaced `silent_mode` parameter with `verbosity_mode` argument
- Changed default `OSMExtractSource` value from `any` to `Geofabrik`
- Modified OpenStreetMap\_fr scraping process with better progress bar UI

### Removed

- `silent_mode` parameter from the Python API

### Fixed

- Replaced slash characters in Geofabrik index IDs with underscore to prevent nested directories creation
- Added additional check on number of points in a LineString when trying to represent them as a polygon

## [0.6.1] - 2024-04-17

### Changed

- Set minimal `duckdb` version to `0.10.2`
- Added support for Python 3.12

## [0.6.0] - 2024-04-16

### Added

- Option to filter by OSM tags with negative values (`False`) and with wildcard asterisk (`*`) expansion in both keys and values [#49](https://github.com/kraina-ai/quackosm/issues/49) [#53](https://github.com/kraina-ai/quackosm/issues/53)

### Changed

- Set minimal `typer` version to `0.9.0`

## [0.5.3] - 2024-04-05

### Fixed

- Made geometry orientation agnostic hash algorithm

## [0.5.2] - 2024-04-03

### Added

- Progress bars for final merge of multiple geoparquet files into a single file
- Option to allow provided geometry to not be fully covered by existing OSM extracts [#68](https://github.com/kraina-ai/quackosm/issues/68)

### Fixed

- Changed tqdm's kwargs for parallel OSM extracts checks

## [0.5.1] - 2024-03-23

### Fixed

- Added alternative way to remove `feature_id` duplicates for big data operations
- Slowed down rich progress bars refresh rate

## [0.5.0] - 2024-03-14

### Added

- Option to disable progress reporting with the `--silent` flag and `silent_mode` argument [#14](https://github.com/kraina-ai/quackosm/issues/14)
- New example notebook dedicated to the command line interface
- Option to save parquet files with WKT geometry [#7](https://github.com/kraina-ai/quackosm/issues/7)
- Total elapsed time summary at the end [#15](https://github.com/kraina-ai/quackosm/issues/15)

### Changed

- Simplified and improved ways grouping process
- Renamed `rows_per_bucket` parameter to `rows_per_group`

### Fixed

- Set minimal `h3` and `h3ronpy` versions in requirements

## [0.4.5] - 2024-03-07

### Fixed

- Added automatic downscaling of the `rows_per_bucket` parameter for ways grouping operation [#50](https://github.com/kraina-ai/quackosm/issues/50)

## [0.4.4] - 2024-02-14

### Fixed

- Locked DuckDB's version to 0.9.2 to avoid segmentation fault

## [0.4.3] - 2024-02-13

### Fixed

- Added parquet schema unification when joining multiple files together [#42](https://github.com/kraina-ai/quackosm/issues/42)

## [0.4.2] - 2024-02-02

### Fixed

- Removed last grouping step when using `keep_all_tags` parameter with `GroupedOsmTagsFilter` filter

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

[Unreleased]: https://github.com/kraina-ai/quackosm/compare/0.15.1...HEAD

[0.15.1]: https://github.com/kraina-ai/quackosm/compare/0.15.0...0.15.1

[0.15.0]: https://github.com/kraina-ai/quackosm/compare/0.14.2...0.15.0

[0.14.2]: https://github.com/kraina-ai/quackosm/compare/0.14.1...0.14.2

[0.14.1]: https://github.com/kraina-ai/quackosm/compare/0.14.0...0.14.1

[0.14.0]: https://github.com/kraina-ai/quackosm/compare/0.13.0...0.14.0

[0.13.0]: https://github.com/kraina-ai/quackosm/compare/0.12.1...0.13.0

[0.12.1]: https://github.com/kraina-ai/quackosm/compare/0.12.0...0.12.1

[0.12.0]: https://github.com/kraina-ai/quackosm/compare/0.11.4...0.12.0

[0.11.4]: https://github.com/kraina-ai/quackosm/compare/0.11.3...0.11.4

[0.11.3]: https://github.com/kraina-ai/quackosm/compare/0.11.2...0.11.3

[0.11.2]: https://github.com/kraina-ai/quackosm/compare/0.11.1...0.11.2

[0.11.1]: https://github.com/kraina-ai/quackosm/compare/0.11.0...0.11.1

[0.11.0]: https://github.com/kraina-ai/quackosm/compare/0.10.0...0.11.0

[0.10.0]: https://github.com/kraina-ai/quackosm/compare/0.9.4...0.10.0

[0.9.4]: https://github.com/kraina-ai/quackosm/compare/0.9.3...0.9.4

[0.9.3]: https://github.com/kraina-ai/quackosm/compare/0.9.2...0.9.3

[0.9.2]: https://github.com/kraina-ai/quackosm/compare/0.9.1...0.9.2

[0.9.1]: https://github.com/kraina-ai/quackosm/compare/0.9.0...0.9.1

[0.9.0]: https://github.com/kraina-ai/quackosm/compare/0.8.3...0.9.0

[0.8.3]: https://github.com/kraina-ai/quackosm/compare/0.8.2...0.8.3

[0.8.2]: https://github.com/kraina-ai/quackosm/compare/0.8.1...0.8.2

[0.8.1]: https://github.com/kraina-ai/quackosm/compare/0.8.0...0.8.1

[0.8.0]: https://github.com/kraina-ai/quackosm/compare/0.7.3...0.8.0

[0.7.3]: https://github.com/kraina-ai/quackosm/compare/0.7.2...0.7.3

[0.7.2]: https://github.com/kraina-ai/quackosm/compare/0.7.1...0.7.2

[0.7.1]: https://github.com/kraina-ai/quackosm/compare/0.7.0...0.7.1

[0.7.0]: https://github.com/kraina-ai/quackosm/compare/0.6.1...0.7.0

[0.6.1]: https://github.com/kraina-ai/quackosm/compare/0.6.0...0.6.1

[0.6.0]: https://github.com/kraina-ai/quackosm/compare/0.5.3...0.6.0

[0.5.3]: https://github.com/kraina-ai/quackosm/compare/0.5.2...0.5.3

[0.5.2]: https://github.com/kraina-ai/quackosm/compare/0.5.1...0.5.2

[0.5.1]: https://github.com/kraina-ai/quackosm/compare/0.5.0...0.5.1

[0.5.0]: https://github.com/kraina-ai/quackosm/compare/0.4.5...0.5.0

[0.4.5]: https://github.com/kraina-ai/quackosm/compare/0.4.4...0.4.5

[0.4.4]: https://github.com/kraina-ai/quackosm/compare/0.4.3...0.4.4

[0.4.3]: https://github.com/kraina-ai/quackosm/compare/0.4.2...0.4.3

[0.4.2]: https://github.com/kraina-ai/quackosm/compare/0.4.1...0.4.2

[0.4.1]: https://github.com/kraina-ai/quackosm/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/kraina-ai/quackosm/compare/0.3.3...0.4.0

[0.3.3]: https://github.com/kraina-ai/quackosm/compare/0.3.2...0.3.3

[0.3.2]: https://github.com/kraina-ai/quackosm/compare/0.3.1...0.3.2

[0.3.1]: https://github.com/kraina-ai/quackosm/compare/0.3.0...0.3.1

[0.3.0]: https://github.com/kraina-ai/quackosm/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/kraina-ai/quackosm/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/kraina-ai/quackosm/releases/tag/0.1.0
