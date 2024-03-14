"""
PBF File Reader.

This module contains a reader capable of parsing a PBF file into a GeoDataFrame.
"""

import hashlib
import itertools
import json
import secrets
import shutil
import tempfile
import time
import warnings
from collections.abc import Iterable
from math import floor
from multiprocessing import Pool
from pathlib import Path
from time import sleep
from typing import Any, Literal, NamedTuple, Optional, Union, cast

import duckdb
import geoarrow.pyarrow as ga
import geopandas as gpd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import shapely.wkt as wktlib
from geoarrow.pyarrow import io
from pyarrow_ops import drop_duplicates
from shapely.geometry import LinearRing, Polygon
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry

from quackosm._constants import FEATURES_INDEX, GEOMETRY_COLUMN, WGS84_CRS
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter, merge_osm_tags_filter
from quackosm._osm_way_polygon_features import OsmWayPolygonConfig, parse_dict_to_config_object
from quackosm._rich_progress import (  # type: ignore[attr-defined]
    TaskProgressBar,
    TaskProgressSpinner,
    log_message,
    show_total_elapsed_time,
)
from quackosm._typing import is_expected_type
from quackosm.osm_extracts import (
    OsmExtractSource,
    download_extracts_pbf_files,
    find_smallest_containing_extract,
)

__all__ = [
    "PbfFileReader",
]

MEMORY_1GB = 1024**3


class PbfFileReader:
    """
    PbfFileReader.

    PBF(Protocolbuffer Binary Format)[1] file reader is a dedicated `*.osm.pbf` files reader
    class based on DuckDB[2] and its spatial extension[3].

    Handler can filter out OSM features based on tags filter and geometry filter
    to limit the result.

    References:
        1. https://wiki.openstreetmap.org/wiki/PBF_Format
        2. https://duckdb.org/
        3. https://github.com/duckdb/duckdb_spatial
    """

    class ConvertedOSMParquetFiles(NamedTuple):
        """List of parquet files read from the `*.osm.pbf` file."""

        nodes_valid_with_tags: "duckdb.DuckDBPyRelation"
        nodes_filtered_ids: "duckdb.DuckDBPyRelation"

        ways_all_with_tags: "duckdb.DuckDBPyRelation"
        ways_with_unnested_nodes_refs: "duckdb.DuckDBPyRelation"
        ways_required_ids: "duckdb.DuckDBPyRelation"
        ways_filtered_ids: "duckdb.DuckDBPyRelation"

        relations_all_with_tags: "duckdb.DuckDBPyRelation"
        relations_with_unnested_way_refs: "duckdb.DuckDBPyRelation"
        relations_filtered_ids: "duckdb.DuckDBPyRelation"

    ROWS_PER_GROUP_MEMORY_CONFIG = {
        0: 100_000,
        8: 500_000,
        16: 1_000_000,
        24: 5_000_000,
    }

    def __init__(
        self,
        tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
        geometry_filter: Optional[BaseGeometry] = None,
        working_directory: Union[str, Path] = "files",
        osm_way_polygon_features_config: Optional[
            Union[OsmWayPolygonConfig, dict[str, Any]]
        ] = None,
        parquet_compression: str = "snappy",
        osm_extract_source: OsmExtractSource = OsmExtractSource.any,
        silent_mode: bool = False,
    ) -> None:
        """
        Initialize PbfFileReader.

        Args:
            tags_filter (Union[OsmTagsFilter, GroupedOsmTagsFilter], optional): A dictionary
                specifying which tags to download.
                The keys should be OSM tags (e.g. `building`, `amenity`).
                The values should either be `True` for retrieving all objects with the tag,
                string for retrieving a single tag-value pair
                or list of strings for retrieving all values specified in the list.
                `tags={'leisure': 'park}` would return parks from the area.
                `tags={'leisure': 'park, 'amenity': True, 'shop': ['bakery', 'bicycle']}`
                would return parks, all amenity types, bakeries and bicycle shops.
                If `None`, handler will allow all of the tags to be parsed. Defaults to `None`.
            geometry_filter (BaseGeometry, optional): Region which can be used to filter only
                intersecting OSM objects. Defaults to `None`.
            working_directory (Union[str, Path], optional): Directory where to save
                the parsed `*.parquet` files. Defaults to "files".
            osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
                Config used to determine which closed way features are polygons.
                Modifications to this config left are left for experienced OSM users.
                Defaults to predefined "osm_way_polygon_features.json".
            parquet_compression (str): Compression of intermediate parquet files.
                Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
                Defaults to "snappy".
            osm_extract_source (OsmExtractSource): A source for automatic downloading of
                OSM extracts. Can be Geofabrik, BBBike, OSM_fr or any. Defaults to `any`.
            silent_mode (bool): Disable progress bars.
        """
        self.tags_filter = tags_filter
        self.merged_tags_filter = merge_osm_tags_filter(tags_filter) if tags_filter else None
        self.geometry_filter = geometry_filter
        self.osm_extract_source = osm_extract_source
        self.working_directory = Path(working_directory)
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.connection: duckdb.DuckDBPyConnection = None
        self.encountered_query_exception = False
        self.silent_mode = silent_mode

        self.rows_per_group = PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[0]
        actual_memory = psutil.virtual_memory()
        # If more than 8 / 16 / 24 GB total memory, increase the number of rows per group
        for memory_gb, rows_per_group in PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG.items():
            if actual_memory.total >= (memory_gb * MEMORY_1GB):
                self.rows_per_group = rows_per_group
            else:
                break

        self.parquet_compression = parquet_compression

        if osm_way_polygon_features_config is None:
            # Config based on two sources + manual OSM wiki check
            # 1. https://github.com/tyrasd/osm-polygon-features/blob/v0.9.2/polygon-features.json
            # 2. https://github.com/ideditor/id-area-keys/blob/v5.0.1/areaKeys.json
            osm_way_polygon_features_config = json.loads(
                (Path(__file__).parent / "osm_way_polygon_features.json").read_text()
            )

        self.osm_way_polygon_features_config: OsmWayPolygonConfig = (
            osm_way_polygon_features_config
            if isinstance(osm_way_polygon_features_config, OsmWayPolygonConfig)
            else parse_dict_to_config_object(osm_way_polygon_features_config)
        )

    def convert_pbf_to_gpq(
        self,
        pbf_path: Union[str, Path],
        result_file_path: Optional[Union[str, Path]] = None,
        keep_all_tags: bool = False,
        explode_tags: Optional[bool] = None,
        ignore_cache: bool = False,
        filter_osm_ids: Optional[list[str]] = None,
        save_as_wkt: bool = False,
    ) -> Path:
        """
        Convert PBF file to GeoParquet file.

        Args:
            pbf_path (Union[str, Path]): Pbf file to be parsed to GeoParquet.
            result_file_path (Union[str, Path], optional): Where to save
                the geoparquet file. If not provided, will be generated based on hashes
                from provided tags filter and geometry filter. Defaults to `None`.
            keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
                Whether to keep all tags related to the element, or return only those defined
                in the `tags_filter`. When `True`, will override the optional grouping defined
                in the `tags_filter`. Defaults to `False`.
            explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
                If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
                If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
                be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
            ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
                Defaults to False.
            filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
                Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
                Defaults to an empty list.
            save_as_wkt (bool): Whether to save the file with geometry in the WKT form instead
                of WKB. If `True`, it will be saved as a `.parquet` file, because it won't be
                in the GeoParquet standard. Defaults to `False`.

        Returns:
            Path: Path to the generated GeoParquet file.
        """
        if filter_osm_ids is None:
            filter_osm_ids = []

        if explode_tags is None:
            explode_tags = self.tags_filter is not None and not keep_all_tags

        with tempfile.TemporaryDirectory(dir=self.working_directory.resolve()) as self.tmp_dir_name:
            self.tmp_dir_path = Path(self.tmp_dir_name)
            try:
                self.encountered_query_exception = False
                self.connection = _set_up_duckdb_connection(tmp_dir_path=self.tmp_dir_path)
                result_file_path = result_file_path or self._generate_result_file_path(
                    pbf_path,
                    filter_osm_ids=filter_osm_ids,
                    keep_all_tags=keep_all_tags,
                    explode_tags=explode_tags,
                    save_as_wkt=save_as_wkt,
                )
                parsed_geoparquet_file = self._parse_pbf_file(
                    pbf_path=pbf_path,
                    result_file_path=Path(result_file_path),
                    filter_osm_ids=filter_osm_ids,
                    keep_all_tags=keep_all_tags,
                    explode_tags=explode_tags,
                    ignore_cache=ignore_cache,
                    save_as_wkt=save_as_wkt,
                )

                return parsed_geoparquet_file
            finally:
                if self.connection is not None:
                    self.connection.close()
                    self.connection = None

    def convert_geometry_filter_to_gpq(
        self,
        result_file_path: Optional[Union[str, Path]] = None,
        keep_all_tags: bool = False,
        explode_tags: Optional[bool] = None,
        ignore_cache: bool = False,
        filter_osm_ids: Optional[list[str]] = None,
        save_as_wkt: bool = False,
    ) -> Path:
        """
        Convert geometry to GeoParquet file.

        Will automatically find and download OSM extracts covering a given geometry
        and convert them to a single GeoParquet file.

        Args:
            result_file_path (Union[str, Path], optional): Where to save
                the geoparquet file. If not provided, will be generated based on hashes
                from provided tags filter and geometry filter. Defaults to `None`.
            keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
                Whether to keep all tags related to the element, or return only those defined
                in the `tags_filter`. When `True`, will override the optional grouping defined
                in the `tags_filter`. Defaults to `False`.
            explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
                If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
                If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
                be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
            ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
                Defaults to False.
            filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
                Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
                Defaults to an empty list.
            save_as_wkt (bool): Whether to save the file with geometry in the WKT form instead
                of WKB. If `True`, it will be saved as a `.parquet` file, because it won't be
                in the GeoParquet standard. Defaults to `False`.

        Returns:
            Path: Path to the generated GeoParquet file.
        """
        if self.geometry_filter is None:
            raise AttributeError(
                "Cannot find matching OSM extracts without geometry filter. Please configure"
                " geometry_filter first: PbfFileReader(geometry_filter=..., **kwargs)."
            )

        if filter_osm_ids is None:
            filter_osm_ids = []

        if explode_tags is None:
            explode_tags = self.tags_filter is not None and not keep_all_tags

        result_file_path = Path(
            result_file_path
            or self._generate_result_file_path_from_geometry(
                filter_osm_ids=filter_osm_ids,
                keep_all_tags=keep_all_tags,
                explode_tags=explode_tags,
                save_as_wkt=save_as_wkt,
            )
        )

        matching_extracts = find_smallest_containing_extract(
            self.geometry_filter, self.osm_extract_source
        )

        if len(matching_extracts) == 1:
            pbf_files = download_extracts_pbf_files(matching_extracts, self.working_directory)
            return self.convert_pbf_to_gpq(
                pbf_files[0],
                result_file_path=result_file_path,
                keep_all_tags=keep_all_tags,
                explode_tags=explode_tags,
                ignore_cache=ignore_cache,
                filter_osm_ids=filter_osm_ids,
                save_as_wkt=save_as_wkt,
            )
        else:
            if not result_file_path.exists() or ignore_cache:
                matching_extracts = find_smallest_containing_extract(
                    self.geometry_filter, self.osm_extract_source
                )
                pbf_files = download_extracts_pbf_files(matching_extracts, self.working_directory)

                parsed_geoparquet_files = []
                for file_path in pbf_files:
                    parsed_geoparquet_file = self.convert_pbf_to_gpq(
                        file_path,
                        keep_all_tags=keep_all_tags,
                        explode_tags=explode_tags,
                        ignore_cache=ignore_cache,
                        filter_osm_ids=filter_osm_ids,
                        save_as_wkt=save_as_wkt,
                    )
                    parsed_geoparquet_files.append(parsed_geoparquet_file)

                joined_parquet_table = self._drop_duplicates_features_in_pyarrow_table(
                    parsed_geoparquet_files
                )
                if save_as_wkt:
                    pq.write_table(joined_parquet_table, result_file_path)
                else:
                    io.write_geoparquet_table(
                        joined_parquet_table,
                        result_file_path,
                        primary_geometry_column=GEOMETRY_COLUMN,
                    )

        return Path(result_file_path)

    def get_features_gdf(
        self,
        file_paths: Union[str, Path, Iterable[Union[str, Path]]],
        keep_all_tags: bool = False,
        explode_tags: Optional[bool] = None,
        ignore_cache: bool = False,
        filter_osm_ids: Optional[list[str]] = None,
    ) -> gpd.GeoDataFrame:
        """
        Get features GeoDataFrame from a list of PBF files.

        Function parses multiple PBF files and returns a single GeoDataFrame with parsed
        OSM objects.

        Args:
            file_paths (Union[str, Path, Iterable[Union[str, Path]]]):
                Path or list of paths of `*.osm.pbf` files to be parsed.
            keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
                Whether to keep all tags related to the element, or return only those defined
                in the `tags_filter`. When `True`, will override the optional grouping defined
                in the `tags_filter`. Defaults to `False`.
            explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
                If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
                If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
                be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
            ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
                Defaults to False.
            filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
                Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
                Defaults to an empty list.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame with OSM features.
        """
        if isinstance(file_paths, (str, Path)):
            file_paths = [file_paths]

        if filter_osm_ids is None:
            filter_osm_ids = []

        if explode_tags is None:
            explode_tags = self.tags_filter is not None and not keep_all_tags

        parsed_geoparquet_files = []
        for file_path in file_paths:
            parsed_geoparquet_file = self.convert_pbf_to_gpq(
                file_path,
                keep_all_tags=keep_all_tags,
                explode_tags=explode_tags,
                ignore_cache=ignore_cache,
                filter_osm_ids=filter_osm_ids,
            )
            parsed_geoparquet_files.append(parsed_geoparquet_file)

        joined_parquet_table = self._drop_duplicates_features_in_pyarrow_table(
            parsed_geoparquet_files
        )
        gdf_parquet = gpd.GeoDataFrame(
            data=joined_parquet_table.drop(GEOMETRY_COLUMN).to_pandas(maps_as_pydicts="strict"),
            geometry=ga.to_geopandas(joined_parquet_table.column(GEOMETRY_COLUMN)),
        ).set_index(FEATURES_INDEX)

        return gdf_parquet

    def get_features_gdf_from_geometry(
        self,
        keep_all_tags: bool = False,
        explode_tags: Optional[bool] = None,
        ignore_cache: bool = False,
        filter_osm_ids: Optional[list[str]] = None,
    ) -> gpd.GeoDataFrame:
        """
        Get features GeoDataFrame from a provided geometry filter.

        Will automatically find and download OSM extracts covering a given geometry
        and return a single GeoDataFrame with parsed OSM objects.

        Args:
            keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
                Whether to keep all tags related to the element, or return only those defined
                in the `tags_filter`. When `True`, will override the optional grouping defined
                in the `tags_filter`. Defaults to `False`.
            explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
                If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
                If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
                be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
            ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
                Defaults to False.
            filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
                Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
                Defaults to an empty list.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame with OSM features.
        """
        parsed_geoparquet_file = self.convert_geometry_filter_to_gpq(
            keep_all_tags=keep_all_tags,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            filter_osm_ids=filter_osm_ids,
        )
        joined_parquet_table = io.read_geoparquet_table(parsed_geoparquet_file)
        gdf_parquet = gpd.GeoDataFrame(
            data=joined_parquet_table.drop(GEOMETRY_COLUMN).to_pandas(maps_as_pydicts="strict"),
            geometry=ga.to_geopandas(joined_parquet_table.column(GEOMETRY_COLUMN)),
        ).set_index(FEATURES_INDEX)

        return gdf_parquet

    def _drop_duplicates_features_in_pyarrow_table(
        self, parsed_geoparquet_files: list[Path]
    ) -> pa.Table:
        parquet_tables = [
            pq.read_table(parsed_parquet_file) for parsed_parquet_file in parsed_geoparquet_files
        ]
        joined_parquet_table: pa.Table = pa.concat_tables(parquet_tables, promote_options="default")
        if joined_parquet_table.num_rows > 0:
            joined_parquet_table = drop_duplicates(
                joined_parquet_table, on=["feature_id"], keep="first"
            )
        return joined_parquet_table

    def _parse_pbf_file(
        self,
        pbf_path: Union[str, Path],
        result_file_path: Path,
        filter_osm_ids: list[str],
        keep_all_tags: bool = False,
        explode_tags: bool = True,
        ignore_cache: bool = False,
        save_as_wkt: bool = False,
    ) -> Path:
        if not result_file_path.exists() or ignore_cache:
            start_time = time.time()

            elements = self.connection.sql(f"SELECT * FROM ST_READOSM('{Path(pbf_path)}');")
            converted_osm_parquet_files = self._prefilter_elements_ids(elements, filter_osm_ids)

            self._delete_directories(
                [
                    "nodes_filtered_non_distinct_ids",
                    "nodes_prepared_ids",
                    "ways_valid_ids",
                    "ways_filtered_non_distinct_ids",
                    "relations_valid_ids",
                    "relations_ids",
                ],
            )

            filtered_nodes_with_geometry_path = self._get_filtered_nodes_with_geometry(
                converted_osm_parquet_files
            )
            self._delete_directories("nodes_filtered_ids")

            filtered_ways_with_linestrings = self._get_filtered_ways_with_linestrings(
                osm_parquet_files=converted_osm_parquet_files
            )
            required_ways_with_linestrings = self._get_required_ways_with_linestrings(
                osm_parquet_files=converted_osm_parquet_files
            )
            self._delete_directories(
                [
                    "nodes_valid_with_tags",
                    "ways_required_grouped",
                    "ways_required_ids",
                    "ways_with_unnested_nodes_refs",
                    "required_ways_ids_grouped",
                    "required_ways_grouped",
                    "required_ways_tmp",
                    "filtered_ways_ids_grouped",
                    "filtered_ways_grouped",
                    "filtered_ways_tmp",
                ],
            )

            filtered_ways_with_proper_geometry_path = self._get_filtered_ways_with_proper_geometry(
                converted_osm_parquet_files, filtered_ways_with_linestrings
            )
            self._delete_directories(
                [
                    "ways_prepared_ids",
                    "ways_filtered_ids",
                    "ways_all_with_tags",
                    "filtered_ways_with_linestrings",
                ],
            )

            filtered_relations_with_geometry_path = self._get_filtered_relations_with_geometry(
                converted_osm_parquet_files, required_ways_with_linestrings
            )
            self._delete_directories(
                [
                    "relations_all_with_tags",
                    "relations_with_unnested_way_refs",
                    "relations_filtered_ids",
                    "required_ways_with_linestrings",
                    "valid_relation_parts",
                    "relation_inner_parts",
                    "relation_outer_parts",
                    "relation_outer_parts_with_holes",
                    "relation_outer_parts_without_holes",
                ],
            )

            parsed_geometries = self.connection.sql(
                f"""
                SELECT * FROM read_parquet([
                    '{filtered_nodes_with_geometry_path}/**',
                    '{filtered_ways_with_proper_geometry_path}/**',
                    '{filtered_relations_with_geometry_path}/**'
                ]);
                """
            )

            self._concatenate_results_to_geoparquet(
                parsed_geometries=parsed_geometries,
                save_file_path=result_file_path,
                keep_all_tags=keep_all_tags,
                explode_tags=explode_tags,
                save_as_wkt=save_as_wkt,
            )

            if not self.silent_mode:
                end_time = time.time()
                elapsed_seconds = end_time - start_time
                show_total_elapsed_time(elapsed_seconds)

        return result_file_path

    def _generate_result_file_path(
        self,
        pbf_file_path: Union[str, Path],
        keep_all_tags: bool,
        explode_tags: bool,
        filter_osm_ids: list[str],
        save_as_wkt: bool,
    ) -> Path:
        pbf_file_name = Path(pbf_file_path).name.removesuffix(".osm.pbf")

        osm_filter_tags_hash_part = "nofilter"
        if self.tags_filter is not None:
            keep_all_tags_part = "" if not keep_all_tags else "_alltags"
            h = hashlib.new("sha256")
            h.update(json.dumps(self.tags_filter).encode())
            osm_filter_tags_hash_part = f"{h.hexdigest()}{keep_all_tags_part}"

        clipping_geometry_hash_part = self._generate_geometry_hash()

        exploded_tags_part = "exploded" if explode_tags else "compact"

        filter_osm_ids_hash_part = ""
        if filter_osm_ids:
            h = hashlib.new("sha256")
            h.update(json.dumps(sorted(set(filter_osm_ids))).encode())
            filter_osm_ids_hash_part = f"_{h.hexdigest()}"

        if save_as_wkt:
            result_file_name = (
                f"{pbf_file_name}_{osm_filter_tags_hash_part}"
                f"_{clipping_geometry_hash_part}_{exploded_tags_part}{filter_osm_ids_hash_part}_wkt.parquet"
            )
        else:
            result_file_name = (
                f"{pbf_file_name}_{osm_filter_tags_hash_part}"
                f"_{clipping_geometry_hash_part}_{exploded_tags_part}{filter_osm_ids_hash_part}.geoparquet"
            )
        return Path(self.working_directory) / result_file_name

    def _generate_result_file_path_from_geometry(
        self, keep_all_tags: bool, explode_tags: bool, filter_osm_ids: list[str], save_as_wkt: bool
    ) -> Path:
        osm_filter_tags_hash_part = "nofilter"
        if self.tags_filter is not None:
            keep_all_tags_part = "" if not keep_all_tags else "_alltags"
            h = hashlib.new("sha256")
            h.update(json.dumps(self.tags_filter).encode())
            osm_filter_tags_hash_part = f"{h.hexdigest()}{keep_all_tags_part}"

        clipping_geometry_hash_part = self._generate_geometry_hash()

        exploded_tags_part = "exploded" if explode_tags else "compact"

        filter_osm_ids_hash_part = ""
        if filter_osm_ids:
            h = hashlib.new("sha256")
            h.update(json.dumps(sorted(set(filter_osm_ids))).encode())
            filter_osm_ids_hash_part = f"_{h.hexdigest()}"

        if save_as_wkt:
            result_file_name = (
                f"{clipping_geometry_hash_part}_{osm_filter_tags_hash_part}"
                f"_{exploded_tags_part}{filter_osm_ids_hash_part}_wkt.parquet"
            )
        else:
            result_file_name = (
                f"{clipping_geometry_hash_part}_{osm_filter_tags_hash_part}"
                f"_{exploded_tags_part}{filter_osm_ids_hash_part}.geoparquet"
            )
        return Path(self.working_directory) / result_file_name

    def _generate_geometry_hash(self) -> str:
        clipping_geometry_hash_part = "noclip"
        oriented_geometry = self._get_oriented_geometry_filter()
        if oriented_geometry is not None:
            h = hashlib.new("sha256")
            h.update(wktlib.dumps(oriented_geometry).encode())
            clipping_geometry_hash_part = h.hexdigest()

        return clipping_geometry_hash_part

    def _get_oriented_geometry_filter(
        self,
        geometry: Optional[BaseGeometry] = None,
    ) -> Optional[BaseGeometry]:
        if self.geometry_filter is None:
            return None

        if geometry is None:
            geometry = self.geometry_filter

        if isinstance(geometry, LinearRing):
            # https://stackoverflow.com/a/73073112/7766101
            new_coords = []
            perimeter = list(geometry.coords)
            smallest_point = sorted(perimeter)[0]
            double_iteration = itertools.chain(perimeter[:-1], perimeter)
            for point in double_iteration:
                if point == smallest_point:
                    new_coords.append((round(point[0], 7), round(point[1], 7)))
                    while len(new_coords) < len(perimeter):
                        next_point = next(double_iteration)
                        new_coords.append((round(next_point[0], 7), round(next_point[1], 7)))
                    break
            return LinearRing(new_coords)
        if isinstance(geometry, Polygon):
            oriented_exterior = self._get_oriented_geometry_filter(geometry.exterior)
            oriented_interiors = [
                cast(BaseGeometry, self._get_oriented_geometry_filter(interior))
                for interior in geometry.interiors
            ]
            return Polygon(
                oriented_exterior,
                sorted(oriented_interiors, key=lambda geom: (geom.centroid.x, geom.centroid.y)),
            )
        elif isinstance(geometry, BaseMultipartGeometry):
            oriented_geoms = [
                cast(BaseGeometry, self._get_oriented_geometry_filter(geom))
                for geom in geometry.geoms
            ]
            return geometry.__class__(
                sorted(oriented_geoms, key=lambda geom: (geom.centroid.x, geom.centroid.y))
            )

        return geometry

    def _prefilter_elements_ids(
        self, elements: "duckdb.DuckDBPyRelation", filter_osm_ids: list[str]
    ) -> ConvertedOSMParquetFiles:
        sql_filter = self._generate_osm_tags_sql_filter()
        filtered_tags_clause = self._generate_filtered_tags_clause()

        is_intersecting = self.geometry_filter is not None

        with TaskProgressSpinner("Reading nodes", "1", self.silent_mode):
            # NODES - VALID (NV)
            # - select all with kind = 'node'
            # - select all with lat and lon not empty
            nodes_valid_with_tags = self._sql_to_parquet_file(
                sql_query=f"""
                SELECT
                    id,
                    {filtered_tags_clause},
                    lon,
                    lat
                FROM ({elements.sql_query()})
                WHERE kind = 'node'
                AND lat IS NOT NULL AND lon IS NOT NULL
                """,
                file_path=self.tmp_dir_path / "nodes_valid_with_tags",
            )
        # NODES - INTERSECTING (NI)
        # - select all from NV which intersect given geometry filter
        # NODES - FILTERED (NF)
        # - select all from NI with tags filter
        filter_osm_node_ids_filter = self._generate_elements_filter(filter_osm_ids, "node")
        if is_intersecting:
            wkt = cast(BaseGeometry, self.geometry_filter).wkt
            intersection_filter = f"ST_Intersects(ST_Point(lon, lat), ST_GeomFromText('{wkt}'))"
            with TaskProgressSpinner("Filtering nodes - intersection", "2", self.silent_mode):
                nodes_intersecting_ids = self._sql_to_parquet_file(
                    sql_query=f"""
                    SELECT DISTINCT id FROM ({nodes_valid_with_tags.sql_query()}) n
                    WHERE {intersection_filter} = true
                    """,
                    file_path=self.tmp_dir_path / "nodes_intersecting_ids",
                )
            with TaskProgressSpinner("Filtering nodes - tags", "3", self.silent_mode):
                self._sql_to_parquet_file(
                    sql_query=f"""
                    SELECT id FROM ({nodes_valid_with_tags.sql_query()}) n
                    SEMI JOIN ({nodes_intersecting_ids.sql_query()}) ni ON n.id = ni.id
                    WHERE tags IS NOT NULL AND cardinality(tags) > 0 AND ({sql_filter})
                    AND ({filter_osm_node_ids_filter})
                    """,
                    file_path=self.tmp_dir_path / "nodes_filtered_non_distinct_ids",
                )
        else:
            with TaskProgressSpinner("Filtering nodes - intersection", "2", self.silent_mode):
                pass
            with TaskProgressSpinner("Filtering nodes - tags", "3", self.silent_mode):
                nodes_intersecting_ids = nodes_valid_with_tags
                self._sql_to_parquet_file(
                    sql_query=f"""
                    SELECT id FROM ({nodes_valid_with_tags.sql_query()}) n
                    WHERE tags IS NOT NULL AND cardinality(tags) > 0 AND ({sql_filter})
                    AND ({filter_osm_node_ids_filter})
                    """,
                    file_path=self.tmp_dir_path / "nodes_filtered_non_distinct_ids",
                )
        with TaskProgressSpinner("Calculating distinct filtered nodes ids", "4", self.silent_mode):
            nodes_filtered_ids = self._calculate_unique_ids_to_parquet(
                self.tmp_dir_path / "nodes_filtered_non_distinct_ids",
                self.tmp_dir_path / "nodes_filtered_ids",
            )

        with TaskProgressSpinner("Reading ways", "5", self.silent_mode):
            # WAYS - VALID (WV)
            # - select all with kind = 'way'
            # - select all with more then one ref
            # - join all NV to refs
            # - select all where all refs has been joined (total_refs == found_refs)
            self.connection.sql(
                f"""
                SELECT *
                FROM ({elements.sql_query()}) w
                WHERE kind = 'way' AND len(refs) >= 2
                """
            ).to_view("ways", replace=True)
            ways_all_with_tags = self._sql_to_parquet_file(
                sql_query=f"""
                WITH filtered_tags AS (
                    SELECT id, {filtered_tags_clause}, tags as raw_tags
                    FROM ways w
                    WHERE tags IS NOT NULL AND cardinality(tags) > 0
                )
                SELECT id, tags, raw_tags
                FROM filtered_tags
                WHERE tags IS NOT NULL AND cardinality(tags) > 0
                """,
                file_path=self.tmp_dir_path / "ways_all_with_tags",
            )
        with TaskProgressSpinner("Unnesting ways", "6", self.silent_mode):
            ways_with_unnested_nodes_refs = self._sql_to_parquet_file(
                sql_query="""
                SELECT w.id, UNNEST(refs) as ref, UNNEST(range(length(refs))) as ref_idx
                FROM ways w
                """,
                file_path=self.tmp_dir_path / "ways_with_unnested_nodes_refs",
            )
        with TaskProgressSpinner("Filtering ways - valid refs", "7", self.silent_mode):
            ways_valid_ids = self._sql_to_parquet_file(
                sql_query=f"""
                WITH total_ways_with_nodes_refs AS (
                    SELECT id
                    FROM ({ways_with_unnested_nodes_refs.sql_query()})
                ),
                unmatched_ways_with_nodes_refs AS (
                    SELECT id
                    FROM ({ways_with_unnested_nodes_refs.sql_query()}) w
                    ANTI JOIN ({nodes_valid_with_tags.sql_query()}) nv ON nv.id = w.ref
                )
                SELECT DISTINCT id
                FROM total_ways_with_nodes_refs
                ANTI JOIN unmatched_ways_with_nodes_refs USING (id)
                """,
                file_path=self.tmp_dir_path / "ways_valid_ids",
            )

        with TaskProgressSpinner("Filtering ways - intersection", "8", self.silent_mode):
            # WAYS - INTERSECTING (WI)
            # - select all from WV with joining any from NV on ref
            if is_intersecting:
                ways_intersecting_ids = self._sql_to_parquet_file(
                    sql_query=f"""
                    SELECT DISTINCT uwr.id
                    FROM ({ways_with_unnested_nodes_refs.sql_query()}) uwr
                    SEMI JOIN ({ways_valid_ids.sql_query()}) wv ON uwr.id = wv.id
                    SEMI JOIN ({nodes_intersecting_ids.sql_query()}) n ON n.id = uwr.ref
                    """,
                    file_path=self.tmp_dir_path / "ways_intersecting_ids",
                )
            else:
                ways_intersecting_ids = ways_valid_ids
        with TaskProgressSpinner("Filtering ways - tags", "9", self.silent_mode):
            # WAYS - FILTERED (WF)
            # - select all from WI with tags filter
            filter_osm_way_ids_filter = self._generate_elements_filter(filter_osm_ids, "way")
            self._sql_to_parquet_file(
                sql_query=f"""
                SELECT id FROM ({ways_all_with_tags.sql_query()}) w
                SEMI JOIN ({ways_intersecting_ids.sql_query()}) wi ON w.id = wi.id
                WHERE ({sql_filter}) AND ({filter_osm_way_ids_filter})
                """,
                file_path=self.tmp_dir_path / "ways_filtered_non_distinct_ids",
            )

        with TaskProgressSpinner("Calculating distinct filtered ways ids", "10", self.silent_mode):
            ways_filtered_ids = self._calculate_unique_ids_to_parquet(
                self.tmp_dir_path / "ways_filtered_non_distinct_ids",
                self.tmp_dir_path / "ways_filtered_ids",
            )

        with TaskProgressSpinner("Reading relations", "11", self.silent_mode):
            # RELATIONS - VALID (RV)
            # - select all with kind = 'relation'
            # - select all with more then one ref
            # - select all with type in ['boundary', 'multipolygon']
            # - join all WV to refs
            # - select all where all refs has been joined (total_refs == found_refs)
            self.connection.sql(
                f"""
                SELECT *
                FROM ({elements.sql_query()})
                WHERE kind = 'relation' AND len(refs) > 0
                AND list_contains(map_keys(tags), 'type')
                AND list_has_any(map_extract(tags, 'type'), ['boundary', 'multipolygon'])
                """
            ).to_view("relations", replace=True)
            relations_all_with_tags = self._sql_to_parquet_file(
                sql_query=f"""
                WITH filtered_tags AS (
                    SELECT id, {filtered_tags_clause}
                    FROM relations r
                    WHERE tags IS NOT NULL AND cardinality(tags) > 0
                )
                SELECT id, tags
                FROM filtered_tags
                WHERE tags IS NOT NULL AND cardinality(tags) > 0
                """,
                file_path=self.tmp_dir_path / "relations_all_with_tags",
            )

        with TaskProgressSpinner("Unnesting relations", "12", self.silent_mode):
            relations_with_unnested_way_refs = self._sql_to_parquet_file(
                sql_query="""
                WITH unnested_relation_refs AS (
                    SELECT
                        r.id,
                        UNNEST(refs) as ref,
                        UNNEST(ref_types) as ref_type,
                        UNNEST(ref_roles) as ref_role,
                        UNNEST(range(length(refs))) as ref_idx
                    FROM relations r
                )
                SELECT id, ref, ref_role, ref_idx
                FROM unnested_relation_refs
                WHERE ref_type = 'way'
                """,
                file_path=self.tmp_dir_path / "relations_with_unnested_way_refs",
            )

        with TaskProgressSpinner("Filtering relations - valid refs", "13", self.silent_mode):
            relations_valid_ids = self._sql_to_parquet_file(
                sql_query=f"""
                WITH total_relation_refs AS (
                    SELECT id
                    FROM ({relations_with_unnested_way_refs.sql_query()}) frr
                ),
                unmatched_relation_refs AS (
                    SELECT id
                    FROM ({relations_with_unnested_way_refs.sql_query()}) r
                    ANTI JOIN ({ways_valid_ids.sql_query()}) wv ON wv.id = r.ref
                )
                SELECT DISTINCT id
                FROM total_relation_refs
                ANTI JOIN unmatched_relation_refs USING (id)
                """,
                file_path=self.tmp_dir_path / "relations_valid_ids",
            )

        with TaskProgressSpinner("Filtering relations - intersection", "14", self.silent_mode):
            # RELATIONS - INTERSECTING (RI)
            # - select all from RW with joining any from RV on ref
            if is_intersecting:
                relations_intersecting_ids = self._sql_to_parquet_file(
                    sql_query=f"""
                    SELECT frr.id
                    FROM ({relations_with_unnested_way_refs.sql_query()}) frr
                    SEMI JOIN ({relations_valid_ids.sql_query()}) rv ON frr.id = rv.id
                    SEMI JOIN ({ways_intersecting_ids.sql_query()}) wi ON wi.id = frr.ref
                    """,
                    file_path=self.tmp_dir_path / "relations_intersecting_ids",
                )
            else:
                relations_intersecting_ids = relations_valid_ids

        with TaskProgressSpinner("Filtering relations - tags", "15", self.silent_mode):
            # RELATIONS - FILTERED (RF)
            # - select all from RI with tags filter
            filter_osm_relation_ids_filter = self._generate_elements_filter(
                filter_osm_ids, "relation"
            )

            relations_ids_path = self.tmp_dir_path / "relations_ids"
            relations_ids_path.mkdir(parents=True, exist_ok=True)
            self._sql_to_parquet_file(
                sql_query=f"""
                SELECT id FROM ({relations_all_with_tags.sql_query()}) r
                SEMI JOIN ({relations_intersecting_ids.sql_query()}) ri ON r.id = ri.id
                WHERE ({sql_filter}) AND ({filter_osm_relation_ids_filter})
                """,
                file_path=relations_ids_path / "filtered",
            )

        with TaskProgressSpinner(
            "Calculating distinct filtered relations ids", "16", self.silent_mode
        ):
            relations_filtered_ids = self._calculate_unique_ids_to_parquet(
                relations_ids_path / "filtered", self.tmp_dir_path / "relations_filtered_ids"
            )

        ways_prepared_ids_path = self.tmp_dir_path / "ways_prepared_ids"
        ways_prepared_ids_path.mkdir(parents=True, exist_ok=True)

        with TaskProgressSpinner("Loading required ways - by relations", "17", self.silent_mode):
            # WAYS - REQUIRED (WR)
            # - required - all IDs from WF
            #   + all needed to construct relations from RF
            self._sql_to_parquet_file(
                sql_query=f"""
                SELECT ref as id
                FROM ({relations_with_unnested_way_refs.sql_query()}) frr
                SEMI JOIN ({relations_filtered_ids.sql_query()}) fri ON fri.id = frr.id
                """,
                file_path=ways_prepared_ids_path / "required_by_relations",
            )

        with TaskProgressSpinner("Calculating distinct required ways ids", "18", self.silent_mode):
            ways_required_ids = self._calculate_unique_ids_to_parquet(
                ways_prepared_ids_path, self.tmp_dir_path / "ways_required_ids"
            )

        return PbfFileReader.ConvertedOSMParquetFiles(
            nodes_valid_with_tags=nodes_valid_with_tags,
            nodes_filtered_ids=nodes_filtered_ids,
            ways_all_with_tags=ways_all_with_tags,
            ways_with_unnested_nodes_refs=ways_with_unnested_nodes_refs,
            ways_required_ids=ways_required_ids,
            ways_filtered_ids=ways_filtered_ids,
            relations_all_with_tags=relations_all_with_tags,
            relations_with_unnested_way_refs=relations_with_unnested_way_refs,
            relations_filtered_ids=relations_filtered_ids,
        )

    def _delete_directories(self, directories: Union[str, Path, list[Union[str, Path]]]) -> None:
        _directories = []
        if isinstance(directories, (str, Path)):
            _directories = [directories]
        else:
            _directories = directories
        for directory in _directories:
            if isinstance(directory, Path):
                directory_path = directory
            else:
                directory_path = self.tmp_dir_path / directory
            tries = 100
            while directory_path.exists() and tries > 0:
                try:
                    shutil.rmtree(directory_path)
                except Exception as ex:
                    if tries == 0:
                        raise ex
                    sleep(0.5)
                finally:
                    tries -= 1

    def _generate_osm_tags_sql_filter(self) -> str:
        """Prepare features filter clauses based on tags filter."""
        filter_clauses = ["(1=1)"]

        if self.merged_tags_filter:
            filter_clauses.clear()

            for filter_tag_key, filter_tag_value in self.merged_tags_filter.items():
                if isinstance(filter_tag_value, bool) and filter_tag_value:
                    filter_clauses.append(f"(list_contains(map_keys(tags), '{filter_tag_key}'))")
                elif isinstance(filter_tag_value, str):
                    escaped_value = self._sql_escape(filter_tag_value)
                    filter_clauses.append(
                        f"list_extract(map_extract(tags, '{filter_tag_key}'), 1) ="
                        f" '{escaped_value}'"
                    )
                elif isinstance(filter_tag_value, list) and filter_tag_value:
                    values_list = [f"'{self._sql_escape(value)}'" for value in filter_tag_value]
                    filter_clauses.append(
                        f"list_extract(map_extract(tags, '{filter_tag_key}'), 1) IN"
                        f" ({', '.join(values_list)})"
                    )

        return " OR ".join(filter_clauses)

    def _generate_filtered_tags_clause(self) -> str:
        """Prepare filtered tags clause by removing tags commonly ignored by OGR."""
        tags_to_ignore = [
            "area",
            "created_by",
            "converted_by",
            "source",
            "time",
            "ele",
            "note",
            "todo",
            "fixme",
            "FIXME",
            "openGeoDB:",
        ]
        escaped_tags_to_ignore = [f"'{tag}'" for tag in tags_to_ignore]

        return f"""
        map_from_entries(
            [
                tag_entry
                for tag_entry in map_entries(tags)
                if not tag_entry.key in ({','.join(escaped_tags_to_ignore)})
                and not starts_with(tag_entry.key, 'openGeoDB:')
            ]
        ) as tags
        """

    def _generate_elements_filter(
        self, filter_osm_ids: list[str], element_type: Literal["node", "way", "relation"]
    ) -> str:
        filter_osm_relation_ids = [
            osm_id.replace(f"{element_type}/", "")
            for osm_id in filter_osm_ids
            if osm_id.startswith(f"{element_type}/")
        ]
        if not filter_osm_ids:
            filter_osm_ids_filter = "1=1"
        elif filter_osm_relation_ids:
            filter_osm_ids_filter = f"id in ({','.join(filter_osm_relation_ids)})"
        else:
            filter_osm_ids_filter = "id IS NULL"

        return filter_osm_ids_filter

    def _sql_escape(self, value: str) -> str:
        """Escape value for SQL query."""
        return value.replace("'", "''")

    def _sql_to_parquet_file(self, sql_query: str, file_path: Path) -> "duckdb.DuckDBPyRelation":
        relation = self.connection.sql(sql_query)
        return self._save_parquet_file(relation, file_path)

    def _save_parquet_file(
        self,
        relation: "duckdb.DuckDBPyRelation",
        file_path: Path,
        run_in_separate_process: bool = False,
    ) -> "duckdb.DuckDBPyRelation":
        query = f"""
            COPY (
                {relation.sql_query()}
            ) TO '{file_path}' (
                FORMAT 'parquet',
                PER_THREAD_OUTPUT true,
                ROW_GROUP_SIZE 25000,
                COMPRESSION '{self.parquet_compression}'
            )
        """
        self._run_query(query, run_in_separate_process)
        return self.connection.sql(f"SELECT * FROM read_parquet('{file_path}/**')")

    def _run_query(
        self,
        sql_queries: Union[str, list[str]],
        run_in_separate_process: bool = False,
        query_timeout_seconds: Optional[int] = None,
    ) -> None:
        if isinstance(sql_queries, str):
            sql_queries = [sql_queries]

        if run_in_separate_process:
            with Pool() as pool:
                r = pool.apply_async(_run_query, args=(sql_queries, self.tmp_dir_path))
                start_time = time.time()
                actual_memory = psutil.virtual_memory()
                percentage_threshold = 95
                if (actual_memory.total * 0.05) > MEMORY_1GB:
                    percentage_threshold = (
                        100 * (actual_memory.total - MEMORY_1GB) / actual_memory.total
                    )
                while not r.ready():
                    actual_memory = psutil.virtual_memory()
                    if actual_memory.percent > percentage_threshold:
                        raise MemoryError()

                    current_time = time.time()
                    elapsed_seconds = current_time - start_time
                    if (
                        query_timeout_seconds is not None
                        and elapsed_seconds > query_timeout_seconds
                    ):
                        raise TimeoutError()

                    sleep(0.5)
                r.get()
        else:
            for sql_query in sql_queries:
                self.connection.sql(sql_query)

    def _calculate_unique_ids_to_parquet(
        self, file_path: Path, result_path: Optional[Path] = None
    ) -> "duckdb.DuckDBPyRelation":
        if result_path is None:
            result_path = file_path / "distinct"

        self.connection.sql(
            f"""
            COPY (
                SELECT id FROM read_parquet('{file_path}/**') GROUP BY id
            ) TO '{result_path}' (
                FORMAT 'parquet',
                PER_THREAD_OUTPUT true,
                ROW_GROUP_SIZE 25000,
                COMPRESSION '{self.parquet_compression}'
            )
            """
        )

        return self.connection.sql(f"SELECT * FROM read_parquet('{result_path}/**')")

    def _get_filtered_nodes_with_geometry(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
    ) -> Path:
        nodes_with_geometry = self.connection.sql(
            f"""
            SELECT
                'node/' || n.id as feature_id,
                n.tags,
                ST_Point(round(n.lon, 7), round(n.lat, 7)) geometry
            FROM ({osm_parquet_files.nodes_valid_with_tags.sql_query()}) n
            SEMI JOIN ({osm_parquet_files.nodes_filtered_ids.sql_query()}) fn ON n.id = fn.id
            """
        )
        result_path = self.tmp_dir_path / "filtered_nodes_with_geometry"
        self._save_parquet_file_with_geometry(
            relation=nodes_with_geometry,
            file_path=result_path,
            step_name="Saving filtered nodes with geometries",
            step_number="19",
        )
        return result_path

    def _get_ways_refs_with_nodes_structs(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
    ) -> "duckdb.DuckDBPyRelation":
        ways_refs_with_nodes_structs = self.connection.sql(
            f"""
            SELECT
                w.id,
                w.ref,
                w.ref_idx,
                struct_pack(x := round(n.lon, 7), y := round(n.lat, 7))::POINT_2D point
            FROM ({osm_parquet_files.nodes_valid_with_tags.sql_query()}) n
            JOIN ({osm_parquet_files.ways_with_unnested_nodes_refs.sql_query()}) w ON w.ref = n.id
            """
        )
        with TaskProgressSpinner("Saving required nodes with structs", "20", self.silent_mode):
            ways_refs_parquet = self._save_parquet_file(
                relation=ways_refs_with_nodes_structs,
                file_path=self.tmp_dir_path / "ways_refs_with_nodes_structs",
            )
        return ways_refs_parquet

    def _get_filtered_ways_with_linestrings(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
    ) -> "duckdb.DuckDBPyRelation":
        grouped_ways_path = self.tmp_dir_path / "filtered_ways_grouped"
        grouped_ways_tmp_path = self.tmp_dir_path / "filtered_ways_tmp"
        destination_dir_path = self.tmp_dir_path / "filtered_ways_with_linestrings"

        return self._get_ways_with_linestrings(
            ways_ids=osm_parquet_files.ways_filtered_ids,
            mode="filtered",
            step_number=20,
            osm_parquet_files=osm_parquet_files,
            destination_dir_path=destination_dir_path,
            grouped_ways_path=grouped_ways_path,
            grouped_ways_tmp_path=grouped_ways_tmp_path,
        )

    def _get_required_ways_with_linestrings(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
    ) -> "duckdb.DuckDBPyRelation":
        grouped_ways_path = self.tmp_dir_path / "required_ways_grouped"
        grouped_ways_tmp_path = self.tmp_dir_path / "required_ways_tmp"
        destination_dir_path = self.tmp_dir_path / "required_ways_with_linestrings"

        return self._get_ways_with_linestrings(
            ways_ids=osm_parquet_files.ways_required_ids,
            mode="required",
            step_number=22,
            osm_parquet_files=osm_parquet_files,
            destination_dir_path=destination_dir_path,
            grouped_ways_path=grouped_ways_path,
            grouped_ways_tmp_path=grouped_ways_tmp_path,
        )

    def _get_ways_with_linestrings(
        self,
        ways_ids: "duckdb.DuckDBPyRelation",
        mode: Literal["filtered", "required"],
        step_number: int,
        osm_parquet_files: ConvertedOSMParquetFiles,
        destination_dir_path: Path,
        grouped_ways_tmp_path: Path,
        grouped_ways_path: Path,
    ) -> "duckdb.DuckDBPyRelation":
        finished_operation = False

        while not finished_operation:
            try:
                if not self.encountered_query_exception:
                    groups = self._group_ways(
                        ways_ids=ways_ids,
                        mode=mode,
                        step_number=step_number,
                        osm_parquet_files=osm_parquet_files,
                        destination_dir_path=destination_dir_path,
                        grouped_ways_tmp_path=grouped_ways_tmp_path,
                        grouped_ways_path=grouped_ways_path,
                        group_all_at_once=True,
                    )
                else:
                    groups = self._group_ways(
                        ways_ids=ways_ids,
                        mode=mode,
                        step_number=step_number,
                        osm_parquet_files=osm_parquet_files,
                        destination_dir_path=destination_dir_path,
                        grouped_ways_tmp_path=grouped_ways_tmp_path,
                        grouped_ways_path=grouped_ways_path,
                        group_all_at_once=False,
                    )

                self._delete_directories(grouped_ways_tmp_path)

                with TaskProgressBar(
                    f"Saving {mode} ways with linestrings", str(step_number + 1), self.silent_mode
                ) as bar:
                    self._construct_ways_linestrings(
                        bar=bar,
                        groups=groups,
                        destination_dir_path=destination_dir_path,
                        grouped_ways_path=grouped_ways_path,
                    )

                finished_operation = True
            except (duckdb.OutOfMemoryException, MemoryError, TimeoutError) as ex:
                self.encountered_query_exception = True
                if self.rows_per_group > PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[0]:
                    self._delete_directories(
                        [destination_dir_path, grouped_ways_tmp_path, grouped_ways_path]
                    )
                    smaller_rows_per_group = 0
                    for rows_per_group in PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG.values():
                        if rows_per_group < self.rows_per_group:
                            smaller_rows_per_group = rows_per_group
                        else:
                            break
                    self.rows_per_group = smaller_rows_per_group
                    if not self.silent_mode:
                        log_message(
                            f"Encountered {ex.__class__.__name__} during operation."
                            " Retrying with lower number of rows per group"
                            f" ({self.rows_per_group})."
                        )
                else:
                    raise

        ways_parquet = self.connection.sql(
            f"SELECT * FROM read_parquet('{destination_dir_path}/**')"
        )
        return ways_parquet

    def _group_ways(
        self,
        ways_ids: "duckdb.DuckDBPyRelation",
        osm_parquet_files: ConvertedOSMParquetFiles,
        destination_dir_path: Path,
        grouped_ways_tmp_path: Path,
        grouped_ways_path: Path,
        mode: Literal["filtered", "required"],
        step_number: int,
        group_all_at_once: bool = True,
    ) -> int:
        total_required_ways = ways_ids.count("id").fetchone()[0]

        destination_dir_path.mkdir(parents=True, exist_ok=True)
        grouped_ways_tmp_path.mkdir(parents=True, exist_ok=True)

        if total_required_ways == 0:
            empty_file_path = str(destination_dir_path / "empty.parquet")
            self.connection.sql("CREATE OR REPLACE TABLE x(id STRING, linestring LINESTRING_2D);")
            self.connection.table("x").to_parquet(empty_file_path)
            return -1

        groups = int(floor(total_required_ways / self.rows_per_group))

        grouped_ways_ids_with_group_path = grouped_ways_tmp_path / "ids_with_group"
        grouped_ways_ids_with_points_path = grouped_ways_tmp_path / "ids_with_points"

        with TaskProgressSpinner(
            f"Grouping {mode} ways - assigning groups", f"{step_number}.1", self.silent_mode
        ):
            ways_ids_grouped_relation = self.connection.sql(
                f"""
                SELECT id,
                    floor(
                        row_number() OVER () / {self.rows_per_group}
                    )::INTEGER as "group",
                FROM ({ways_ids.sql_query()})
                """
            )
            ways_ids_grouped_relation_parquet = self._save_parquet_file(
                relation=ways_ids_grouped_relation, file_path=grouped_ways_ids_with_group_path
            )

        if group_all_at_once:
            with TaskProgressSpinner(
                f"Grouping {mode} ways - joining with nodes", f"{step_number}.2", self.silent_mode
            ):
                ways_with_nodes_points_relation = self.connection.sql(
                    f"""
                    SELECT
                        w.id,
                        struct_pack(x := round(n.lon, 7), y := round(n.lat, 7))::POINT_2D point,
                        w.ref_idx,
                        rw."group"
                    FROM ({ways_ids_grouped_relation_parquet.sql_query()}) rw
                    JOIN ({osm_parquet_files.ways_with_unnested_nodes_refs.sql_query()}) w
                    ON rw.id = w.id
                    JOIN ({osm_parquet_files.nodes_valid_with_tags.sql_query()}) n
                    ON w.ref = n.id
                    """
                )

                ways_with_nodes_points_relation_parquet = self._save_parquet_file(
                    relation=ways_with_nodes_points_relation,
                    file_path=grouped_ways_ids_with_points_path,
                    run_in_separate_process=(
                        self.rows_per_group > PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[0]
                    ),
                )
        else:
            ways_ids_grouped_files = list(grouped_ways_ids_with_group_path.glob("**/*.parquet"))
            ways_with_unnested_nodes_refs_files = list(
                (self.tmp_dir_path / "ways_with_unnested_nodes_refs").glob("**/*.parquet")
            )
            with TaskProgressBar(
                f"Grouping {mode} ways - joining with nodes", f"{step_number}.2", self.silent_mode
            ) as bar:
                for current_step, (
                    ways_ids_grouped_parquet_file,
                    ways_with_unnested_nodes_refs_parquet_file,
                ) in bar.track(
                    enumerate(
                        itertools.product(
                            ways_ids_grouped_files,
                            ways_with_unnested_nodes_refs_files,
                        )
                    )
                ):
                    current_grouped_ways_ids_with_points_path = (
                        grouped_ways_ids_with_points_path / str(current_step)
                    )
                    current_grouped_ways_ids_with_points_path.mkdir(parents=True, exist_ok=True)

                    ways_with_nodes_points_relation = self.connection.sql(
                        f"""
                        SELECT
                            w.id,
                            struct_pack(x := round(n.lon, 7), y := round(n.lat, 7))::POINT_2D point,
                            w.ref_idx,
                            rw."group"
                        FROM read_parquet('{ways_ids_grouped_parquet_file}') rw
                        JOIN read_parquet('{ways_with_unnested_nodes_refs_parquet_file}') w
                        ON rw.id = w.id
                        JOIN ({osm_parquet_files.nodes_valid_with_tags.sql_query()}) n
                        ON w.ref = n.id
                        """
                    )

                    self._save_parquet_file(
                        relation=ways_with_nodes_points_relation,
                        file_path=current_grouped_ways_ids_with_points_path,
                        run_in_separate_process=(
                            self.rows_per_group > PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[0]
                        ),
                    )

            ways_with_nodes_points_relation_parquet = self.connection.sql(
                f"SELECT * FROM read_parquet('{grouped_ways_ids_with_points_path}/**')"
            )

        with TaskProgressSpinner(
            f"Grouping {mode} ways - partitioning by group", f"{step_number}.3", self.silent_mode
        ):
            self.connection.sql(
                f"""
                COPY (
                    SELECT
                        id, point, ref_idx, "group"
                    FROM ({ways_with_nodes_points_relation_parquet.sql_query()}) w
                ) TO '{grouped_ways_path}' (
                    FORMAT 'parquet',
                    PARTITION_BY ("group"),
                    ROW_GROUP_SIZE 25000,
                    COMPRESSION '{self.parquet_compression}'
                )
                """
            )

        return groups

    def _construct_ways_linestrings(
        self,
        bar: TaskProgressBar,
        groups: int,
        destination_dir_path: Path,
        grouped_ways_path: Path,
    ) -> None:
        for group in bar.track(range(groups + 1)):
            current_ways_group_path = grouped_ways_path / f"group={group}"
            current_destination_path = destination_dir_path / f"group={group}"

            temp_table_name = f"tbl{secrets.token_hex(16)}"
            create_temp_table_query = f"""
                CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
                FROM read_parquet('{current_ways_group_path}/**')
            """
            group_ways_query = f"""
                COPY (
                    SELECT id, list(point ORDER BY ref_idx ASC)::LINESTRING_2D linestring
                    FROM {temp_table_name}
                    GROUP BY id
                ) TO '{current_destination_path}' (
                    FORMAT 'parquet',
                    PER_THREAD_OUTPUT true,
                    ROW_GROUP_SIZE 25000,
                    COMPRESSION '{self.parquet_compression}'
                )
            """
            drop_table_query = f"DROP TABLE {temp_table_name}"
            queries = [create_temp_table_query, group_ways_query, drop_table_query]

            self._run_query(
                queries,
                run_in_separate_process=(
                    self.rows_per_group > PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[0]
                ),
            )

            self._delete_directories(current_ways_group_path)

    def _get_filtered_ways_with_proper_geometry(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
        required_ways_with_linestrings: "duckdb.DuckDBPyRelation",
    ) -> "duckdb.DuckDBPyRelation":
        osm_way_polygon_features_filter_clauses = [
            "list_contains(map_keys(raw_tags), 'area') AND "
            "list_extract(map_extract(raw_tags, 'area'), 1) = 'yes'"
        ]

        for osm_tag_key in self.osm_way_polygon_features_config.all:
            osm_way_polygon_features_filter_clauses.append(
                f"list_contains(map_keys(raw_tags), '{osm_tag_key}')"
            )

        for osm_tag_key, osm_tag_values in self.osm_way_polygon_features_config.allowlist.items():
            escaped_values = ",".join(
                [f"'{self._sql_escape(osm_tag_value)}'" for osm_tag_value in osm_tag_values]
            )
            osm_way_polygon_features_filter_clauses.append(
                f"list_contains(map_keys(raw_tags), '{osm_tag_key}') AND"
                f" list_has_any(map_extract(raw_tags, '{osm_tag_key}'), [{escaped_values}])"
            )

        for osm_tag_key, osm_tag_values in self.osm_way_polygon_features_config.denylist.items():
            escaped_values = ",".join(
                [f"'{self._sql_escape(osm_tag_value)}'" for osm_tag_value in osm_tag_values]
            )
            osm_way_polygon_features_filter_clauses.append(
                f"list_contains(map_keys(raw_tags), '{osm_tag_key}') AND NOT"
                f" list_has_any(map_extract(raw_tags, '{osm_tag_key}'), [{escaped_values}])"
            )

        ways_with_proper_geometry = self.connection.sql(
            f"""
            WITH required_ways_with_linestrings AS (
                SELECT
                    w.id,
                    w.tags,
                    w_l.linestring,
                    -- Filter below is based on `_is_closed_way_a_polygon` function from OSMnx
                    -- Filter values are built dynamically from a config.
                    (
                        -- if first and last nodes are the same
                        ST_Equals(linestring[1]::POINT_2D, linestring[-1]::POINT_2D)
                        -- if the element doesn't have any tags leave it as a Linestring
                        AND raw_tags IS NOT NULL
                        -- if the element is specifically tagged 'area':'no' -> LineString
                        AND NOT (
                            list_contains(map_keys(raw_tags), 'area')
                            AND list_extract(map_extract(raw_tags, 'area'), 1) = 'no'
                        )
                        AND ({' OR '.join(osm_way_polygon_features_filter_clauses)})
                    ) AS is_polygon
                FROM ({required_ways_with_linestrings.sql_query()}) w_l
                SEMI JOIN ({osm_parquet_files.ways_filtered_ids.sql_query()}) fw ON w_l.id = fw.id
                JOIN ({osm_parquet_files.ways_all_with_tags.sql_query()}) w ON w.id = w_l.id
            ),
            proper_geometries AS (
                SELECT
                    id,
                    tags,
                    (CASE
                        WHEN is_polygon
                        THEN linestring_to_polygon_wkt(linestring)
                        ELSE linestring_to_linestring_wkt(linestring)
                    END)::GEOMETRY AS geometry
                FROM
                    required_ways_with_linestrings w
            )
            SELECT 'way/' || id as feature_id, tags, geometry FROM proper_geometries
            """
        )
        result_path = self.tmp_dir_path / "filtered_ways_with_geometry"
        self._save_parquet_file_with_geometry(
            relation=ways_with_proper_geometry,
            file_path=result_path,
            step_name="Saving filtered ways with geometries",
            step_number="24",
        )
        return result_path

    def _get_filtered_relations_with_geometry(
        self,
        osm_parquet_files: ConvertedOSMParquetFiles,
        required_ways_with_linestrings: "duckdb.DuckDBPyRelation",
    ) -> Path:
        valid_relation_parts = self.connection.sql(
            f"""
            WITH unnested_relations AS (
                SELECT
                    r.id,
                    COALESCE(r.ref_role, 'outer') as ref_role,
                    r.ref,
                    linestring_to_linestring_wkt(w.linestring)::GEOMETRY as geometry
                FROM ({osm_parquet_files.relations_with_unnested_way_refs.sql_query()}) r
                SEMI JOIN ({osm_parquet_files.relations_filtered_ids.sql_query()}) fr
                ON r.id = fr.id
                JOIN ({required_ways_with_linestrings.sql_query()}) w
                ON w.id = r.ref
            ),
            any_outer_refs AS (
                SELECT id, bool_or(ref_role == 'outer') any_outer_refs
                FROM unnested_relations
                GROUP BY id
            ),
            relations_with_geometries AS (
                SELECT
                    x.id,
                    CASE WHEN aor.any_outer_refs
                        THEN x.ref_role ELSE 'outer'
                    END as ref_role,
                    x.geom geometry,
                    row_number() OVER (PARTITION BY x.id) as geometry_id
                FROM (
                    SELECT
                        id,
                        ref_role,
                        UNNEST(
                            ST_Dump(ST_LineMerge(ST_Collect(list(geometry)))), recursive := true
                        ),
                    FROM unnested_relations
                    GROUP BY id, ref_role
                ) x
                JOIN any_outer_refs aor ON aor.id = x.id
                WHERE ST_NPoints(geom) >= 4
            ),
            valid_relations AS (
                SELECT id, is_valid
                FROM (
                    SELECT
                        id,
                        bool_and(
                            ST_Equals(ST_StartPoint(geometry), ST_EndPoint(geometry))
                        ) is_valid
                    FROM relations_with_geometries
                    GROUP BY id
                )
                WHERE is_valid = true
            )
            SELECT * FROM relations_with_geometries
            SEMI JOIN valid_relations ON relations_with_geometries.id = valid_relations.id
            """
        )
        valid_relation_parts_parquet = self._save_parquet_file_with_geometry(
            relation=valid_relation_parts,
            file_path=self.tmp_dir_path / "valid_relation_parts",
            step_name="Saving valid relations parts",
            step_number="25",
        )
        relation_inner_parts = self.connection.sql(
            f"""
            SELECT id, geometry_id, ST_MakePolygon(geometry) geometry
            FROM ({valid_relation_parts_parquet.sql_query()})
            WHERE ref_role = 'inner'
            """
        )
        relation_inner_parts_parquet = self._save_parquet_file_with_geometry(
            relation=relation_inner_parts,
            file_path=self.tmp_dir_path / "relation_inner_parts",
            fix_geometries=True,
            step_name="Saving relations inner parts",
            step_number="26",
        )
        relation_outer_parts = self.connection.sql(
            f"""
            SELECT id, geometry_id, ST_MakePolygon(geometry) geometry
            FROM ({valid_relation_parts_parquet.sql_query()})
            WHERE ref_role = 'outer'
            """
        )
        relation_outer_parts_parquet = self._save_parquet_file_with_geometry(
            relation=relation_outer_parts,
            file_path=self.tmp_dir_path / "relation_outer_parts",
            fix_geometries=True,
            step_name="Saving relations outer parts",
            step_number="27",
        )
        relation_outer_parts_with_holes = self.connection.sql(
            f"""
            SELECT
                og.id,
                og.geometry_id,
                ST_Difference(any_value(og.geometry), ST_Union_Agg(ig.geometry)) geometry
            FROM ({relation_outer_parts_parquet.sql_query()}) og
            JOIN ({relation_inner_parts_parquet.sql_query()}) ig
            ON og.id = ig.id AND ST_WITHIN(ig.geometry, og.geometry)
            GROUP BY og.id, og.geometry_id
            """
        )
        relation_outer_parts_with_holes_parquet = self._save_parquet_file_with_geometry(
            relation=relation_outer_parts_with_holes,
            file_path=self.tmp_dir_path / "relation_outer_parts_with_holes",
            step_name="Saving relations outer parts with holes",
            step_number="28",
        )
        relation_outer_parts_without_holes = self.connection.sql(
            f"""
            SELECT
                og.id,
                og.geometry_id,
                og.geometry
            FROM ({relation_outer_parts_parquet.sql_query()}) og
            ANTI JOIN ({relation_outer_parts_with_holes_parquet.sql_query()}) ogwh
            ON og.id = ogwh.id AND og.geometry_id = ogwh.geometry_id
            """
        )
        relation_outer_parts_without_holes_parquet = self._save_parquet_file_with_geometry(
            relation=relation_outer_parts_without_holes,
            file_path=self.tmp_dir_path / "relation_outer_parts_without_holes",
            step_name="Saving relations outer parts without holes",
            step_number="29",
        )
        relations_with_geometry = self.connection.sql(
            f"""
            WITH unioned_outer_geometries AS (
                SELECT id, geometry
                FROM ({relation_outer_parts_with_holes_parquet.sql_query()})
                UNION ALL
                SELECT id, geometry
                FROM ({relation_outer_parts_without_holes_parquet.sql_query()})
            ),
            final_geometries AS (
                SELECT id, ST_Union_Agg(geometry) geometry
                FROM unioned_outer_geometries
                GROUP BY id
            )
            SELECT 'relation/' || r_g.id as feature_id, r.tags, r_g.geometry
            FROM final_geometries r_g
            JOIN ({osm_parquet_files.relations_all_with_tags.sql_query()}) r
            ON r.id = r_g.id
            """
        )

        result_path = self.tmp_dir_path / "filtered_relations_with_geometry"
        self._save_parquet_file_with_geometry(
            relation=relations_with_geometry,
            file_path=result_path,
            step_name="Saving filtered relations with geometries",
            step_number="30",
        )
        return result_path

    def _save_parquet_file_with_geometry(
        self,
        relation: "duckdb.DuckDBPyRelation",
        file_path: Path,
        step_name: str,
        step_number: str,
        fix_geometries: bool = False,
    ) -> "duckdb.DuckDBPyRelation":
        if not fix_geometries:
            with TaskProgressSpinner(step_name, step_number, self.silent_mode):
                self.connection.sql(
                    f"""
                    COPY (
                        SELECT
                            * EXCLUDE (geometry), ST_AsWKB(geometry) geometry_wkb
                        FROM ({relation.sql_query()})
                    ) TO '{file_path}' (
                        FORMAT 'parquet',
                        PER_THREAD_OUTPUT true,
                        ROW_GROUP_SIZE 25000,
                        COMPRESSION '{self.parquet_compression}'
                    )
                    """
                )
        else:
            valid_path = file_path / "valid"
            invalid_path = file_path / "invalid"
            fixed_path = file_path / "fixed"

            valid_path.mkdir(parents=True, exist_ok=True)
            invalid_path.mkdir(parents=True, exist_ok=True)
            fixed_path.mkdir(parents=True, exist_ok=True)

            # Save valid features
            with TaskProgressSpinner(
                f"{step_name} - valid geometries", f"{step_number}.1", self.silent_mode
            ):
                self.connection.sql(
                    f"""
                    COPY (
                        SELECT
                            * EXCLUDE (geometry), ST_AsWKB(geometry) geometry_wkb
                        FROM ({relation.sql_query()})
                        WHERE ST_IsValid(geometry)
                    ) TO '{valid_path}' (
                        FORMAT 'parquet',
                        PER_THREAD_OUTPUT true,
                        ROW_GROUP_SIZE 25000,
                        COMPRESSION '{self.parquet_compression}'
                    )
                    """
                )

            # Save invalid features
            with TaskProgressSpinner(
                f"{step_name} - invalid geometries", f"{step_number}.2", self.silent_mode
            ):
                self.connection.sql(
                    f"""
                    COPY (
                        SELECT
                            * EXCLUDE (geometry), ST_AsWKB(geometry) geometry_wkb,
                            floor(
                                row_number() OVER () / {self.rows_per_group}
                            )::INTEGER as "group",
                        FROM ({relation.sql_query()})
                        WHERE NOT ST_IsValid(geometry)
                    ) TO '{invalid_path}' (
                        FORMAT 'parquet',
                        PARTITION_BY ("group"),
                        ROW_GROUP_SIZE 25000,
                        COMPRESSION '{self.parquet_compression}'
                    )
                    """
                )

            # Fix invalid features
            total_groups = 0
            while (invalid_path / f"group={total_groups}").exists():
                total_groups += 1

            if total_groups > 0:
                with TaskProgressBar(
                    f"{step_name} - fixing invalid geometries", f"{step_number}.3", self.silent_mode
                ) as bar:
                    for group_id in bar.track(range(total_groups)):
                        current_invalid_features_group_path = invalid_path / f"group={group_id}"
                        current_invalid_features_group_table = pq.read_table(
                            current_invalid_features_group_path
                        ).drop("group")
                        valid_geometry_column = ga.as_wkb(
                            ga.to_geopandas(
                                ga.with_crs(
                                    current_invalid_features_group_table.column("geometry_wkb"),
                                    WGS84_CRS,
                                )
                            ).make_valid(),
                        )
                        current_invalid_features_group_table = (
                            current_invalid_features_group_table.drop("geometry_wkb")
                        )

                        current_invalid_features_group_table = (
                            current_invalid_features_group_table.append_column(
                                "geometry_wkb", valid_geometry_column
                            )
                        )
                        pq.write_table(
                            current_invalid_features_group_table,
                            fixed_path / f"data_{group_id}.parquet",
                        )

            self._delete_directories(invalid_path)

        return self.connection.sql(
            f"""
            SELECT * EXCLUDE (geometry_wkb), ST_GeomFromWKB(geometry_wkb) geometry
            FROM read_parquet('{file_path}/**')
            """
        )

    def _concatenate_results_to_geoparquet(
        self,
        parsed_geometries: "duckdb.DuckDBPyRelation",
        save_file_path: Path,
        keep_all_tags: bool,
        explode_tags: bool,
        save_as_wkt: bool,
    ) -> None:
        select_clauses = [
            "feature_id",
            *self._generate_osm_tags_sql_select(
                parsed_geometries, keep_all_tags=keep_all_tags, explode_tags=explode_tags
            ),
            "ST_GeomFromWKB(geometry_wkb) AS geometry",
        ]

        unioned_features = self.connection.sql(
            f"""
            SELECT {', '.join(select_clauses)}
            FROM ({parsed_geometries.sql_query()})
            """
        )

        grouped_features = self._parse_features_relation_to_groups(
            unioned_features, keep_all_tags=keep_all_tags, explode_tags=explode_tags
        )

        valid_features_full_relation = self.connection.sql(
            f"""
            SELECT * FROM ({grouped_features.sql_query()})
            WHERE ST_IsValid(geometry)
            """
        )

        valid_features_parquet_path = self.tmp_dir_path / "osm_valid_elements"
        valid_features_parquet_relation = self._save_parquet_file_with_geometry(
            valid_features_full_relation,
            valid_features_parquet_path,
            step_name="Saving valid features",
            step_number="31.1",
        )

        valid_features_parquet_table = pq.read_table(valid_features_parquet_path)

        is_empty = valid_features_parquet_table.num_rows == 0

        if not is_empty and save_as_wkt:
            geometry_column = ga.as_wkt(
                ga.with_crs(valid_features_parquet_table.column("geometry_wkb"), WGS84_CRS)
            )
        elif not is_empty:
            geometry_column = ga.as_wkb(
                ga.with_crs(valid_features_parquet_table.column("geometry_wkb"), WGS84_CRS)
            )
        elif save_as_wkt:
            geometry_column = ga.as_wkt(gpd.GeoSeries([], crs=WGS84_CRS))
        else:
            geometry_column = ga.as_wkb(gpd.GeoSeries([], crs=WGS84_CRS))

        valid_features_parquet_table = valid_features_parquet_table.append_column(
            GEOMETRY_COLUMN, geometry_column
        ).drop("geometry_wkb")

        parquet_tables = [valid_features_parquet_table]

        invalid_features_full_relation = self.connection.sql(
            f"""
            SELECT * FROM ({grouped_features.sql_query()}) a
            ANTI JOIN ({valid_features_parquet_relation.sql_query()}) b
            ON a.feature_id = b.feature_id
            """
        )

        total_features = parsed_geometries.count("feature_id").fetchone()[0]

        valid_features = valid_features_parquet_relation.count("feature_id").fetchone()[0]

        invalid_features = total_features - valid_features

        if invalid_features > 0:
            with TaskProgressSpinner("Grouping invalid features", "31.2", self.silent_mode):
                groups = floor(invalid_features / self.rows_per_group)
                grouped_invalid_features_result_parquet = (
                    self.tmp_dir_path / "osm_invalid_elements_grouped"
                )
                self.connection.sql(
                    f"""
                    COPY (
                        SELECT
                            * EXCLUDE (geometry), ST_AsWKB(geometry) geometry_wkb,
                            floor(
                                row_number() OVER () / {self.rows_per_group}
                            )::INTEGER as "group",
                        FROM ({invalid_features_full_relation.sql_query()})
                    ) TO '{grouped_invalid_features_result_parquet}' (
                        FORMAT 'parquet',
                        PARTITION_BY ("group"),
                        ROW_GROUP_SIZE 25000,
                        COMPRESSION '{self.parquet_compression}'
                    )
                    """
                )

            with TaskProgressBar("Fixing invalid features", "31.3", self.silent_mode) as bar:
                for group in bar.track(range(groups + 1)):
                    current_invalid_features_group_path = (
                        grouped_invalid_features_result_parquet / f"group={group}"
                    )
                    current_invalid_features_group_table = pq.read_table(
                        current_invalid_features_group_path
                    ).drop("group")
                    if save_as_wkt:
                        valid_geometry_column = ga.as_wkt(
                            ga.to_geopandas(
                                ga.with_crs(
                                    current_invalid_features_group_table.column("geometry_wkb"),
                                    WGS84_CRS,
                                )
                            ).make_valid()
                        )
                    else:
                        valid_geometry_column = ga.as_wkb(
                            ga.to_geopandas(
                                ga.with_crs(
                                    current_invalid_features_group_table.column("geometry_wkb"),
                                    WGS84_CRS,
                                )
                            ).make_valid()
                        )

                    current_invalid_features_group_table = (
                        current_invalid_features_group_table.append_column(
                            GEOMETRY_COLUMN, valid_geometry_column
                        )
                    )
                    current_invalid_features_group_table = (
                        current_invalid_features_group_table.drop("geometry_wkb")
                    )
                    parquet_tables.append(current_invalid_features_group_table)

        joined_parquet_table: pa.Table = pa.concat_tables(parquet_tables)

        is_empty = joined_parquet_table.num_rows == 0

        empty_columns = []
        for column_name in joined_parquet_table.column_names:
            if column_name in (FEATURES_INDEX, GEOMETRY_COLUMN):
                continue
            if (
                is_empty
                or pa.compute.all(
                    pa.compute.is_null(joined_parquet_table.column(column_name))
                ).as_py()
            ):
                empty_columns.append(column_name)

        if empty_columns:
            joined_parquet_table = joined_parquet_table.drop(empty_columns)

        if save_as_wkt:
            with TaskProgressSpinner("Saving final parquet file", "32", self.silent_mode):
                pq.write_table(joined_parquet_table, save_file_path)
        else:
            with TaskProgressSpinner("Saving final geoparquet file", "32", self.silent_mode):
                io.write_geoparquet_table(
                    joined_parquet_table, save_file_path, primary_geometry_column=GEOMETRY_COLUMN
                )

    def _generate_osm_tags_sql_select(
        self, parsed_geometries: "duckdb.DuckDBPyRelation", keep_all_tags: bool, explode_tags: bool
    ) -> list[str]:
        """Prepare features filter clauses based on tags filter."""
        osm_tag_keys_select_clauses = []

        no_tags_filter = not self.merged_tags_filter
        tags_filter_and_keep_all_tags = self.merged_tags_filter and keep_all_tags
        keep_tags_compact = not explode_tags

        if (no_tags_filter and keep_tags_compact) or (
            tags_filter_and_keep_all_tags and keep_tags_compact
        ):
            osm_tag_keys_select_clauses = ["tags"]
        elif (no_tags_filter and explode_tags) or (tags_filter_and_keep_all_tags and explode_tags):
            osm_tag_keys = set()
            found_tag_keys = [
                row[0]
                for row in self.connection.sql(
                    f"""
                    SELECT DISTINCT UNNEST(map_keys(tags)) tag_key
                    FROM ({parsed_geometries.sql_query()})
                    """
                ).fetchall()
            ]
            osm_tag_keys.update(found_tag_keys)
            osm_tag_keys_select_clauses = [
                f"list_extract(map_extract(tags, '{osm_tag_key}'), 1) as \"{osm_tag_key}\""
                for osm_tag_key in sorted(list(osm_tag_keys))
            ]
        elif self.merged_tags_filter and keep_tags_compact:
            filter_tag_clauses = []
            for filter_tag_key, filter_tag_value in self.merged_tags_filter.items():
                if isinstance(filter_tag_value, bool) and filter_tag_value:
                    filter_tag_clauses.append(f"tag_entry.key = '{filter_tag_key}'")
                elif isinstance(filter_tag_value, str):
                    escaped_value = self._sql_escape(filter_tag_value)
                    filter_tag_clauses.append(
                        f"(tag_entry.key = '{filter_tag_key}' AND tag_entry.value ="
                        f" '{escaped_value}')"
                    )
                elif isinstance(filter_tag_value, list) and filter_tag_value:
                    values_list = [f"'{self._sql_escape(value)}'" for value in filter_tag_value]
                    filter_tag_clauses.append(
                        f"(tag_entry.key = '{filter_tag_key}' AND tag_entry.value IN"
                        f" ({', '.join(values_list)}))"
                    )
            osm_tag_keys_select_clauses = [
                f"""
                map_from_entries(
                    [
                        tag_entry
                        for tag_entry in map_entries(tags)
                        if {" OR ".join(filter_tag_clauses)}
                    ]
                ) as tags
                """
            ]
        elif self.merged_tags_filter and explode_tags:
            for filter_tag_key, filter_tag_value in self.merged_tags_filter.items():
                if isinstance(filter_tag_value, bool) and filter_tag_value:
                    osm_tag_keys_select_clauses.append(
                        f"list_extract(map_extract(tags, '{filter_tag_key}'), 1) as"
                        f' "{filter_tag_key}"'
                    )
                elif isinstance(filter_tag_value, str):
                    escaped_value = self._sql_escape(filter_tag_value)
                    osm_tag_keys_select_clauses.append(
                        f"""
                        CASE WHEN list_extract(
                            map_extract(tags, '{filter_tag_key}'), 1
                        ) = '{escaped_value}'
                        THEN '{escaped_value}'
                        ELSE NULL
                        END as "{filter_tag_key}"
                        """
                    )
                elif isinstance(filter_tag_value, list) and filter_tag_value:
                    values_list = [f"'{self._sql_escape(value)}'" for value in filter_tag_value]
                    osm_tag_keys_select_clauses.append(
                        f"""
                        CASE WHEN list_extract(
                            map_extract(tags, '{filter_tag_key}'), 1
                        ) IN ({', '.join(values_list)})
                        THEN list_extract(map_extract(tags, '{filter_tag_key}'), 1)
                        ELSE NULL
                        END as "{filter_tag_key}"
                        """
                    )

        if len(osm_tag_keys_select_clauses) > 100:
            warnings.warn(
                "Select clause contains more than 100 columns"
                f" (found {len(osm_tag_keys_select_clauses)} columns)."
                " Query might fail with insufficient memory resources."
                " Consider applying more restrictive OsmTagsFilter for parsing.",
                stacklevel=1,
            )

        return osm_tag_keys_select_clauses

    def _parse_features_relation_to_groups(
        self,
        features_relation: "duckdb.DuckDBPyRelation",
        explode_tags: bool,
        keep_all_tags: bool,
    ) -> "duckdb.DuckDBPyRelation":
        """
        Optionally group raw OSM features into groups defined in `GroupedOsmTagsFilter`.

        Creates new features based on definition from `GroupedOsmTagsFilter`.
        Returns transformed DuckDB relation with columns based on group names from the filter.
        Values are built by concatenation of matching tag key and value with
        an equal sign (eg. amenity=parking). Since many tags can match a definition
        of a single group, a first match is used as a feature value.

        Args:
            features_relation (duckdb.DuckDBPyRelation): Generated features from the loader.
            explode_tags (bool): Whether to split tags into columns based on OSM tag keys.
            keep_all_tags (bool): Works only with the `tags_filter` parameter.
                Whether to keep all tags related to the element, or return only those defined
                in the `tags_filter`. When `True`, will override the optional grouping defined
                in the `tags_filter`. Defaults to `False`.

        Returns:
            duckdb.DuckDBPyRelation: Parsed features_relation.
        """
        if (
            not self.tags_filter
            or not is_expected_type(self.tags_filter, GroupedOsmTagsFilter)
            or keep_all_tags
        ):
            return features_relation

        grouped_features_relation: "duckdb.DuckDBPyRelation"
        grouped_tags_filter = cast(GroupedOsmTagsFilter, self.tags_filter)

        if explode_tags:
            case_clauses = []
            for group_name in sorted(grouped_tags_filter.keys()):
                osm_filter = grouped_tags_filter[group_name]
                case_when_clauses = []
                for osm_tag_key, osm_tag_value in osm_filter.items():
                    if isinstance(osm_tag_value, bool) and osm_tag_value:
                        case_when_clauses.append(
                            f"WHEN \"{osm_tag_key}\" IS NOT NULL THEN '{osm_tag_key}=' ||"
                            f' "{osm_tag_key}"'
                        )
                    elif isinstance(osm_tag_value, str):
                        escaped_value = self._sql_escape(osm_tag_value)
                        case_when_clauses.append(
                            f"WHEN \"{osm_tag_key}\" = '{escaped_value}' THEN '{osm_tag_key}=' ||"
                            f' "{osm_tag_key}"'
                        )
                    elif isinstance(osm_tag_value, list) and osm_tag_value:
                        values_list = [f"'{self._sql_escape(value)}'" for value in osm_tag_value]
                        case_when_clauses.append(
                            f"WHEN \"{osm_tag_key}\" IN ({', '.join(values_list)}) THEN"
                            f" '{osm_tag_key}=' || \"{osm_tag_key}\""
                        )
                case_clause = f'CASE {" ".join(case_when_clauses)} END AS "{group_name}"'
                case_clauses.append(case_clause)

            joined_case_clauses = ", ".join(case_clauses)
            grouped_features_relation = self.connection.sql(
                f"""
                SELECT feature_id, {joined_case_clauses}, geometry
                FROM ({features_relation.sql_query()})
                """
            )
        else:
            case_clauses = []
            group_names = sorted(grouped_tags_filter.keys())
            for group_name in group_names:
                osm_filter = grouped_tags_filter[group_name]
                case_when_clauses = []
                for osm_tag_key, osm_tag_value in osm_filter.items():
                    element_clause = f"element_at(tags, '{osm_tag_key}')[1]"
                    if isinstance(osm_tag_value, bool) and osm_tag_value:
                        case_when_clauses.append(
                            f"WHEN {element_clause} IS NOT NULL THEN '{osm_tag_key}=' ||"
                            f" {element_clause}"
                        )
                    elif isinstance(osm_tag_value, str):
                        escaped_value = self._sql_escape(osm_tag_value)
                        case_when_clauses.append(
                            f"WHEN {element_clause} = '{escaped_value}' THEN '{osm_tag_key}=' ||"
                            f" {element_clause}"
                        )
                    elif isinstance(osm_tag_value, list) and osm_tag_value:
                        values_list = [f"'{self._sql_escape(value)}'" for value in osm_tag_value]
                        case_when_clauses.append(
                            f"WHEN {element_clause} IN ({', '.join(values_list)}) THEN"
                            f" '{osm_tag_key}=' || {element_clause}"
                        )
                case_clause = f'CASE {" ".join(case_when_clauses)} END'
                case_clauses.append(case_clause)

            group_names_as_sql_strings = [f"'{group_name}'" for group_name in group_names]
            groups_map = (
                f"map([{', '.join(group_names_as_sql_strings)}], [{', '.join(case_clauses)}])"
            )
            non_null_groups_map = f"""map_from_entries(
                [
                    tag_entry
                    for tag_entry in map_entries({groups_map})
                    if tag_entry.value IS NOT NULL
                ]
            ) as tags"""

            grouped_features_relation = self.connection.sql(
                f"""
                SELECT feature_id, {non_null_groups_map}, geometry
                FROM ({features_relation.sql_query()})
                """
            )

        return grouped_features_relation


def _set_up_duckdb_connection(
    tmp_dir_path: Path, is_main_connection: bool = True
) -> "duckdb.DuckDBPyConnection":
    local_db_file = "db.duckdb" if is_main_connection else f"{secrets.token_hex(16)}.duckdb"
    connection = duckdb.connect(
        database=str(tmp_dir_path / local_db_file),
        config=dict(preserve_insertion_order=False),
    )
    connection.sql("SET enable_progress_bar = false;")
    connection.sql("SET enable_progress_bar_print = false;")
    for extension_name in ("parquet", "spatial"):
        connection.install_extension(extension_name)
        connection.load_extension(extension_name)

    connection.sql(
        """
        CREATE OR REPLACE MACRO linestring_to_linestring_wkt(ls) AS
        'LINESTRING (' || array_to_string([pt.x || ' ' || pt.y for pt in ls], ', ') || ')';
        """
    )
    connection.sql(
        """
        CREATE OR REPLACE MACRO linestring_to_polygon_wkt(ls) AS
        'POLYGON ((' || array_to_string([pt.x || ' ' || pt.y for pt in ls], ', ') || '))';
        """
    )

    return connection


def _run_query(sql_queries: Union[str, list[str]], tmp_dir_path: Path) -> None:
    if isinstance(sql_queries, str):
        sql_queries = [sql_queries]
    conn = _set_up_duckdb_connection(tmp_dir_path=tmp_dir_path, is_main_connection=False)
    for sql_query in sql_queries:
        conn.sql(sql_query)
    conn.close()
