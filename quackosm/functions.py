"""
Functions.

This module contains helper functions to simplify the usage.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, Optional, Union

import geopandas as gpd
from pandas.util._decorators import deprecate, deprecate_kwarg
from shapely.geometry.base import BaseGeometry

from quackosm._constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_VERSION,
)
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm._osm_way_polygon_features import OsmWayPolygonConfig
from quackosm._rich_progress import VERBOSITY_MODE
from quackosm.osm_extracts import OsmExtractSource, download_extract_by_query
from quackosm.pbf_file_reader import PbfFileReader

__all__ = [
    "convert_pbf_to_parquet",
    "convert_pbf_to_duckdb",
    "convert_pbf_to_geodataframe",
    "convert_geometry_to_parquet",
    "convert_geometry_to_duckdb",
    "convert_geometry_to_geodataframe",
    "convert_osm_extract_to_parquet",
    "convert_osm_extract_to_duckdb",
    "convert_osm_extract_to_geodataframe",
]


def convert_pbf_to_duckdb(
    pbf_path: Union[str, Path, Iterable[Union[str, Path]]],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    duckdb_table_name: str = "quackosm",
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Convert PBF file to DuckDB file.

    Args:
        pbf_path (Union[str, Path, Iterable[Union[str, Path]]]):
            Path or list of paths of `*.osm.pbf` files to be parsed. Can be an URL.
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
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        duckdb_table_name (str): Table in which to store the OSM data inside the DuckDB database.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated DuckDB file.

    Examples:
        Get OSM data from a PBF file.

        Tags will be kept in a single column
        >>> from pathlib import Path
        >>> import quackosm as qosm

        >>> ddb_path = qosm.convert_pbf_to_duckdb(monaco_pbf_path) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/monaco_nofilter_noclip_compact_sorted.duckdb'

        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10005045289 │ {shop=bakery}        │ POINT (7.4224498 43.7310532)                 │
        │ node/10020887517 │ {leisure=swimming_…  │ POINT (7.4131561 43.7338391)                 │
        │ node/10021298117 │ {leisure=swimming_…  │ POINT (7.4277743 43.7427669)                 │
        │ node/10021298717 │ {leisure=swimming_…  │ POINT (7.4263029 43.7409734)                 │
        │ node/10025656383 │ {ferry=yes, name=Q…  │ POINT (7.4254971 43.7369002)                 │
        │ node/10025656390 │ {amenity=restauran…  │ POINT (7.4269287 43.7368818)                 │
        │ node/10025656391 │ {name=Capitainerie…  │ POINT (7.4272127 43.7359593)                 │
        │ node/10025656392 │ {name=Direction de…  │ POINT (7.4270392 43.7365262)                 │
        │ node/10025656393 │ {brand=IQOS, brand…  │ POINT (7.4275175 43.7373195)                 │
        │ node/10025656394 │ {artist_name=Anna …  │ POINT (7.4293446 43.737448)                  │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/986864693    │ {natural=bare_rock}  │ POLYGON ((7.4340482 43.745598, 7.4340263 4…  │
        │ way/986864694    │ {barrier=wall}       │ LINESTRING (7.4327547 43.7445382, 7.432808…  │
        │ way/986864695    │ {natural=bare_rock}  │ POLYGON ((7.4332994 43.7449315, 7.4332912 …  │
        │ way/986864696    │ {barrier=wall}       │ LINESTRING (7.4356006 43.7464325, 7.435574…  │
        │ way/986864697    │ {natural=bare_rock}  │ POLYGON ((7.4362767 43.74697, 7.4362983 43…  │
        │ way/990669427    │ {amenity=shelter, …  │ POLYGON ((7.4146087 43.733883, 7.4146192 4…  │
        │ way/990669428    │ {highway=secondary…  │ LINESTRING (7.4136598 43.7334433, 7.413640…  │
        │ way/990669429    │ {highway=secondary…  │ LINESTRING (7.4137621 43.7334251, 7.413746…  │
        │ way/990848785    │ {addr:city=Monaco,…  │ POLYGON ((7.4142551 43.7339622, 7.4143113 …  │
        │ way/993121275    │ {building=yes, nam…  │ POLYGON ((7.4321416 43.7481309, 7.4321638 …  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 8154 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Get only buildings, amenities and highways from a PBF file.
        >>> ddb_path = qosm.convert_pbf_to_duckdb(
        ...     monaco_pbf_path, tags_filter={"building": True, "amenity": True, "highway": True}
        ... ) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/monaco_6593ca69_noclip_compact_sorted.duckdb'

        Get features for Malé - the capital city of Maldives

        Tags will be kept in a single column.
        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────┬────────────┬─────────────┬───────────────────────────────┐
        │    feature_id    │ building │  amenity   │   highway   │           geometry            │
        │     varchar      │ varchar  │  varchar   │   varchar   │           geometry            │
        ├──────────────────┼──────────┼────────────┼─────────────┼───────────────────────────────┤
        │ node/10025656390 │ NULL     │ restaurant │ NULL        │ POINT (7.4269287 43.7368818)  │
        │ node/10025843517 │ NULL     │ restaurant │ NULL        │ POINT (7.4219362 43.7367446)  │
        │ node/10025852089 │ NULL     │ bar        │ NULL        │ POINT (7.4227543 43.7369926)  │
        │ node/10025852090 │ NULL     │ restaurant │ NULL        │ POINT (7.4225093 43.7369627)  │
        │ node/10068880332 │ NULL     │ NULL       │ bus_stop    │ POINT (7.4380858 43.7493026)  │
        │ node/10068880335 │ NULL     │ bench      │ NULL        │ POINT (7.4186855 43.7321515)  │
        │ node/10127713363 │ NULL     │ cafe       │ NULL        │ POINT (7.4266367 43.7420755)  │
        │ node/10601158089 │ NULL     │ restaurant │ NULL        │ POINT (7.4213086 43.7336187)  │
        │ node/10671507005 │ NULL     │ bar        │ NULL        │ POINT (7.4296915 43.7423307)  │
        │ node/10674256605 │ NULL     │ bar        │ NULL        │ POINT (7.4213558 43.7336317)  │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │ way/981971425    │ NULL     │ NULL       │ residential │ LINESTRING (7.4321217 43.74…  │
        │ way/982061461    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4246341 43.74…  │
        │ way/982081599    │ NULL     │ NULL       │ tertiary    │ LINESTRING (7.4225202 43.73…  │
        │ way/982081600    │ NULL     │ NULL       │ service     │ LINESTRING (7.4225202 43.73…  │
        │ way/986029035    │ NULL     │ NULL       │ path        │ LINESTRING (7.4189462 43.73…  │
        │ way/990669427    │ NULL     │ shelter    │ NULL        │ POLYGON ((7.4146087 43.7338…  │
        │ way/990669428    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4136598 43.73…  │
        │ way/990669429    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4137621 43.73…  │
        │ way/990848785    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4142551 43.7339…  │
        │ way/993121275    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4321416 43.7481…  │
        ├──────────────────┴──────────┴────────────┴─────────────┴───────────────────────────────┤
        │ 5902 rows (20 shown)                                                         5 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        >>> from shapely.geometry import box
        >>> ddb_path = qosm.convert_pbf_to_duckdb(
        ...     maldives_pbf_path,
        ...     geometry_filter=box(
        ...         minx=73.4975872,
        ...         miny=4.1663240,
        ...         maxx=73.5215528,
        ...         maxy=4.1818121
        ...     )
        ... ) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/maldives_nofilter_4eeabb20_compact_sorted.duckdb'

        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10010180778 │ {brand=Ooredoo, br…  │ POINT (73.5179039 4.1752105)                 │
        │ node/10062500171 │ {contact:facebook=…  │ POINT (73.509583 4.1724485)                  │
        │ node/10078084764 │ {addr:city=Male', …  │ POINT (73.5047972 4.1726734)                 │
        │ node/10078086040 │ {addr:city=Malé, a…  │ POINT (73.5031714 4.1759622)                 │
        │ node/10158825718 │ {addr:postcode=201…  │ POINT (73.5083189 4.1730108)                 │
        │ node/10289176711 │ {addr:street=Dhona…  │ POINT (73.5133902 4.1725724)                 │
        │ node/10294045310 │ {amenity=restauran…  │ POINT (73.5091277 4.1735378)                 │
        │ node/10294045311 │ {amenity=restauran…  │ POINT (73.5055534 4.1759515)                 │
        │ node/10294045411 │ {amenity=restauran…  │ POINT (73.5037257 4.1717866)                 │
        │ node/10294045412 │ {amenity=restauran…  │ POINT (73.5024147 4.1761633)                 │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │ way/91986244     │ {highway=residenti…  │ LINESTRING (73.5069785 4.1704686, 73.50759…  │
        │ way/91986245     │ {highway=residenti…  │ LINESTRING (73.5135834 4.1740562, 73.51383…  │
        │ way/91986249     │ {highway=residenti…  │ LINESTRING (73.5153971 4.1735146, 73.51601…  │
        │ way/91986251     │ {highway=residenti…  │ LINESTRING (73.5082522 4.1709887, 73.50823…  │
        │ way/91986254     │ {highway=residenti…  │ LINESTRING (73.508114 4.1693477, 73.508154…  │
        │ way/91986255     │ {landuse=cemetery,…  │ POLYGON ((73.507509 4.1731064, 73.5078884 …  │
        │ way/91986256     │ {highway=residenti…  │ LINESTRING (73.5106692 4.1744828, 73.51082…  │
        │ way/935784864    │ {layer=-1, locatio…  │ LINESTRING (73.4875382 4.1703263, 73.50074…  │
        │ way/935784867    │ {layer=-1, locatio…  │ LINESTRING (73.446172 4.1856738, 73.460937…  │
        │ way/959150179    │ {amenity=place_of_…  │ POLYGON ((73.5184052 4.1755282, 73.5184863…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 2168 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘
    """
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_duckdb(
        pbf_path=pbf_path,
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        duckdb_table_name=duckdb_table_name,
    )
    return Path(result_path)


def convert_geometry_to_duckdb(
    geometry_filter: BaseGeometry = None,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    duckdb_table_name: str = "quackosm",
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Get a DuckDB file with OpenStreetMap features within given geometry.

    Automatically downloads matching OSM extracts from different sources and returns a single file
    as a result.

    Args:
        geometry_filter (BaseGeometry): Geometry filter used to download matching OSM extracts.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        result_file_path (Union[str, Path], optional): Where to save
            the DuckDB file. If not provided, will be generated based on hashes
            from provided tags filter and geometry filter. Defaults to `None`.
        keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
            Whether to keep all tags related to the element, or return only those defined
            in the `tags_filter`. When `True`, will override the optional grouping defined
            in the `tags_filter`. Defaults to `False`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
            If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
            be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        duckdb_table_name (str): Table in which to store the OSM data inside the DuckDB database.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between 0
            and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Works only when PbfFileReader is asked to download OSM extracts
            automatically. Defaults to `False`.
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated DuckDB file.

    Examples:
        Get OSM data from the center of Monaco.

        >>> import quackosm as qosm
        >>> from shapely import from_wkt
        >>> wkt = (
        ...     "POLYGON ((7.41644 43.73598, 7.41644 43.73142, 7.42378 43.73142,"
        ...     " 7.42378 43.73598, 7.41644 43.73598))"
        ... )
        >>> ddb_path = qosm.convert_geometry_to_duckdb(from_wkt(wkt)) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/bf4b33de_nofilter_compact_sorted.duckdb'

        Inspect the file with duckdb
        >>> import duckdb
        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10068880335 │ {amenity=bench, ma…  │ POINT (7.4186855 43.7321515)                 │
        │ node/10196648824 │ {contact:city=Mona…  │ POINT (7.4193805 43.7337539)                 │
        │ node/10601158089 │ {addr:city=Monaco,…  │ POINT (7.4213086 43.7336187)                 │
        │ node/10672624925 │ {addr:city=Monaco,…  │ POINT (7.4215683 43.7351727)                 │
        │ node/10674256605 │ {amenity=bar, name…  │ POINT (7.4213558 43.7336317)                 │
        │ node/1074584632  │ {crossing=marked, …  │ POINT (7.4188525 43.7323654)                 │
        │ node/1074584650  │ {crossing=marked, …  │ POINT (7.4174145 43.7341601)                 │
        │ node/1079045434  │ {addr:country=MC, …  │ POINT (7.4173175 43.7320823)                 │
        │ node/1079045443  │ {highway=traffic_s…  │ POINT (7.4182804 43.7319223)                 │
        │ node/10862390705 │ {amenity=drinking_…  │ POINT (7.4219582 43.7355272)                 │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/952068828    │ {attraction=water_…  │ LINESTRING (7.4221787 43.7343579, 7.422176…  │
        │ way/952068829    │ {attraction=water_…  │ LINESTRING (7.4220996 43.7343719, 7.422131…  │
        │ way/952068830    │ {attraction=water_…  │ LINESTRING (7.4221161 43.7343595, 7.422119…  │
        │ way/952068831    │ {attraction=water_…  │ LINESTRING (7.4221421 43.7343773, 7.422159…  │
        │ way/952068832    │ {attraction=water_…  │ LINESTRING (7.4221748 43.7343815, 7.422173…  │
        │ way/952419569    │ {highway=primary, …  │ LINESTRING (7.4171229 43.7316079, 7.417117…  │
        │ way/952419570    │ {highway=primary, …  │ LINESTRING (7.4171473 43.7315034, 7.417166…  │
        │ way/952419571    │ {highway=primary, …  │ LINESTRING (7.4171671 43.731656, 7.4171486…  │
        │ way/952419572    │ {highway=primary, …  │ LINESTRING (7.4173054 43.7316813, 7.417276…  │
        │ way/952419573    │ {highway=primary, …  │ LINESTRING (7.4173897 43.7316435, 7.417372…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 1384 rows (20 shown)                                                                   │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Making sure that you are using specific OSM extract source - here Geofabrik.

        >>> ddb_path = qosm.convert_geometry_to_duckdb(
        ...     from_wkt(wkt),
        ...     osm_extract_source='Geofabrik',
        ... ) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/bf4b33de_nofilter_compact_sorted.duckdb'

        Inspect the file with duckdb
        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10068880335 │ {amenity=bench, ma…  │ POINT (7.4186855 43.7321515)                 │
        │ node/10196648824 │ {contact:city=Mona…  │ POINT (7.4193805 43.7337539)                 │
        │ node/10601158089 │ {addr:city=Monaco,…  │ POINT (7.4213086 43.7336187)                 │
        │ node/10672624925 │ {addr:city=Monaco,…  │ POINT (7.4215683 43.7351727)                 │
        │ node/10674256605 │ {amenity=bar, name…  │ POINT (7.4213558 43.7336317)                 │
        │ node/1074584632  │ {crossing=marked, …  │ POINT (7.4188525 43.7323654)                 │
        │ node/1074584650  │ {crossing=marked, …  │ POINT (7.4174145 43.7341601)                 │
        │ node/1079045434  │ {addr:country=MC, …  │ POINT (7.4173175 43.7320823)                 │
        │ node/1079045443  │ {highway=traffic_s…  │ POINT (7.4182804 43.7319223)                 │
        │ node/10862390705 │ {amenity=drinking_…  │ POINT (7.4219582 43.7355272)                 │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/952068828    │ {attraction=water_…  │ LINESTRING (7.4221787 43.7343579, 7.422176…  │
        │ way/952068829    │ {attraction=water_…  │ LINESTRING (7.4220996 43.7343719, 7.422131…  │
        │ way/952068830    │ {attraction=water_…  │ LINESTRING (7.4221161 43.7343595, 7.422119…  │
        │ way/952068831    │ {attraction=water_…  │ LINESTRING (7.4221421 43.7343773, 7.422159…  │
        │ way/952068832    │ {attraction=water_…  │ LINESTRING (7.4221748 43.7343815, 7.422173…  │
        │ way/952419569    │ {highway=primary, …  │ LINESTRING (7.4171229 43.7316079, 7.417117…  │
        │ way/952419570    │ {highway=primary, …  │ LINESTRING (7.4171473 43.7315034, 7.417166…  │
        │ way/952419571    │ {highway=primary, …  │ LINESTRING (7.4171671 43.731656, 7.4171486…  │
        │ way/952419572    │ {highway=primary, …  │ LINESTRING (7.4173054 43.7316813, 7.417276…  │
        │ way/952419573    │ {highway=primary, …  │ LINESTRING (7.4173897 43.7316435, 7.417372…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 1384 rows (20 shown)                                                                   │
        └────────────────────────────────────────────────────────────────────────────────────────┘
    """
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        osm_extract_source=osm_extract_source,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_geometry_to_duckdb(
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        duckdb_table_name=duckdb_table_name,
    )
    return Path(result_path)


def convert_osm_extract_to_duckdb(
    osm_extract_query: str,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    duckdb_table_name: str = "quackosm",
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Get a single OpenStreetMap extract from a given source and transform it to a DuckDB file.

    Args:
        osm_extract_query (str):
            Query to find an OpenStreetMap extract from available sources.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        duckdb_table_name (str): Table in which to store the OSM data inside the DuckDB database.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated DuckDB file.

    Examples:
        Get OSM data for the Monaco.

        >>> import quackosm as qosm
        >>> ddb_path = qosm.convert_osm_extract_to_duckdb(
        ...     "monaco", osm_extract_source="geofabrik"
        ... ) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.duckdb'

        Inspect the file with duckdb
        >>> import duckdb
        >>> with duckdb.connect(str(ddb_path)) as con:
        ...     con.load_extension('spatial')
        ...     con.sql("SELECT * FROM quackosm ORDER BY feature_id;") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10005045289 │ {shop=bakery}        │ POINT (7.4224498 43.7310532)                 │
        │ node/10020887517 │ {leisure=swimming_…  │ POINT (7.4131561 43.7338391)                 │
        │ node/10021298117 │ {leisure=swimming_…  │ POINT (7.4277743 43.7427669)                 │
        │ node/10021298717 │ {leisure=swimming_…  │ POINT (7.4263029 43.7409734)                 │
        │ node/10025656383 │ {ferry=yes, name=Q…  │ POINT (7.4254971 43.7369002)                 │
        │ node/10025656390 │ {amenity=restauran…  │ POINT (7.4269287 43.7368818)                 │
        │ node/10025656391 │ {name=Capitainerie…  │ POINT (7.4272127 43.7359593)                 │
        │ node/10025656392 │ {name=Direction de…  │ POINT (7.4270392 43.7365262)                 │
        │ node/10025656393 │ {name=IQOS, openin…  │ POINT (7.4275175 43.7373195)                 │
        │ node/10025656394 │ {artist_name=Anna …  │ POINT (7.4293446 43.737448)                  │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/986864693    │ {natural=bare_rock}  │ POLYGON ((7.4340482 43.745598, 7.4340263 4…  │
        │ way/986864694    │ {barrier=wall}       │ LINESTRING (7.4327547 43.7445382, 7.432808…  │
        │ way/986864695    │ {natural=bare_rock}  │ POLYGON ((7.4332994 43.7449315, 7.4332912 …  │
        │ way/986864696    │ {barrier=wall}       │ LINESTRING (7.4356006 43.7464325, 7.435574…  │
        │ way/986864697    │ {natural=bare_rock}  │ POLYGON ((7.4362767 43.74697, 7.4362983 43…  │
        │ way/990669427    │ {amenity=shelter, …  │ POLYGON ((7.4146087 43.733883, 7.4146192 4…  │
        │ way/990669428    │ {highway=secondary…  │ LINESTRING (7.4136598 43.7334433, 7.413640…  │
        │ way/990669429    │ {highway=secondary…  │ LINESTRING (7.4137621 43.7334251, 7.413746…  │
        │ way/990848785    │ {addr:city=Monaco,…  │ POLYGON ((7.4142551 43.7339622, 7.4143113 …  │
        │ way/993121275    │ {building=yes, nam…  │ POLYGON ((7.4321416 43.7481309, 7.4321638 …  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 7906 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Full name can also be used. Osm extract source can be skipped.

        >>> ddb_path = qosm.convert_osm_extract_to_duckdb(
        ...     "geofabrik_europe_monaco"
        ... ) # doctest: +IGNORE_RESULT
        >>> ddb_path.as_posix()
        'files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.duckdb'
    """
    downloaded_osm_extract = download_extract_by_query(
        query=osm_extract_query, source=osm_extract_source, progressbar=verbosity_mode != "silent"
    )
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_duckdb(
        pbf_path=downloaded_osm_extract,
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        duckdb_table_name=duckdb_table_name,
    )
    return Path(result_path)


def convert_pbf_to_parquet(
    pbf_path: Union[str, Path, Iterable[Union[str, Path]]],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    save_as_wkt: bool = False,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Convert PBF file to GeoParquet file.

    Args:
        pbf_path (Union[str, Path, Iterable[Union[str, Path]]]):
            Path or list of paths of `*.osm.pbf` files to be parsed. Can be an URL.
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
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        save_as_wkt (bool): Whether to save the file with geometry in the WKT form instead of WKB.
            If `True`, it will be saved as a `.parquet` file, because it won't be in the GeoParquet
            standard. Defaults to `False`.
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated GeoParquet file.

    Examples:
        Get OSM data from a PBF file.

        Tags will be kept in a single column.
        >>> import quackosm as qosm
        >>> gpq_path = qosm.convert_pbf_to_parquet(monaco_pbf_path) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/monaco_nofilter_noclip_compact_sorted.parquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10005045289 │ {shop=bakery}        │ POINT (7.4224498 43.7310532)                 │
        │ node/10020887517 │ {leisure=swimming_…  │ POINT (7.4131561 43.7338391)                 │
        │ node/10021298117 │ {leisure=swimming_…  │ POINT (7.4277743 43.7427669)                 │
        │ node/10021298717 │ {leisure=swimming_…  │ POINT (7.4263029 43.7409734)                 │
        │ node/10025656383 │ {ferry=yes, name=Q…  │ POINT (7.4254971 43.7369002)                 │
        │ node/10025656390 │ {amenity=restauran…  │ POINT (7.4269287 43.7368818)                 │
        │ node/10025656391 │ {name=Capitainerie…  │ POINT (7.4272127 43.7359593)                 │
        │ node/10025656392 │ {name=Direction de…  │ POINT (7.4270392 43.7365262)                 │
        │ node/10025656393 │ {name=IQOS, openin…  │ POINT (7.4275175 43.7373195)                 │
        │ node/10025656394 │ {artist_name=Anna …  │ POINT (7.4293446 43.737448)                  │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/986864693    │ {natural=bare_rock}  │ POLYGON ((7.4340482 43.745598, 7.4340263 4…  │
        │ way/986864694    │ {barrier=wall}       │ LINESTRING (7.4327547 43.7445382, 7.432808…  │
        │ way/986864695    │ {natural=bare_rock}  │ POLYGON ((7.4332994 43.7449315, 7.4332912 …  │
        │ way/986864696    │ {barrier=wall}       │ LINESTRING (7.4356006 43.7464325, 7.435574…  │
        │ way/986864697    │ {natural=bare_rock}  │ POLYGON ((7.4362767 43.74697, 7.4362983 43…  │
        │ way/990669427    │ {amenity=shelter, …  │ POLYGON ((7.4146087 43.733883, 7.4146192 4…  │
        │ way/990669428    │ {highway=secondary…  │ LINESTRING (7.4136598 43.7334433, 7.413640…  │
        │ way/990669429    │ {highway=secondary…  │ LINESTRING (7.4137621 43.7334251, 7.413746…  │
        │ way/990848785    │ {addr:city=Monaco,…  │ POLYGON ((7.4142551 43.7339622, 7.4143113 …  │
        │ way/993121275    │ {building=yes, nam…  │ POLYGON ((7.4321416 43.7481309, 7.4321638 …  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 7906 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Get only buildings, amenities and highways from a PBF file.

        Tags will be split into separate columns because of applying the filter.
        >>> gpq_path = qosm.convert_pbf_to_parquet(
        ...     monaco_pbf_path,
        ...     tags_filter={"building": True, "amenity": True, "highway": True}
        ... ) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/monaco_6593ca69_noclip_exploded_sorted.parquet'

        Inspect the file with duckdb
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────┬────────────┬─────────────┬───────────────────────────────┐
        │    feature_id    │ building │  amenity   │   highway   │           geometry            │
        │     varchar      │ varchar  │  varchar   │   varchar   │           geometry            │
        ├──────────────────┼──────────┼────────────┼─────────────┼───────────────────────────────┤
        │ node/10025656390 │ NULL     │ restaurant │ NULL        │ POINT (7.4269287 43.7368818)  │
        │ node/10025843517 │ NULL     │ restaurant │ NULL        │ POINT (7.4219362 43.7367446)  │
        │ node/10025852089 │ NULL     │ bar        │ NULL        │ POINT (7.4227543 43.7369926)  │
        │ node/10025852090 │ NULL     │ restaurant │ NULL        │ POINT (7.4225093 43.7369627)  │
        │ node/10068880332 │ NULL     │ NULL       │ platform    │ POINT (7.4380849 43.7493273)  │
        │ node/10068880335 │ NULL     │ bench      │ NULL        │ POINT (7.4186855 43.7321515)  │
        │ node/10127713363 │ NULL     │ cafe       │ NULL        │ POINT (7.4266367 43.7420755)  │
        │ node/10601158089 │ NULL     │ restaurant │ NULL        │ POINT (7.4213086 43.7336187)  │
        │ node/10671507005 │ NULL     │ bar        │ NULL        │ POINT (7.4296915 43.7423307)  │
        │ node/10674256605 │ NULL     │ bar        │ NULL        │ POINT (7.4213558 43.7336317)  │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │ way/981971425    │ NULL     │ NULL       │ residential │ LINESTRING (7.4321217 43.74…  │
        │ way/982061461    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4246341 43.74…  │
        │ way/982081599    │ NULL     │ NULL       │ tertiary    │ LINESTRING (7.4225202 43.73…  │
        │ way/982081600    │ NULL     │ NULL       │ service     │ LINESTRING (7.4225202 43.73…  │
        │ way/986029035    │ NULL     │ NULL       │ path        │ LINESTRING (7.4189462 43.73…  │
        │ way/990669427    │ NULL     │ shelter    │ NULL        │ POLYGON ((7.4146087 43.7338…  │
        │ way/990669428    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4136598 43.73…  │
        │ way/990669429    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4137621 43.73…  │
        │ way/990848785    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4142551 43.7339…  │
        │ way/993121275    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4321416 43.7481…  │
        ├──────────────────┴──────────┴────────────┴─────────────┴───────────────────────────────┤
        │ 5772 rows (20 shown)                                                         5 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Get features for Malé - the capital city of Maldives

        Tags will be kept in a single column.
        >>> from shapely.geometry import box
        >>> gpq_path = qosm.convert_pbf_to_parquet(
        ...     maldives_pbf_path,
        ...     geometry_filter=box(
        ...         minx=73.4975872,
        ...         miny=4.1663240,
        ...         maxx=73.5215528,
        ...         maxy=4.1818121
        ...     )
        ... ) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/maldives_nofilter_4eeabb20_compact_sorted.parquet'

        Inspect the file with duckdb
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10010180778 │ {brand=Ooredoo, br…  │ POINT (73.5179039 4.1752105)                 │
        │ node/10062500171 │ {contact:facebook=…  │ POINT (73.509583 4.1724485)                  │
        │ node/10078084764 │ {addr:city=Male', …  │ POINT (73.5047972 4.1726734)                 │
        │ node/10078086040 │ {addr:city=Malé, a…  │ POINT (73.5031714 4.1759622)                 │
        │ node/10158825718 │ {addr:postcode=201…  │ POINT (73.5083189 4.1730108)                 │
        │ node/10289176711 │ {addr:street=Dhona…  │ POINT (73.5133902 4.1725724)                 │
        │ node/10294045310 │ {amenity=restauran…  │ POINT (73.5091277 4.1735378)                 │
        │ node/10294045311 │ {amenity=restauran…  │ POINT (73.5055534 4.1759515)                 │
        │ node/10294045411 │ {amenity=restauran…  │ POINT (73.5037257 4.1717866)                 │
        │ node/10294045412 │ {amenity=restauran…  │ POINT (73.5024147 4.1761633)                 │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │ way/91986244     │ {highway=residenti…  │ LINESTRING (73.5069785 4.1704686, 73.50759…  │
        │ way/91986245     │ {highway=residenti…  │ LINESTRING (73.5135834 4.1740562, 73.51383…  │
        │ way/91986249     │ {highway=residenti…  │ LINESTRING (73.5153971 4.1735146, 73.51601…  │
        │ way/91986251     │ {highway=residenti…  │ LINESTRING (73.5082522 4.1709887, 73.50823…  │
        │ way/91986254     │ {highway=residenti…  │ LINESTRING (73.508114 4.1693477, 73.508154…  │
        │ way/91986255     │ {landuse=cemetery,…  │ POLYGON ((73.507509 4.1731064, 73.5078884 …  │
        │ way/91986256     │ {highway=residenti…  │ LINESTRING (73.5106692 4.1744828, 73.51082…  │
        │ way/935784864    │ {layer=-1, locatio…  │ LINESTRING (73.4875382 4.1703263, 73.50074…  │
        │ way/935784867    │ {layer=-1, locatio…  │ LINESTRING (73.446172 4.1856738, 73.460937…  │
        │ way/959150179    │ {amenity=place_of_…  │ POLYGON ((73.5184052 4.1755282, 73.5184863…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 2140 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘
    """
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_parquet(
        pbf_path=pbf_path,
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        save_as_wkt=save_as_wkt,
    )
    return Path(result_path)


def convert_geometry_to_parquet(
    geometry_filter: BaseGeometry = None,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    save_as_wkt: bool = False,
    verbosity_mode: VERBOSITY_MODE = "transient",
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Get a GeoParquet file with OpenStreetMap features within given geometry.

    Automatically downloads matching OSM extracts from different sources and returns a single file
    as a result.

    Args:
        geometry_filter (BaseGeometry): Geometry filter used to download matching OSM extracts.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        save_as_wkt (bool): Whether to save the file with geometry in the WKT form instead of WKB.
            If `True`, it will be saved as a `.parquet` file, because it won't be in the GeoParquet
            standard. Defaults to `False`.
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between 0
            and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Works only when PbfFileReader is asked to download OSM extracts
            automatically. Defaults to `False`.
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated GeoParquet file.

    Examples:
        Get OSM data from the center of Monaco.

        >>> import quackosm as qosm
        >>> from shapely import from_wkt
        >>> wkt = (
        ...     "POLYGON ((7.41644 43.73598, 7.41644 43.73142, 7.42378 43.73142,"
        ...     " 7.42378 43.73598, 7.41644 43.73598))"
        ... )
        >>> gpq_path = qosm.convert_geometry_to_parquet(from_wkt(wkt)) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/bf4b33de_nofilter_compact_sorted.parquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10068880335 │ {amenity=bench, ma…  │ POINT (7.4186855 43.7321515)                 │
        │ node/10196648824 │ {contact:city=Mona…  │ POINT (7.4193805 43.7337539)                 │
        │ node/10601158089 │ {addr:city=Monaco,…  │ POINT (7.4213086 43.7336187)                 │
        │ node/10672624925 │ {addr:city=Monaco,…  │ POINT (7.4215683 43.7351727)                 │
        │ node/10674256605 │ {amenity=bar, name…  │ POINT (7.4213558 43.7336317)                 │
        │ node/1074584632  │ {crossing=marked, …  │ POINT (7.4188525 43.7323654)                 │
        │ node/1074584650  │ {crossing=marked, …  │ POINT (7.4174145 43.7341601)                 │
        │ node/1079045434  │ {addr:country=MC, …  │ POINT (7.4173175 43.7320823)                 │
        │ node/1079045443  │ {highway=traffic_s…  │ POINT (7.4182804 43.7319223)                 │
        │ node/10862390705 │ {amenity=drinking_…  │ POINT (7.4219582 43.7355272)                 │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/952068828    │ {attraction=water_…  │ LINESTRING (7.4221787 43.7343579, 7.422176…  │
        │ way/952068829    │ {attraction=water_…  │ LINESTRING (7.4220996 43.7343719, 7.422131…  │
        │ way/952068830    │ {attraction=water_…  │ LINESTRING (7.4221161 43.7343595, 7.422119…  │
        │ way/952068831    │ {attraction=water_…  │ LINESTRING (7.4221421 43.7343773, 7.422159…  │
        │ way/952068832    │ {attraction=water_…  │ LINESTRING (7.4221748 43.7343815, 7.422173…  │
        │ way/952419569    │ {highway=primary, …  │ LINESTRING (7.4171229 43.7316079, 7.417117…  │
        │ way/952419570    │ {highway=primary, …  │ LINESTRING (7.4171473 43.7315034, 7.417166…  │
        │ way/952419571    │ {highway=primary, …  │ LINESTRING (7.4171671 43.731656, 7.4171486…  │
        │ way/952419572    │ {highway=primary, …  │ LINESTRING (7.4173054 43.7316813, 7.417276…  │
        │ way/952419573    │ {highway=primary, …  │ LINESTRING (7.4173897 43.7316435, 7.417372…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 1384 rows (20 shown)                                                                   │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Making sure that you are using specific OSM extract source - here Geofabrik.

        >>> gpq_path = qosm.convert_geometry_to_parquet(
        ...     from_wkt(wkt),
        ...     osm_extract_source='Geofabrik',
        ... ) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/bf4b33de_nofilter_compact_sorted.parquet'

        Inspect the file with duckdb
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10068880335 │ {amenity=bench, ma…  │ POINT (7.4186855 43.7321515)                 │
        │ node/10196648824 │ {contact:city=Mona…  │ POINT (7.4193805 43.7337539)                 │
        │ node/10601158089 │ {addr:city=Monaco,…  │ POINT (7.4213086 43.7336187)                 │
        │ node/10672624925 │ {addr:city=Monaco,…  │ POINT (7.4215683 43.7351727)                 │
        │ node/10674256605 │ {amenity=bar, name…  │ POINT (7.4213558 43.7336317)                 │
        │ node/1074584632  │ {crossing=marked, …  │ POINT (7.4188525 43.7323654)                 │
        │ node/1074584650  │ {crossing=marked, …  │ POINT (7.4174145 43.7341601)                 │
        │ node/1079045434  │ {addr:country=MC, …  │ POINT (7.4173175 43.7320823)                 │
        │ node/1079045443  │ {highway=traffic_s…  │ POINT (7.4182804 43.7319223)                 │
        │ node/10862390705 │ {amenity=drinking_…  │ POINT (7.4219582 43.7355272)                 │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/952068828    │ {attraction=water_…  │ LINESTRING (7.4221787 43.7343579, 7.422176…  │
        │ way/952068829    │ {attraction=water_…  │ LINESTRING (7.4220996 43.7343719, 7.422131…  │
        │ way/952068830    │ {attraction=water_…  │ LINESTRING (7.4221161 43.7343595, 7.422119…  │
        │ way/952068831    │ {attraction=water_…  │ LINESTRING (7.4221421 43.7343773, 7.422159…  │
        │ way/952068832    │ {attraction=water_…  │ LINESTRING (7.4221748 43.7343815, 7.422173…  │
        │ way/952419569    │ {highway=primary, …  │ LINESTRING (7.4171229 43.7316079, 7.417117…  │
        │ way/952419570    │ {highway=primary, …  │ LINESTRING (7.4171473 43.7315034, 7.417166…  │
        │ way/952419571    │ {highway=primary, …  │ LINESTRING (7.4171671 43.731656, 7.4171486…  │
        │ way/952419572    │ {highway=primary, …  │ LINESTRING (7.4173054 43.7316813, 7.417276…  │
        │ way/952419573    │ {highway=primary, …  │ LINESTRING (7.4173897 43.7316435, 7.417372…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 1384 rows (20 shown)                                                                   │
        └────────────────────────────────────────────────────────────────────────────────────────┘
    """
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        osm_extract_source=osm_extract_source,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_geometry_to_parquet(
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        save_as_wkt=save_as_wkt,
    )
    return Path(result_path)


def convert_osm_extract_to_parquet(
    osm_extract_query: str,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    save_as_wkt: bool = False,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> Path:
    """
    Get a single OpenStreetMap extract from a given source and transform it to a GeoParquet file.

    Args:
        osm_extract_query (str):
            Query to find an OpenStreetMap extract from available sources.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        save_as_wkt (bool): Whether to save the file with geometry in the WKT form instead of WKB.
            If `True`, it will be saved as a `.parquet` file, because it won't be in the GeoParquet
            standard. Defaults to `False`.
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        Path: Path to the generated GeoParquet file.

    Examples:
        Get OSM data for the Monaco.

        >>> import quackosm as qosm
        >>> gpq_path = qosm.convert_osm_extract_to_parquet(
        ...     "monaco", osm_extract_source="geofabrik"
        ... ) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.parquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10005045289 │ {shop=bakery}        │ POINT (7.4224498 43.7310532)                 │
        │ node/10020887517 │ {leisure=swimming_…  │ POINT (7.4131561 43.7338391)                 │
        │ node/10021298117 │ {leisure=swimming_…  │ POINT (7.4277743 43.7427669)                 │
        │ node/10021298717 │ {leisure=swimming_…  │ POINT (7.4263029 43.7409734)                 │
        │ node/10025656383 │ {ferry=yes, name=Q…  │ POINT (7.4254971 43.7369002)                 │
        │ node/10025656390 │ {amenity=restauran…  │ POINT (7.4269287 43.7368818)                 │
        │ node/10025656391 │ {name=Capitainerie…  │ POINT (7.4272127 43.7359593)                 │
        │ node/10025656392 │ {name=Direction de…  │ POINT (7.4270392 43.7365262)                 │
        │ node/10025656393 │ {name=IQOS, openin…  │ POINT (7.4275175 43.7373195)                 │
        │ node/10025656394 │ {artist_name=Anna …  │ POINT (7.4293446 43.737448)                  │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/986864693    │ {natural=bare_rock}  │ POLYGON ((7.4340482 43.745598, 7.4340263 4…  │
        │ way/986864694    │ {barrier=wall}       │ LINESTRING (7.4327547 43.7445382, 7.432808…  │
        │ way/986864695    │ {natural=bare_rock}  │ POLYGON ((7.4332994 43.7449315, 7.4332912 …  │
        │ way/986864696    │ {barrier=wall}       │ LINESTRING (7.4356006 43.7464325, 7.435574…  │
        │ way/986864697    │ {natural=bare_rock}  │ POLYGON ((7.4362767 43.74697, 7.4362983 43…  │
        │ way/990669427    │ {amenity=shelter, …  │ POLYGON ((7.4146087 43.733883, 7.4146192 4…  │
        │ way/990669428    │ {highway=secondary…  │ LINESTRING (7.4136598 43.7334433, 7.413640…  │
        │ way/990669429    │ {highway=secondary…  │ LINESTRING (7.4137621 43.7334251, 7.413746…  │
        │ way/990848785    │ {addr:city=Monaco,…  │ POLYGON ((7.4142551 43.7339622, 7.4143113 …  │
        │ way/993121275    │ {building=yes, nam…  │ POLYGON ((7.4321416 43.7481309, 7.4321638 …  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 7906 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Full name can also be used. Osm extract source can be skipped.

        >>> gpq_path = qosm.convert_osm_extract_to_parquet(
        ...     "geofabrik_europe_monaco"
        ... ) # doctest: +IGNORE_RESULT
        >>> gpq_path.as_posix()
        'files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.parquet'
    """
    downloaded_osm_extract = download_extract_by_query(
        query=osm_extract_query, source=osm_extract_source, progressbar=verbosity_mode != "silent"
    )
    result_path = PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_parquet(
        pbf_path=downloaded_osm_extract,
        result_file_path=result_file_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
        save_as_wkt=save_as_wkt,
    )
    return Path(result_path)


@deprecate_kwarg(old_arg_name="file_paths", new_arg_name="pbf_path")  # type: ignore
def convert_pbf_to_geodataframe(
    pbf_path: Union[str, Path, Iterable[Union[str, Path]]],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> gpd.GeoDataFrame:
    """
    Get features GeoDataFrame from a PBF file or list of PBF files.

    Function can parse multiple PBF files and returns a single GeoDataFrame with loaded
    OSM objects.

    Args:
        pbf_path (Union[str, Path, Iterable[Union[str, Path]]]):
            Path or list of paths of `*.osm.pbf` files to be parsed. Can be an URL.
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
        keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
            Whether to keep all tags related to the element, or return only those defined
            in the `tags_filter`. When `True`, will override the optional grouping defined
            in the `tags_filter`. Defaults to `False`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
            If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
            be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with OSM features.

    Examples:
        Get OSM data from a PBF file.

        Tags will be kept in a single column.
        >>> import quackosm as qosm
        >>> gdf = qosm.convert_pbf_to_geodataframe(monaco_pbf_path) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10005045289                  {'shop': 'bakery'}            POINT (7.42245 43.73105)
        node/10020887517  {'leisure': 'swimming_pool', 's...            POINT (7.41316 43.73384)
        node/10021298117  {'leisure': 'swimming_pool', 's...            POINT (7.42777 43.74277)
        node/10021298717  {'leisure': 'swimming_pool', 's...             POINT (7.4263 43.74097)
        node/10025656383  {'ferry': 'yes', 'name': 'Quai ...              POINT (7.4255 43.7369)
        ...                                              ...                                 ...
        way/990669427     {'amenity': 'shelter', 'shelter...  POLYGON ((7.41461 43.73388, 7.4...
        way/990669428     {'highway': 'secondary', 'junct...  LINESTRING (7.41366 43.73344, 7...
        way/990669429     {'highway': 'secondary', 'junct...  LINESTRING (7.41376 43.73343, 7...
        way/990848785     {'addr:city': 'Monaco', 'addr:h...  POLYGON ((7.41426 43.73396, 7.4...
        way/993121275     {'building': 'yes', 'name': 'Re...  POLYGON ((7.43214 43.74813, 7.4...
        <BLANKLINE>
        [7906 rows x 2 columns]

        Get only buildings from a PBF file.

        Tags will be split into separate columns because of applying the filter.
        >>> gdf = qosm.convert_pbf_to_geodataframe(
        ...     monaco_pbf_path, tags_filter={"building": True}
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                              building                            geometry
        feature_id
        relation/11384697          yes  POLYGON ((7.42749 43.73125, 7.4...
        relation/11484092        hotel  POLYGON ((7.4179 43.72483, 7.41...
        relation/11484093   apartments  POLYGON ((7.41815 43.72561, 7.4...
        relation/11484094  residential  POLYGON ((7.41753 43.72583, 7.4...
        relation/11485520   apartments  POLYGON ((7.42071 43.7326, 7.42...
        ...                        ...                                 ...
        way/94452886        apartments  POLYGON ((7.43242 43.74761, 7.4...
        way/946074428              yes  POLYGON ((7.42235 43.74037, 7.4...
        way/952067351              yes  POLYGON ((7.42207 43.73434, 7.4...
        way/990848785              yes  POLYGON ((7.41426 43.73396, 7.4...
        way/993121275              yes  POLYGON ((7.43214 43.74813, 7.4...
        <BLANKLINE>
        [1283 rows x 2 columns]

        Get features for Malé - the capital city of Maldives

        Tags will be kept in a single column.
        >>> from shapely.geometry import box
        >>> gdf = qosm.convert_pbf_to_geodataframe(
        ...     maldives_pbf_path,
        ...     geometry_filter=box(
        ...         minx=73.4975872,
        ...         miny=4.1663240,
        ...         maxx=73.5215528,
        ...         maxy=4.1818121
        ...     )
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10010180778  {'brand': 'Ooredoo', 'brand:wik...             POINT (73.5179 4.17521)
        node/10062500171  {'contact:facebook': 'https://w...            POINT (73.50958 4.17245)
        node/10078084764  {'addr:city': 'Male'', 'addr:po...             POINT (73.5048 4.17267)
        node/10078086040  {'addr:city': 'Malé', 'addr:pos...            POINT (73.50317 4.17596)
        node/10158825718  {'addr:postcode': '20175', 'add...            POINT (73.50832 4.17301)
        ...                                              ...                                 ...
        way/91986255      {'landuse': 'cemetery', 'name':...  POLYGON ((73.50751 4.17311, 73....
        way/91986256      {'highway': 'residential', 'nam...  LINESTRING (73.51067 4.17448, 7...
        way/935784864     {'layer': '-1', 'location': 'un...  LINESTRING (73.48754 4.17033, 7...
        way/935784867     {'layer': '-1', 'location': 'un...  LINESTRING (73.44617 4.18567, 7...
        way/959150179     {'amenity': 'place_of_worship',...  POLYGON ((73.51841 4.17553, 73....
        <BLANKLINE>
        [2140 rows x 2 columns]


        Get features grouped into catgegories for Christmas Island

        Even though we apply the filter, the tags will be kept in a single column
        because of manual `explode_tags` value setting.
        >>> gdf = qosm.convert_pbf_to_geodataframe(
        ...     kiribati_pbf_path,
        ...     tags_filter={
        ...         "highway": {"highway": True},
        ...         "tree": {"natural": "tree"},
        ...         "building": {"building": True},
        ...     },
        ...     geometry_filter=box(
        ...         minx=-157.6046004,
        ...         miny=1.6724409,
        ...         maxx=-157.1379507,
        ...         maxy=2.075240
        ...     ),
        ...     explode_tags=False,
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                  tags                            geometry
        feature_id
        node/2377661784  {'building': 'building=ruin'}          POINT (-157.18826 1.75186)
        node/4150479646       {'tree': 'natural=tree'}          POINT (-157.36152 1.98363)
        node/4396875565       {'tree': 'natural=tree'}          POINT (-157.36143 1.98364)
        node/4396875566       {'tree': 'natural=tree'}          POINT (-157.36135 1.98364)
        node/4396875567       {'tree': 'natural=tree'}          POINT (-157.36141 1.98371)
        ...                                        ...                                 ...
        way/997441336     {'highway': 'highway=track'}  LINESTRING (-157.38083 1.77798,...
        way/997441337     {'highway': 'highway=track'}  LINESTRING (-157.39796 1.79933,...
        way/998103305      {'highway': 'highway=path'}  LINESTRING (-157.56048 1.87379,...
        way/998103306     {'highway': 'highway=track'}  LINESTRING (-157.55513 1.86846,...
        way/998370723      {'highway': 'highway=path'}  LINESTRING (-157.47069 1.83903,...
        <BLANKLINE>
        [3109 rows x 2 columns]
    """
    return PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_geodataframe(
        pbf_path=pbf_path,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )


def convert_geometry_to_geodataframe(
    geometry_filter: BaseGeometry = None,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> gpd.GeoDataFrame:
    """
    Get features GeoDataFrame with OpenStreetMap features within given geometry.

    Automatically downloads matching OSM extracts from different sources and returns a single file
    as a result.

    Args:
        geometry_filter (BaseGeometry): Geometry filter used to download matching OSM extracts.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
            Whether to keep all tags related to the element, or return only those defined
            in the `tags_filter`. When `True`, will override the optional grouping defined
            in the `tags_filter`. Defaults to `False`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
            If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
            be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between 0
            and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Works only when PbfFileReader is asked to download OSM extracts
            automatically. Defaults to `False`.
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with OSM features.

    Examples:
        Get OSM data from the center of Monaco.

        >>> import quackosm as qosm
        >>> from shapely import from_wkt
        >>> wkt = (
        ...     "POLYGON ((7.41644 43.73598, 7.41644 43.73142, 7.42378 43.73142,"
        ...     " 7.42378 43.73598, 7.41644 43.73598))"
        ... )
        >>> gdf = qosm.convert_geometry_to_geodataframe(from_wkt(wkt)) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10068880335  {'amenity': 'bench', 'material'...            POINT (7.41869 43.73215)
        node/10196648824  {'contact:city': 'Monaco', 'con...            POINT (7.41938 43.73375)
        node/10601158089  {'addr:city': 'Monaco', 'addr:h...            POINT (7.42131 43.73362)
        node/10672624925  {'addr:city': 'Monaco', 'addr:h...            POINT (7.42157 43.73517)
        node/10674256605  {'amenity': 'bar', 'name:en': '...            POINT (7.42136 43.73363)
        ...                                              ...                                 ...
        way/952419569     {'highway': 'primary', 'junctio...  LINESTRING (7.41712 43.73161, 7...
        way/952419570     {'highway': 'primary', 'junctio...  LINESTRING (7.41715 43.7315, 7....
        way/952419571     {'highway': 'primary', 'junctio...  LINESTRING (7.41717 43.73166, 7...
        way/952419572     {'highway': 'primary', 'junctio...  LINESTRING (7.41731 43.73168, 7...
        way/952419573     {'highway': 'primary', 'junctio...  LINESTRING (7.41739 43.73164, 7...
        <BLANKLINE>
        [1384 rows x 2 columns]

        Making sure that you are using specific OSM extract source - here Geofabrik.

        >>> gdf = qosm.convert_geometry_to_geodataframe(
        ...     from_wkt(wkt),
        ...     osm_extract_source='Geofabrik',
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10068880335  {'amenity': 'bench', 'material'...            POINT (7.41869 43.73215)
        node/10196648824  {'contact:city': 'Monaco', 'con...            POINT (7.41938 43.73375)
        node/10601158089  {'addr:city': 'Monaco', 'addr:h...            POINT (7.42131 43.73362)
        node/10672624925  {'addr:city': 'Monaco', 'addr:h...            POINT (7.42157 43.73517)
        node/10674256605  {'amenity': 'bar', 'name:en': '...            POINT (7.42136 43.73363)
        ...                                              ...                                 ...
        way/952419569     {'highway': 'primary', 'junctio...  LINESTRING (7.41712 43.73161, 7...
        way/952419570     {'highway': 'primary', 'junctio...  LINESTRING (7.41715 43.7315, 7....
        way/952419571     {'highway': 'primary', 'junctio...  LINESTRING (7.41717 43.73166, 7...
        way/952419572     {'highway': 'primary', 'junctio...  LINESTRING (7.41731 43.73168, 7...
        way/952419573     {'highway': 'primary', 'junctio...  LINESTRING (7.41739 43.73164, 7...
        <BLANKLINE>
        [1384 rows x 2 columns]
    """
    return PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        osm_extract_source=osm_extract_source,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_geometry_to_geodataframe(
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )


def convert_osm_extract_to_geodataframe(
    osm_extract_query: str,
    osm_extract_source: Union[OsmExtractSource, str] = OsmExtractSource.any,
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    keep_all_tags: bool = False,
    explode_tags: Optional[bool] = None,
    sort_result: bool = True,
    compression: str = PARQUET_COMPRESSION,
    compression_level: int = PARQUET_COMPRESSION_LEVEL,
    row_group_size: int = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Literal["v1", "v2"] = PARQUET_VERSION,
    ignore_metadata_tags: bool = True,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    custom_sql_filter: Optional[str] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
    verbosity_mode: VERBOSITY_MODE = "transient",
    debug_memory: bool = False,
    debug_times: bool = False,
    cpu_limit: Optional[int] = None,
) -> gpd.GeoDataFrame:
    """
    Get a single OpenStreetMap extract from a given source and return it as a GeoDataFrame.

    Args:
        osm_extract_query (str):
            Query to find an OpenStreetMap extract from available sources.
        osm_extract_source (Union[OsmExtractSource, str], optional): A source for automatic
            downloading of OSM extracts. Can be Geofabrik, BBBike, OSMfr or any.
            Defaults to `any`.
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
        keep_all_tags (bool, optional): Works only with the `tags_filter` parameter.
            Whether to keep all tags related to the element, or return only those defined
            in the `tags_filter`. When `True`, will override the optional grouping defined
            in the `tags_filter`. Defaults to `False`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` and `keep_all_tags` parameters.
            If there is tags filter defined and `keep_all_tags` is set to `False`, then it will
            be set to `True`. Otherwise it will be set to `False`. Defaults to `None`.
        sort_result (bool, optional): Whether to sort the result by geometry or not.
            Defaults to True.
        compression (str, optional): Compression of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Remember to change compression level together with this parameter.
            Defaults to "zstd".
        compression_level (int, optional): Compression level of the final parquet file.
            Check https://duckdb.org/docs/sql/statements/copy#parquet-options for more info.
            Supported only for zstd compression. Defaults to 3.
        row_group_size (int, optional): Approximate number of rows per row group in the final
            parquet file. Defaults to 100_000.
        parquet_version (Literal["v1", "v2"], optional): What type of parquet version use to
            save final file. Available only in DuckDB version >= 1.3.0. Defaults to "v2".
        ignore_metadata_tags (bool, optional): Remove metadata tags, based on the default GDAL
            config. Defaults to `True`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        custom_sql_filter (str, optional): Allows users to pass custom SQL conditions used
            to filter OSM features. It will be embedded into predefined queries and requires
            DuckDB syntax to operate on tags map object. Defaults to None.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".
        verbosity_mode (Literal["silent", "transient", "verbose"], optional): Set progress
            verbosity mode. Can be one of: silent, transient and verbose. Silent disables
            output completely. Transient tracks progress, but removes output after finished.
            Verbose leaves all progress outputs in the stdout. Defaults to "transient".
        debug_memory (bool, optional): If turned on, will keep all temporary files after operation
            for debugging. Defaults to `False`.
        debug_times (bool, optional): If turned on, will report timestamps at which second each
            step has been executed. Defaults to `False`.
        cpu_limit (int, optional): Max number of threads available for processing.
            If `None`, will use all available threads. Defaults to `None`.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with OSM features.

    Examples:
        Get OSM data for the Monaco.

        >>> import quackosm as qosm
        >>> gdf = qosm.convert_osm_extract_to_geodataframe(
        ...     "monaco", osm_extract_source="geofabrik"
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10005045289                  {'shop': 'bakery'}            POINT (7.42245 43.73105)
        node/10020887517  {'leisure': 'swimming_pool', 's...            POINT (7.41316 43.73384)
        node/10021298117  {'leisure': 'swimming_pool', 's...            POINT (7.42777 43.74277)
        node/10021298717  {'leisure': 'swimming_pool', 's...             POINT (7.4263 43.74097)
        node/10025656383  {'ferry': 'yes', 'name': 'Quai ...              POINT (7.4255 43.7369)
        ...                                              ...                                 ...
        way/990669427     {'amenity': 'shelter', 'shelter...  POLYGON ((7.41461 43.73388, 7.4...
        way/990669428     {'highway': 'secondary', 'junct...  LINESTRING (7.41366 43.73344, 7...
        way/990669429     {'highway': 'secondary', 'junct...  LINESTRING (7.41376 43.73343, 7...
        way/990848785     {'addr:city': 'Monaco', 'addr:h...  POLYGON ((7.41426 43.73396, 7.4...
        way/993121275     {'building': 'yes', 'name': 'Re...  POLYGON ((7.43214 43.74813, 7.4...
        <BLANKLINE>
        [7906 rows x 2 columns]

        Full name can also be used. Osm extract source can be skipped.

        >>> gdf = qosm.convert_osm_extract_to_geodataframe(
        ...     "geofabrik_europe_monaco"
        ... ) # doctest: +IGNORE_RESULT
        >>> gdf.sort_index()
                                                        tags                            geometry
        feature_id
        node/10005045289                  {'shop': 'bakery'}            POINT (7.42245 43.73105)
        node/10020887517  {'leisure': 'swimming_pool', 's...            POINT (7.41316 43.73384)
        node/10021298117  {'leisure': 'swimming_pool', 's...            POINT (7.42777 43.74277)
        node/10021298717  {'leisure': 'swimming_pool', 's...             POINT (7.4263 43.74097)
        node/10025656383  {'ferry': 'yes', 'name': 'Quai ...              POINT (7.4255 43.7369)
        ...                                              ...                                 ...
        way/990669427     {'amenity': 'shelter', 'shelter...  POLYGON ((7.41461 43.73388, 7.4...
        way/990669428     {'highway': 'secondary', 'junct...  LINESTRING (7.41366 43.73344, 7...
        way/990669429     {'highway': 'secondary', 'junct...  LINESTRING (7.41376 43.73343, 7...
        way/990848785     {'addr:city': 'Monaco', 'addr:h...  POLYGON ((7.41426 43.73396, 7.4...
        way/993121275     {'building': 'yes', 'name': 'Re...  POLYGON ((7.43214 43.74813, 7.4...
        <BLANKLINE>
        [7906 rows x 2 columns]
    """
    downloaded_osm_extract = download_extract_by_query(
        query=osm_extract_query, source=osm_extract_source, progressbar=verbosity_mode != "silent"
    )
    return PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        custom_sql_filter=custom_sql_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        parquet_version=parquet_version,
        ignore_metadata_tags=ignore_metadata_tags,
        verbosity_mode=verbosity_mode,
        debug_memory=debug_memory,
        debug_times=debug_times,
        cpu_limit=cpu_limit,
    ).convert_pbf_to_geodataframe(
        pbf_path=downloaded_osm_extract,
        keep_all_tags=keep_all_tags,
        explode_tags=explode_tags,
        sort_result=sort_result,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )


convert_pbf_to_gpq = deprecate(
    "convert_pbf_to_gpq",
    convert_pbf_to_parquet,
    "0.8.1",
    msg="Use `convert_pbf_to_parquet` instead. Deprecated since 0.8.1 version.",
)

convert_geometry_to_gpq = deprecate(
    "convert_geometry_to_gpq",
    convert_geometry_to_parquet,
    "0.8.1",
    msg="Use `convert_geometry_to_parquet` instead. Deprecated since 0.8.1 version.",
)

get_features_gdf = deprecate(
    "get_features_gdf",
    convert_pbf_to_geodataframe,
    "0.8.1",
    msg="Use `convert_pbf_to_geodataframe` instead. Deprecated since 0.8.1 version.",
)

get_features_gdf_from_geometry = deprecate(
    "get_features_gdf_from_geometry",
    convert_geometry_to_geodataframe,
    "0.8.1",
    msg="Use `convert_geometry_to_geodataframe` instead. Deprecated since 0.8.1 version.",
)
