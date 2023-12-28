"""
Functions.

This module contains helper functions to simplify the usage.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
from shapely.geometry.base import BaseGeometry

from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.pbf_file_reader import PbfFileReader


def convert_pbf_to_gpq(
    pbf_path: Union[str, Path],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    explode_tags: Optional[bool] = None,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
) -> Path:
    """
    Convert PBF file to GeoParquet file.

    Args:
        pbf_path (Union[str, Path]): Pbf file to be parsed to GeoParquet.
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
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on tags_filter parameter. If no tags filter is provided,
            then explode_tags will set to `False`, if there is tags filter it will set to `True`.
            Defaults to `None`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.

    Returns:
        Path: Path to the generated GeoParquet file.
    """
    reader = PbfFileReader(tags_filter=tags_filter, geometry_filter=geometry_filter)
    return reader.convert_pbf_to_gpq(
        pbf_path=pbf_path,
        result_file_path=result_file_path,
        explode_tags=explode_tags,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )


def get_features_gdf(
    file_paths: Union[str, Path, Iterable[Union[str, Path]]],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    explode_tags: Optional[bool] = None,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
) -> gpd.GeoDataFrame:
    """
    Get features GeoDataFrame from a PBF file or list of PBF files.

    Function can parse multiple PBF files and returns a single GeoDataFrame with loaded
    OSM objects.

    Args:
        file_paths (Union[str, Path, Iterable[Union[str, Path]]]):
            Path or list of paths of `*.osm.pbf` files to be parsed.
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
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on tags_filter parameter. If no tags filter is provided,
            then explode_tags will set to `False`, if there is tags filter it will set to `True`.
            Defaults to `None`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with OSM features.
    """
    reader = PbfFileReader(tags_filter=tags_filter, geometry_filter=geometry_filter)
    return reader.get_features_gdf(
        file_paths=file_paths,
        explode_tags=explode_tags,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )
