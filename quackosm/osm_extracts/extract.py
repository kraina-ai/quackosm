"""OpenStreetMap extract class."""

import warnings
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, TypeVar, cast

import geopandas as gpd
import pandas as pd
from pyproj import Geod
from shapely.geometry.base import BaseGeometry
from shapely.ops import orient

from quackosm._exceptions import OsmExtractIndexOutdatedWarning


@dataclass
class OpenStreetMapExtract:
    """OSM Extract metadata object."""

    id: str
    name: str
    parent: str
    url: str
    geometry: BaseGeometry
    file_name: str = ""


class OsmExtractSource(str, Enum):
    """Enum of available OSM extract sources."""

    any = "any"
    geofabrik = "Geofabrik"
    osm_fr = "osmfr"
    bbbike = "BBBike"

    @classmethod
    def _missing_(cls, value):  # type: ignore
        value = value.lower()
        for member in cls:
            if member.lower() == value:
                return member
        return None


F = TypeVar("F", bound=Callable[[], gpd.GeoDataFrame])


def load_index_decorator(
    extract_source: OsmExtractSource,
) -> Callable[[Callable[[], gpd.GeoDataFrame]], Callable[[], gpd.GeoDataFrame]]:
    """
    Decorator for loading OSM extracts index.

    Args:
        extract_source (OsmExtractSource): OpenStreetMap extract source.
            Used to save the index to cache.
    """

    def inner(function: Callable[[], gpd.GeoDataFrame]) -> Callable[[], gpd.GeoDataFrame]:
        def wrapper() -> gpd.GeoDataFrame:
            cache_file_path = _get_cache_file_path(extract_source)
            expected_columns = ["id", "name", "file_name", "parent", "geometry", "area", "url"]

            # Check if index exists in cache
            if cache_file_path.exists():
                index_gdf = gpd.read_file(cache_file_path)
            # Download index
            else:  # pragma: no cover
                index_gdf = function()
                # calculate extracts area
                index_gdf["area"] = index_gdf.geometry.apply(_calculate_geodetic_area)
                index_gdf.sort_values(by="area", ignore_index=True, inplace=True)

                # generate full file names
                apply_function = _get_full_file_name_function(index_gdf)
                index_gdf["file_name"] = index_gdf["id"].apply(apply_function)

                index_gdf = index_gdf[expected_columns]

            # Check if columns are right
            if set(expected_columns).symmetric_difference(index_gdf.columns):
                warnings.warn(
                    "Existing cached index has outdated structure. New index will be redownloaded.",
                    OsmExtractIndexOutdatedWarning,
                    stacklevel=0,
                )
                # Invalidate previous cached index
                cache_file_path.rename(cache_file_path.with_suffix(".geojson.old"))
                # Download index again
                index_gdf = wrapper()

            # Save index to cache
            if not cache_file_path.exists():
                cache_file_path.parent.mkdir(parents=True, exist_ok=True)
                index_gdf[expected_columns].to_file(cache_file_path, driver="GeoJSON")

            return index_gdf

        return wrapper

    return inner


def _get_cache_file_path(extract_source: OsmExtractSource) -> Path:
    return Path(f"cache/{extract_source.value.lower()}_index.geojson")


def _calculate_geodetic_area(geometry: BaseGeometry) -> float:
    geod = Geod(ellps="WGS84")
    poly_area, _ = geod.geometry_area_perimeter(orient(geometry, sign=1))
    return cast(float, poly_area)


def _get_full_file_name_function(index: pd.DataFrame) -> Callable[[str], str]:
    ids_index = pd.Index(index["id"])

    def inner_function(id: str) -> str:
        current_id = id
        parts = []
        while True:
            if current_id not in ids_index:
                parts.append(current_id.lower())
                break
            else:
                matching_row = index.iloc[ids_index.get_loc(current_id)]
                parts.append(matching_row["name"].lower())
                current_id = matching_row["parent"]

        return "_".join(parts[::-1])

    return inner_function
