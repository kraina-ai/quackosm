"""OpenStreetMap extract class."""

import warnings
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Callable, cast

import platformdirs

from quackosm._constants import WGS84_CRS

if TYPE_CHECKING:  # pragma: no cover
    from geopandas import GeoDataFrame
    from pandas import DataFrame
    from shapely.geometry.base import BaseGeometry


@dataclass
class OpenStreetMapExtract:
    """OSM Extract metadata object."""

    id: str
    name: str
    parent: str
    url: str
    geometry: "BaseGeometry"
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


def load_index_decorator(
    extract_source: OsmExtractSource,
) -> Callable[[Callable[[], "GeoDataFrame"]], Callable[[], "GeoDataFrame"]]:
    """
    Decorator for loading OSM extracts index.

    Args:
        extract_source (OsmExtractSource): OpenStreetMap extract source.
            Used to save the index to cache.
    """

    def inner(function: Callable[[], "GeoDataFrame"]) -> Callable[[], "GeoDataFrame"]:
        def wrapper() -> "GeoDataFrame":
            global_cache_file_path = _get_global_cache_file_path(extract_source)
            global_cache_file_path.parent.mkdir(exist_ok=True, parents=True)
            expected_columns = ["id", "name", "file_name", "parent", "geometry", "area", "url"]

            # Check if index exists in cache
            if global_cache_file_path.exists():
                import geopandas as gpd

                index_gdf = gpd.read_file(global_cache_file_path)
            elif (local_cache_file_path := _get_local_cache_file_path(extract_source)).exists():
                import shutil

                import geopandas as gpd

                shutil.copy(local_cache_file_path, global_cache_file_path)
                index_gdf = gpd.read_file(global_cache_file_path)
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
                from quackosm._exceptions import OsmExtractIndexOutdatedWarning

                warnings.warn(
                    "Existing cached index has outdated structure. New index will be redownloaded.",
                    OsmExtractIndexOutdatedWarning,
                    stacklevel=0,
                )
                # Invalidate previous cached index
                global_cache_file_path.replace(global_cache_file_path.with_suffix(".geojson.old"))
                # Download index again
                index_gdf = wrapper()

            # Save index to cache
            if not global_cache_file_path.exists():
                global_cache_file_path.parent.mkdir(parents=True, exist_ok=True)
                index_gdf[expected_columns].to_file(global_cache_file_path, driver="GeoJSON")

            return index_gdf

        return wrapper

    return inner


def extracts_to_geodataframe(extracts: list[OpenStreetMapExtract]) -> "GeoDataFrame":
    """Transforms a list of OpenStreetMapExtracts to a GeoDataFrame."""
    import geopandas as gpd

    return gpd.GeoDataFrame(
        data=[asdict(extract) for extract in extracts], geometry="geometry"
    ).set_crs(WGS84_CRS)


def _get_global_cache_file_path(extract_source: OsmExtractSource) -> Path:
    return (
        Path(platformdirs.user_cache_dir("QuackOSM"))
        / f"{extract_source.value.lower()}_index.geojson"
    )


def _get_local_cache_file_path(extract_source: OsmExtractSource) -> Path:
    return Path(f"cache/{extract_source.value.lower()}_index.geojson")


def _calculate_geodetic_area(geometry: "BaseGeometry") -> float:
    from pyproj import Geod
    from shapely.ops import orient

    geod = Geod(ellps="WGS84")
    poly_area_m2, _ = geod.geometry_area_perimeter(orient(geometry, sign=1))
    poly_area_km2 = round(poly_area_m2) / 1_000_000
    return cast(float, poly_area_km2)


def _get_full_file_name_function(index: "DataFrame") -> Callable[[str], str]:
    from pandas import Index

    ids_index = Index(index["id"])

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
