"""
OpenStreetMap extracts.

This module contains iterators for publically available OpenStreetMap `*.osm.pbf` files
repositories.
"""

from collections.abc import Iterable
from enum import Enum
from functools import partial
from math import ceil
from multiprocessing import cpu_count
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
from pooch import retrieve
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry
from tqdm.contrib.concurrent import process_map

from quackosm.osm_extracts.bbbike import _get_bbbike_index
from quackosm.osm_extracts.extract import OpenStreetMapExtract
from quackosm.osm_extracts.geofabrik import _get_geofabrik_index
from quackosm.osm_extracts.osm_fr import _get_openstreetmap_fr_index

__all__ = [
    "download_extracts_pbf_files",
    "find_smallest_containing_extracts_total",
    "find_smallest_containing_geofabrik_extracts",
    "find_smallest_containing_openstreetmap_fr_extracts",
    "find_smallest_containing_bbbike_extracts",
    "OsmExtractSource",
]


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


def download_extracts_pbf_files(
    extracts: list[OpenStreetMapExtract], download_directory: Path
) -> list[Path]:
    """
    Download OSM extracts as PBF files.

    Args:
        extracts (list[OpenStreetMapExtract]): List of extracts to download.
        download_directory (Path): Directory where PBF files should be saved.

    Returns:
        list[Path]: List of downloaded file paths.
    """
    downloaded_extracts_paths = []
    for extract in extracts:
        file_path = retrieve(
            extract.url,
            fname=f"{extract.id}.osm.pbf",
            path=download_directory,
            progressbar=True,
            known_hash=None,
        )
        downloaded_extracts_paths.append(Path(file_path))
    return downloaded_extracts_paths


def find_smallest_containing_extracts_total(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from all OSM extract indexes that contains given polygon.

    Iterates all indexes and finds smallest extracts that covers a given geometry.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    indexes = gpd.pd.concat(
        [_get_bbbike_index(), _get_geofabrik_index(), _get_openstreetmap_fr_index()]
    )
    indexes.sort_values(by="area", ignore_index=True, inplace=True)
    return _find_smallest_containing_extracts(geometry, indexes)


def find_smallest_containing_geofabrik_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from Geofabrik that contains given geometry.

    Iterates a geofabrik index and finds smallest extracts that covers a given geometry.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(geometry, _get_geofabrik_index())


def find_smallest_containing_openstreetmap_fr_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from OpenStreetMap.fr that contains given polygon.

    Iterates an osm.fr index and finds smallest extracts that covers a given geometry.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(geometry, _get_openstreetmap_fr_index())


def find_smallest_containing_bbbike_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from BBBike that contains given polygon.

    Iterates an BBBike index and finds smallest extracts that covers a given geometry.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(geometry, _get_bbbike_index())


OSM_EXTRACT_SOURCE_MATCHING_FUNCTION = {
    OsmExtractSource.any: find_smallest_containing_extracts_total,
    OsmExtractSource.bbbike: find_smallest_containing_bbbike_extracts,
    OsmExtractSource.geofabrik: find_smallest_containing_geofabrik_extracts,
    OsmExtractSource.osm_fr: find_smallest_containing_openstreetmap_fr_extracts,
}


def find_smallest_containing_extract(
    geometry: Union[BaseGeometry, BaseMultipartGeometry], source: Union[OsmExtractSource, str]
) -> list[OpenStreetMapExtract]:
    try:
        source_enum = OsmExtractSource(source)
        return OSM_EXTRACT_SOURCE_MATCHING_FUNCTION[source_enum](geometry)
    except ValueError as ex:
        raise ValueError(f"Unknown OSM extracts source: {source}.") from ex


def _find_smallest_containing_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    polygons_index_gdf: gpd.GeoDataFrame,
    num_of_multiprocessing_workers: int = -1,
    multiprocessing_activation_threshold: Optional[int] = None,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest set of extracts covering a given geometry.

    Iterates a provided extracts index and searches for a smallest set that cover a given geometry.
    It's not guaranteed that this set will be the smallest and there will be no overlaps.

    Geometry will be flattened into singluar geometries if it's `BaseMultipartGeometry`.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        polygons_index_gdf (gpd.GeoDataFrame): Index of available extracts.
        num_of_multiprocessing_workers (int, optional): Number of workers used for multiprocessing.
            Defaults to -1 which results in a total number of available cpu threads.
            `0` and `1` values disable multiprocessing.
            Similar to `n_jobs` parameter from `scikit-learn` library.
        multiprocessing_activation_threshold (int, optional): Number of gometries required to start
            processing on multiple processes. Activating multiprocessing for a small
            amount of points might not be feasible. Defaults to 100.

    Returns:
        List[OpenStreetMapExtract]: List of extracts covering a given geometry.
    """
    if num_of_multiprocessing_workers == 0:
        num_of_multiprocessing_workers = 1
    elif num_of_multiprocessing_workers < 0:
        num_of_multiprocessing_workers = cpu_count()

    if not multiprocessing_activation_threshold:
        multiprocessing_activation_threshold = 100

    unique_extracts_ids: set[str] = set()

    geometries = _flatten_geometry(geometry)

    total_polygons = len(geometries)

    if (
        num_of_multiprocessing_workers > 1
        and total_polygons >= multiprocessing_activation_threshold
    ):
        find_extracts_func = partial(
            _find_smallest_containing_extracts_for_single_geometry,
            polygons_index_gdf=polygons_index_gdf,
        )

        for extract_ids_list in process_map(
            find_extracts_func,
            geometries,
            desc="Finding matching extracts",
            max_workers=num_of_multiprocessing_workers,
            chunksize=ceil(total_polygons / (4 * num_of_multiprocessing_workers)),
        ):
            unique_extracts_ids.update(extract_ids_list)
    else:
        for sub_geometry in geometries:
            unique_extracts_ids.update(
                _find_smallest_containing_extracts_for_single_geometry(
                    sub_geometry, polygons_index_gdf
                )
            )

    extracts_filtered = _filter_extracts(
        geometry,
        unique_extracts_ids,
        polygons_index_gdf,
        num_of_multiprocessing_workers,
        multiprocessing_activation_threshold,
    )

    return extracts_filtered


def _find_smallest_containing_extracts_for_single_geometry(
    geometry: BaseGeometry, polygons_index_gdf: gpd.GeoDataFrame
) -> set[str]:
    """
    Find smallest set of extracts covering a given singular geometry.

    Args:
        geometry (BaseGeometry): Geometry to be covered.
        polygons_index_gdf (gpd.GeoDataFrame): Index of available extracts.

    Raises:
        RuntimeError: If provided extracts index is empty.
        RuntimeError: If there is no extracts covering a given geometry (singularly or in group).

    Returns:
        Set[str]: Selected extract index string values.
    """
    if polygons_index_gdf is None:
        raise RuntimeError("Extracts index is empty.")

    extracts_ids: set[str] = set()
    if geometry.geom_type == "Polygon":
        geometry_to_cover = geometry.buffer(0)
    else:
        geometry_to_cover = geometry.buffer(1e-6)

    exactly_matching_geometry = polygons_index_gdf[
        polygons_index_gdf.geometry.geom_almost_equals(geometry)
    ]
    if len(exactly_matching_geometry) == 1:
        extracts_ids.add(exactly_matching_geometry.iloc[0].id)
        return extracts_ids

    iterations = 100
    while not geometry_to_cover.is_empty and iterations > 0:
        matching_rows = polygons_index_gdf[
            (~polygons_index_gdf["id"].isin(extracts_ids))
            & (polygons_index_gdf.intersects(geometry_to_cover))
        ]
        if 0 in (len(matching_rows), iterations):
            raise RuntimeError("Couldn't find extracts matching given geometry.")

        smallest_extract = matching_rows.iloc[0]
        geometry_to_cover = geometry_to_cover.difference(smallest_extract.geometry)
        extracts_ids.add(smallest_extract.id)
        iterations -= 1
    return extracts_ids


def _filter_extracts(
    geometry: BaseGeometry,
    extracts_ids: Iterable[str],
    polygons_index_gdf: gpd.GeoDataFrame,
    num_of_multiprocessing_workers: int,
    multiprocessing_activation_threshold: int,
) -> list[OpenStreetMapExtract]:
    """
    Filter a set of extracts to include least overlaps in it.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        extracts_ids (Iterable[str]): Group of selected extracts indexes.
        polygons_index_gdf (gpd.GeoDataFrame): Index of available extracts.
        num_of_multiprocessing_workers (int): Number of workers used for multiprocessing.
            Similar to `n_jobs` parameter from `scikit-learn` library.
        multiprocessing_activation_threshold (int): Number of gometries required to start
            processing on multiple processes.

    Raises:
        RuntimeError: If provided extracts index is empty.

    Returns:
        List[OpenStreetMapExtract]: Filtered list of extracts.
    """
    if polygons_index_gdf is None:
        raise RuntimeError("Extracts index is empty.")

    sorted_extracts_gdf = polygons_index_gdf[
        polygons_index_gdf["id"].isin(extracts_ids)
    ].sort_values(by="area", ignore_index=True, ascending=False)

    filtered_extracts: list[OpenStreetMapExtract] = []
    filtered_extracts_ids: set[str] = set()

    geometries = _flatten_geometry(geometry)

    total_geometries = len(geometries)

    if (
        num_of_multiprocessing_workers > 1
        and total_geometries >= multiprocessing_activation_threshold
    ):
        filter_extracts_func = partial(
            _filter_extracts_for_single_geometry,
            sorted_extracts_gdf=sorted_extracts_gdf,
        )

        for extract_ids_list in process_map(
            filter_extracts_func,
            geometries,
            desc="Filtering extracts",
            max_workers=num_of_multiprocessing_workers,
            chunksize=ceil(total_geometries / (4 * num_of_multiprocessing_workers)),
        ):
            filtered_extracts_ids.update(extract_ids_list)
    else:
        for sub_geometry in geometries:
            filtered_extracts_ids.update(
                _filter_extracts_for_single_geometry(sub_geometry, sorted_extracts_gdf)
            )

    simplified_extracts_ids = _simplify_selected_extracts(
        filtered_extracts_ids, sorted_extracts_gdf
    )

    for _, extract_row in sorted_extracts_gdf[
        sorted_extracts_gdf["id"].isin(simplified_extracts_ids)
    ].iterrows():
        extract = OpenStreetMapExtract(
            id=extract_row.id,
            url=extract_row["url"],
            geometry=extract_row.geometry,
        )
        filtered_extracts.append(extract)

    return filtered_extracts


def _filter_extracts_for_single_geometry(
    geometry: BaseGeometry, sorted_extracts_gdf: gpd.GeoDataFrame
) -> set[str]:
    """
    Filter a set of extracts to include least overlaps in it for a single geometry.

    Works by selecting biggest extracts (by area) and not including smaller ones if they don't
    increase a coverage.

    Args:
        geometry (BaseGeometry): Geometry to be covered.
        sorted_extracts_gdf (gpd.GeoDataFrame): Sorted index of available extracts.

    Returns:
        Set[str]: Selected extract index string values.
    """
    filtered_extracts_ids: set[str] = set()

    if geometry.geom_type == "Polygon":
        geometry_to_cover = geometry.buffer(0)
    else:
        geometry_to_cover = geometry.buffer(1e-6)

    for _, extract_row in sorted_extracts_gdf.iterrows():
        if geometry_to_cover.is_empty:
            break

        if extract_row.geometry.disjoint(geometry_to_cover):
            continue

        geometry_to_cover = geometry_to_cover.difference(extract_row.geometry)
        filtered_extracts_ids.add(extract_row.id)

    return filtered_extracts_ids


def _simplify_selected_extracts(
    filtered_extracts_ids: set[str], sorted_extracts_gdf: gpd.GeoDataFrame
) -> set[str]:
    simplified_extracts_ids: set[str] = filtered_extracts_ids.copy()

    matching_extracts = sorted_extracts_gdf[sorted_extracts_gdf["id"].isin(simplified_extracts_ids)]

    simplify_again = True
    while simplify_again:
        simplify_again = False
        extract_to_remove = None
        for extract_id in simplified_extracts_ids:
            extract_geometry = (
                matching_extracts[sorted_extracts_gdf["id"] == extract_id].iloc[0].geometry
            )
            other_geometries = matching_extracts[
                sorted_extracts_gdf["id"] != extract_id
            ].unary_union
            if extract_geometry.covered_by(other_geometries):
                extract_to_remove = extract_id
                simplify_again = True
                break

        if extract_to_remove is not None:
            simplified_extracts_ids.remove(extract_to_remove)

    return simplified_extracts_ids


def _flatten_geometry(geometry: BaseGeometry) -> list[BaseGeometry]:
    """Flatten all geometries into a list of BaseGeometries."""
    if isinstance(geometry, BaseMultipartGeometry):
        geometries = []
        for sub_geom in geometry.geoms:
            geometries.extend(_flatten_geometry(sub_geom))
        return geometries
    return [geometry]
