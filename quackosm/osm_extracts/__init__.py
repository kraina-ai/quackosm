"""
OpenStreetMap extracts.

This module contains iterators for publically available OpenStreetMap `*.osm.pbf` files
repositories.
"""

import difflib
import os
import warnings
from collections.abc import Iterable
from functools import partial
from math import ceil
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, overload

import geopandas as gpd
from pandas.util._decorators import deprecate
from pooch import retrieve
from rich import get_console
from rich import print as rprint
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry
from tqdm.contrib.concurrent import process_map

from quackosm._exceptions import (
    GeometryNotCoveredError,
    GeometryNotCoveredWarning,
    OsmExtractMultipleMatchesError,
    OsmExtractZeroMatchesError,
)
from quackosm.osm_extracts.bbbike import _get_bbbike_index
from quackosm.osm_extracts.extract import OpenStreetMapExtract, OsmExtractSource
from quackosm.osm_extracts.extracts_tree import get_available_extracts_as_rich_tree
from quackosm.osm_extracts.geofabrik import _get_geofabrik_index
from quackosm.osm_extracts.osm_fr import _get_openstreetmap_fr_index

if TYPE_CHECKING: # pragma: no cover
    import pandas as pd

__all__ = [
    "download_extracts_pbf_files",
    "find_smallest_containing_extracts_total",
    "find_smallest_containing_geofabrik_extracts",
    "find_smallest_containing_openstreetmap_fr_extracts",
    "find_smallest_containing_bbbike_extracts",
    "get_extract_by_query",
    "download_extract_by_query",
    "display_available_extracts",
    "OsmExtractSource",
]


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
            fname=f"{extract.file_name}.osm.pbf",
            path=download_directory,
            progressbar=True,
            known_hash=None,
        )
        downloaded_extracts_paths.append(Path(file_path))
    return downloaded_extracts_paths


OSM_EXTRACT_SOURCE_INDEX_FUNCTION = {
    OsmExtractSource.bbbike: _get_bbbike_index,
    OsmExtractSource.geofabrik: _get_geofabrik_index,
    OsmExtractSource.osm_fr: _get_openstreetmap_fr_index,
}


def _get_combined_index() -> gpd.GeoDataFrame:
    combined_index = gpd.pd.concat(
        [get_index_function() for get_index_function in OSM_EXTRACT_SOURCE_INDEX_FUNCTION.values()]
    )
    combined_index.sort_values(by="area", ignore_index=True, inplace=True)
    return combined_index


@overload
def get_extract_by_query(query: str) -> OpenStreetMapExtract: ...


@overload
def get_extract_by_query(
    query: str, source: Union[OsmExtractSource, str]
) -> OpenStreetMapExtract: ...


def get_extract_by_query(
    query: str, source: Union[OsmExtractSource, str] = "any"
) -> OpenStreetMapExtract:
    """
    Find an OSM extract by name.

    Args:
        query (str): Query to search for a particular extract.
        source (Union[OsmExtractSource, str]): OSM source name. Can be one of: 'any', 'Geofabrik',
            'BBBike', 'OSM_fr'. Defaults to 'any'.

    Returns:
        OpenStreetMapExtract: Found extract.
    """
    try:
        source_enum = OsmExtractSource(source)
        index = OSM_EXTRACT_SOURCE_INDEX_FUNCTION.get(source_enum, _get_combined_index)()

        matching_index_row: pd.Series = None

        file_name_matched_rows = (index["file_name"].str.lower() == query.lower().strip()) | (
            index["file_name"].str.replace("_", " ").str.lower()
            == query.lower().replace("_", " ").strip()
        )
        extract_name_matched_rows = (index["name"].str.lower() == query.lower().strip()) | (
            index["name"].str.replace("_", " ").str.lower()
            == query.lower().replace("_", " ").strip()
        )

        # full file name matched
        if sum(file_name_matched_rows) == 1:
            matching_index_row = index[file_name_matched_rows].iloc[0]
        # single name matched
        elif sum(extract_name_matched_rows) == 1:
            matching_index_row = index[extract_name_matched_rows].iloc[0]
        # multiple names matched
        elif extract_name_matched_rows.any():
            matching_rows = index[extract_name_matched_rows]
            matching_full_names = sorted(matching_rows["file_name"])
            full_names = ", ".join(f'"{full_name}"' for full_name in matching_full_names)

            raise OsmExtractMultipleMatchesError(
                f'Multiple extracts matched by query "{query.strip()}".\n'
                f"Matching extracts full names: {full_names}.",
                matching_full_names=matching_full_names,
            )
        # zero names matched
        elif not extract_name_matched_rows.any():
            matching_full_names = []
            suggested_query_names = difflib.get_close_matches(
                query.lower(), index["name"].str.lower().unique(), n=5, cutoff=0.7
            )

            if suggested_query_names:
                for suggested_query_name in suggested_query_names:
                    found_extracts = index[index["name"].str.lower() == suggested_query_name]
                    matching_full_names.extend(found_extracts["file_name"])
                full_names = ", ".join(matching_full_names)
                full_names = ", ".join(f'"{full_name}"' for full_name in matching_full_names)
                exception_message = (
                    f'Zero extracts matched by query "{query}".\n'
                    f"Found full names close to query: {full_names}."
                )
            else:
                exception_message = (
                    f'Zero extracts matched by query "{query}".\n'
                    "Zero close matches have been found."
                )

            raise OsmExtractZeroMatchesError(
                exception_message,
                matching_full_names=matching_full_names,
            )

        return OpenStreetMapExtract(
            id=matching_index_row["id"],
            name=matching_index_row["name"],
            parent=matching_index_row["parent"],
            url=matching_index_row["url"],
            geometry=matching_index_row["geometry"],
            file_name=matching_index_row["file_name"],
        )

    except ValueError as ex:
        raise ValueError(f"Unknown OSM extracts source: {source}.") from ex


@overload
def download_extract_by_query(query: str) -> Path: ...


@overload
def download_extract_by_query(query: str, source: Union[OsmExtractSource, str]) -> Path: ...


def download_extract_by_query(
    query: str,
    source: Union[OsmExtractSource, str] = "any",
    download_directory: Union[str, Path] = "files",
) -> Path:
    """
    Download an OSM extract by name.

    Args:
        query (str): Query to search for a particular extract.
        source (Union[OsmExtractSource, str]): OSM source name. Can be one of: 'any', 'Geofabrik',
            'BBBike', 'OSM_fr'. Defaults to 'any'.
        download_directory (Union[str, Path], optional): Directory where the file should be
            downloaded. Defaults to "files".

    Returns:
        Path: Path to the downloaded OSM extract.
    """
    matching_extract = get_extract_by_query(query, source)
    return download_extracts_pbf_files([matching_extract], Path(download_directory))[0]


def display_available_extracts(
    source: Union[OsmExtractSource, str],
    use_full_names: bool = True,
    use_pager: bool = False,
) -> None:
    """
    Display all available OSM extracts in the form of a tree.

    Output will be printed to the console.

    Args:
        source (Union[OsmExtractSource, str]): Source for which extracts should be displayed.
        use_full_names (bool, optional): Whether to display full name, or short name of the extract.
            Full name contains all parents of the extract. Defaults to `True`.
        use_pager (bool, optional): Whether to display long output using Rich pager
            or just print to output. Defaults to `False`.

    Raises:
        ValueError: If provided source value cannot be parsed to OsmExtractSource.
    """
    try:
        source_enum = OsmExtractSource(source)
        tree = get_available_extracts_as_rich_tree(
            source_enum, OSM_EXTRACT_SOURCE_INDEX_FUNCTION, use_full_names
        )
        if not use_pager:
            rprint(tree)
        else:
            console = get_console()
            with console.pager():
                console.print(tree)
    except ValueError as ex:
        raise ValueError(f"Unknown OSM extracts source: {source}.") from ex


def find_smallest_containing_extracts_total(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from all OSM extract indexes that contains given polygon.

    Iterates all indexes and finds smallest extracts that covers a given geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(
        geometry=geometry,
        polygons_index_gdf=_get_combined_index(),
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
    )


def find_smallest_containing_geofabrik_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from Geofabrik that contains given geometry.

    Iterates a geofabrik index and finds smallest extracts that covers a given geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(
        geometry=geometry,
        polygons_index_gdf=OSM_EXTRACT_SOURCE_INDEX_FUNCTION[OsmExtractSource.geofabrik](),
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
    )


def find_smallest_containing_openstreetmap_fr_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from OpenStreetMap.fr that contains given polygon.

    Iterates an osm.fr index and finds smallest extracts that covers a given geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(
        geometry=geometry,
        polygons_index_gdf=OSM_EXTRACT_SOURCE_INDEX_FUNCTION[OsmExtractSource.osm_fr](),
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
    )


def find_smallest_containing_bbbike_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from BBBike that contains given polygon.

    Iterates an BBBike index and finds smallest extracts that covers a given geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    return _find_smallest_containing_extracts(
        geometry=geometry,
        polygons_index_gdf=OSM_EXTRACT_SOURCE_INDEX_FUNCTION[OsmExtractSource.bbbike](),
        geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        allow_uncovered_geometry=allow_uncovered_geometry,
    )


def find_smallest_containing_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    source: Union[OsmExtractSource, str],
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest extracts from a given OSM source that contains given polygon.

    Iterates an OSM source index and finds smallest extracts that covers a given geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (Union[BaseGeometry, BaseMultipartGeometry]): Geometry to be covered.
        source (Union[OsmExtractSource, str]): OSM source name. Can be one of: 'any', 'Geofabrik',
            'BBBike', 'OSMfr'.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Returns:
        List[OpenStreetMapExtract]: List of extracts name, URL to download it and boundary polygon.
    """
    try:
        source_enum = OsmExtractSource(source)
        index = OSM_EXTRACT_SOURCE_INDEX_FUNCTION.get(source_enum, _get_combined_index)()
        return _find_smallest_containing_extracts(
            geometry=geometry,
            polygons_index_gdf=index,
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
            allow_uncovered_geometry=allow_uncovered_geometry,
        )
    except ValueError as ex:
        raise ValueError(f"Unknown OSM extracts source: {source}.") from ex


def _find_smallest_containing_extracts(
    geometry: Union[BaseGeometry, BaseMultipartGeometry],
    polygons_index_gdf: gpd.GeoDataFrame,
    num_of_multiprocessing_workers: int = -1,
    multiprocessing_activation_threshold: Optional[int] = None,
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> list[OpenStreetMapExtract]:
    """
    Find smallest set of extracts covering a given geometry.

    Iterates a provided extracts index and searches for a smallest set that cover a given geometry.
    It's not guaranteed that this set will be the smallest and there will be no overlaps.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

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
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

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
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
            allow_uncovered_geometry=allow_uncovered_geometry,
        )

        force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"
        for extract_ids_list in process_map(
            find_extracts_func,
            geometries,
            desc="Finding matching extracts",
            max_workers=num_of_multiprocessing_workers,
            chunksize=ceil(total_polygons / (4 * num_of_multiprocessing_workers)),
            disable=True if force_terminal else False,
        ):
            unique_extracts_ids.update(extract_ids_list)
    else:
        for sub_geometry in geometries:
            unique_extracts_ids.update(
                _find_smallest_containing_extracts_for_single_geometry(
                    geometry=sub_geometry,
                    polygons_index_gdf=polygons_index_gdf,
                    geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
                    allow_uncovered_geometry=allow_uncovered_geometry,
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
    geometry: BaseGeometry,
    polygons_index_gdf: gpd.GeoDataFrame,
    geometry_coverage_iou_threshold: float = 0.01,
    allow_uncovered_geometry: bool = False,
) -> set[str]:
    """
    Find smallest set of extracts covering a given singular geometry.

    Extracts are selected based on the highest value of the Intersection over Union metric with
    geometry. Some extracts might be discarded because of low IoU metric value leaving some parts
    of the geometry uncovered.

    Args:
        geometry (BaseGeometry): Geometry to be covered.
        polygons_index_gdf (gpd.GeoDataFrame): Index of available extracts.
        geometry_coverage_iou_threshold (float): Minimal value of the Intersection over Union metric
            for selecting the matching OSM extracts. Is best matching extract has value lower than
            the threshold, it is discarded (except the first one). Has to be in range between
            0 and 1. Value of 0 will allow every intersected extract, value of 1 will only allow
            extracts that match the geometry exactly. Defaults to 0.01.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Raises:
        RuntimeError: If provided extracts index is empty.
        RuntimeError: If there is no extracts covering a given geometry (singularly or in group).
        ValueError: If geometry_coverage_iou_threshold is outside bounds [0, 1].

    Returns:
        Set[str]: Selected extract index string values.
    """
    if polygons_index_gdf is None:
        raise RuntimeError("Extracts index is empty.")

    if geometry_coverage_iou_threshold < 0 or geometry_coverage_iou_threshold > 1:
        raise ValueError("geometry_coverage_iou_threshold is outside required bounds [0, 1]")

    checked_extracts_ids, iou_metric_values = _cover_geometry_with_extracts(
        geometry=geometry,
        polygons_index_gdf=polygons_index_gdf,
        allow_uncovered_geometry=allow_uncovered_geometry,
    )

    selected_extracts_ids: set[str] = set()
    for extract_id, iou_metric_value in zip(checked_extracts_ids, iou_metric_values):
        if iou_metric_value >= geometry_coverage_iou_threshold or not selected_extracts_ids:
            selected_extracts_ids.add(extract_id)
        else:
            skipped_extract = polygons_index_gdf[polygons_index_gdf.id == extract_id].iloc[0]
            warnings.warn(
                (
                    "Skipping extract because of low IoU value "
                    f"({skipped_extract.file_name}, {iou_metric_value:.3g})."
                ),
                GeometryNotCoveredWarning,
                stacklevel=0,
            )

    return selected_extracts_ids


def _cover_geometry_with_extracts(
    geometry: BaseGeometry,
    polygons_index_gdf: gpd.GeoDataFrame,
    allow_uncovered_geometry: bool = False,
) -> tuple[list[str], list[float]]:
    """
    Intersect a geometry with extracts and return the IoU coverage.

    Args:
        geometry (BaseGeometry): Geometry to be covered.
        polygons_index_gdf (gpd.GeoDataFrame): Index of available extracts.
        allow_uncovered_geometry (bool): Suppress an error if some geometry parts aren't covered
            by any OSM extract. Defaults to `False`.

    Raises:
        RuntimeError: If provided extracts index is empty.
        RuntimeError: If there is no extracts covering a given geometry (singularly or in group).

    Returns:
        tuple[list[str], list[float]]: List of extracts index string values with a list
            of IoU metric values.
    """
    if polygons_index_gdf is None:
        raise RuntimeError("Extracts index is empty.")

    checked_extracts_ids: list[str] = []
    iou_metric_values: list[float] = []

    if geometry.geom_type == "Polygon":
        geometry_to_cover = geometry.buffer(0)
    else:
        geometry_to_cover = geometry.buffer(1e-6)

    exactly_matching_geometry = polygons_index_gdf.loc[
        polygons_index_gdf.geometry.geom_equals_exact(geometry, tolerance=1e-6)
    ]
    if len(exactly_matching_geometry) == 1:
        checked_extracts_ids.append(exactly_matching_geometry.iloc[0].id)
        iou_metric_values.append(1.0)
        return checked_extracts_ids, iou_metric_values

    while not geometry_to_cover.is_empty:
        matching_rows = polygons_index_gdf.loc[
            (~polygons_index_gdf["id"].isin(checked_extracts_ids))
            & (polygons_index_gdf.intersects(geometry_to_cover))
        ]
        if not len(matching_rows):
            if not allow_uncovered_geometry:
                raise GeometryNotCoveredError(
                    "Couldn't find extracts covering given geometry."
                    " If it's expected behaviour, you can suppress this error by passing"
                    " the `allow_uncovered_geometry=True` argument"
                    " or add `--allow-uncovered-geometry` flag to the CLI command."
                )
            warnings.warn(
                "Couldn't find extracts covering given geometry.",
                GeometryNotCoveredWarning,
                stacklevel=0,
            )
            break

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            geometry_intersection_area = matching_rows.geometry.intersection(geometry_to_cover).area
            matching_rows["iou_metric"] = geometry_intersection_area / (
                matching_rows.geometry.area + geometry_to_cover.area - geometry_intersection_area
            )

        best_matching_extract = matching_rows.sort_values(
            by=["iou_metric", "area"], ascending=[False, True]
        ).iloc[0]
        geometry_to_cover = geometry_to_cover.difference(best_matching_extract.geometry)
        checked_extracts_ids.append(best_matching_extract.id)
        iou_metric_values.append(best_matching_extract.iou_metric)

    return checked_extracts_ids, iou_metric_values


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

    sorted_extracts_gdf = polygons_index_gdf.loc[
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

        force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"
        for extract_ids_list in process_map(
            filter_extracts_func,
            geometries,
            desc="Filtering extracts",
            max_workers=num_of_multiprocessing_workers,
            chunksize=ceil(total_geometries / (4 * num_of_multiprocessing_workers)),
            disable=True if force_terminal else False,
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

    for extract_row in sorted_extracts_gdf.loc[
        sorted_extracts_gdf["id"].isin(simplified_extracts_ids)
    ].to_dict(orient="records"):
        extract = OpenStreetMapExtract(
            id=extract_row["id"],
            name=extract_row["name"],
            parent=extract_row["parent"],
            url=extract_row["url"],
            geometry=extract_row["geometry"],
            file_name=extract_row["file_name"],
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

    matching_extracts = sorted_extracts_gdf.loc[
        sorted_extracts_gdf["id"].isin(simplified_extracts_ids)
    ]

    simplify_again = True
    while simplify_again:
        simplify_again = False
        extract_to_remove = None
        for extract_id in simplified_extracts_ids:
            extract_geometry = (
                matching_extracts.loc[sorted_extracts_gdf["id"] == extract_id].iloc[0].geometry
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                other_geometries = matching_extracts.loc[
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


find_smallest_containing_extract = deprecate(
    "find_smallest_containing_extract",
    find_smallest_containing_extracts,
    "0.9.0",
    msg="Use `find_smallest_containing_extracts` instead. Deprecated since 0.9.0 version.",
)
