"""
GEO2day OpenStreetMap extracts.

This module contains wrapper for the publically available GEO2day download server (
https://geo2day.com/).
Each region is described by a GeoJSON boundary file.
"""

from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import geopandas as gpd
import requests
from tqdm import tqdm

from quackosm._rich_progress import FORCE_TERMINAL
from quackosm.osm_extracts._geojson_parser import parse_geojson_file
from quackosm.osm_extracts.extract import (
    OpenStreetMapExtract,
    OsmExtractSource,
    extracts_to_geodataframe,
    load_index_decorator,
)

GEO2DAY_BASE_URL = "https://geo2day.com/"
GEO2DAY_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

_USER_AGENT = "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"

__all__ = ["_get_geo2day_index"]


def _get_geo2day_index(**kwargs: Any) -> gpd.GeoDataFrame:
    global GEO2DAY_INDEX_GDF  # noqa: PLW0603

    if GEO2DAY_INDEX_GDF is None:
        GEO2DAY_INDEX_GDF = _load_geo2day_index(**kwargs)

    return GEO2DAY_INDEX_GDF


@load_index_decorator(OsmExtractSource.geo2day)
def _load_geo2day_index(**kwargs: Any) -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available extracts from the GEO2day download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    extracts = []
    with tqdm(disable=FORCE_TERMINAL) as pbar:
        region_objects = _gather_all_geo2day_urls(
            OsmExtractSource.geo2day.value, GEO2DAY_BASE_URL, pbar
        )
        pbar.set_description(OsmExtractSource.geo2day.value)
        extracts = _parse_geo2day_urls(pbar=pbar, region_objects=region_objects)

    gdf = extracts_to_geodataframe(extracts)

    return gdf


def _region_path_segments(url: str) -> list[str]:
    """Return the region path segments for a GEO2day page URL (without the `.html` suffix)."""
    path = urlparse(url).path.strip("/").removesuffix(".html")
    return [segment for segment in path.split("/") if segment]


def _find_subregion_links(page_url: str, soup: Any) -> list[tuple[str, str]]:
    """
    Find direct sub-region links on a GEO2day page.

    A link is a direct sub-region when its region path is exactly one segment deeper than the
    current page's region path and shares its prefix. This naturally excludes breadcrumb links
    (home, parent, self).

    Args:
        page_url (str): URL of the currently processed page.
        soup (Any): Parsed BeautifulSoup object of the page.

    Returns:
        list[tuple[str, str]]: List of (absolute `.html` URL, region name) pairs.
    """
    current_segments = _region_path_segments(page_url)
    subregion_links: list[tuple[str, str]] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not href.endswith(".html"):
            continue

        absolute_url = urljoin(page_url, href)
        if absolute_url in seen_urls:
            continue

        child_segments = _region_path_segments(absolute_url)
        is_direct_child = (
            len(child_segments) == len(current_segments) + 1
            and child_segments[: len(current_segments)] == current_segments
        )
        if not is_direct_child:
            continue

        seen_urls.add(absolute_url)
        subregion_links.append((absolute_url, child_segments[-1]))

    return subregion_links


def _gather_all_geo2day_urls(
    id_prefix: str, page_url: str, pbar: tqdm
) -> list[Any]:  # pragma: no cover
    """
    Iterate GEO2day extracts service page.

    Works recursively, by scraping whole available hierarchy. Only HTML pages are fetched here
    to discover the regions and grow the progress bar total; geometries are downloaded later.

    Args:
        id_prefix (str): Prefix to be applied to extracts ids.
        page_url (str): Page URL to load.
        pbar (tqdm): Progress bar.

    Returns:
        list[Any]: List of geo2day region url objects for further processing.
    """
    from bs4 import BeautifulSoup

    pbar.set_description_str(id_prefix)
    region_objects = []

    result = requests.get(page_url, headers={"User-Agent": _USER_AGENT})
    result.raise_for_status()
    soup = BeautifulSoup(result.text, "html.parser")

    subregion_links = _find_subregion_links(page_url, soup)
    pbar.total = (pbar.total or 0) + len(subregion_links)
    pbar.refresh()

    for child_html_url, child_name in subregion_links:
        child_id = f"{id_prefix}_{child_name}"
        base_url = child_html_url[: -len(".html")]
        region_objects.append((child_id, child_name, id_prefix, base_url))
        region_objects.extend(
            _gather_all_geo2day_urls(
                id_prefix=child_id,
                page_url=child_html_url,
                pbar=pbar,
            )
        )

    return region_objects


def _parse_geo2day_urls(pbar: tqdm, region_objects: list[Any]) -> list[OpenStreetMapExtract]:
    extracts = []

    for region_id, name, id_prefix, base_url in region_objects:
        geometry = parse_geojson_file(f"{base_url}.geojson")
        if geometry is None:
            continue
        extracts.append(
            OpenStreetMapExtract(
                id=region_id,
                name=name,
                parent=id_prefix,
                url=f"{base_url}.pbf",
                geometry=geometry,
            )
        )
        pbar.set_description_str(id_prefix)
        pbar.update()

    return extracts
