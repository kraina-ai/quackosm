"""
OpenStreetMap.fr extracts.

This module contains wrapper for publically available OpenStreetMap.fr download server.
"""

import re
from typing import Any, Optional

import geopandas as gpd
import requests
from tqdm import tqdm

from quackosm._rich_progress import FORCE_TERMINAL
from quackosm.osm_extracts._poly_parser import parse_polygon_file
from quackosm.osm_extracts.extract import (
    OpenStreetMapExtract,
    OsmExtractSource,
    extracts_to_geodataframe,
    load_index_decorator,
)

OPENSTREETMAP_FR_POLYGONS_INDEX_URL = "https://download.openstreetmap.fr/polygons"
OPENSTREETMAP_FR_EXTRACTS_INDEX_URL = "https://download.openstreetmap.fr/extracts"
OPENSTREETMAP_FR_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_openstreetmap_fr_index"]


def _get_openstreetmap_fr_index() -> gpd.GeoDataFrame:
    global OPENSTREETMAP_FR_INDEX_GDF  # noqa: PLW0603

    if OPENSTREETMAP_FR_INDEX_GDF is None:
        OPENSTREETMAP_FR_INDEX_GDF = _load_openstreetmap_fr_index()

    return OPENSTREETMAP_FR_INDEX_GDF


@load_index_decorator(OsmExtractSource.osm_fr)
def _load_openstreetmap_fr_index() -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available extracts from OpenStreetMap.fr download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    extracts = []
    with tqdm(disable=FORCE_TERMINAL) as pbar:
        extract_soup_objects = _gather_all_openstreetmap_fr_urls(
            OsmExtractSource.osm_fr.value, "/", pbar
        )
        pbar.set_description(OsmExtractSource.osm_fr.value)
        extracts = _parse_openstreetmap_fr_urls(
            pbar=pbar, extract_soup_objects=extract_soup_objects
        )

    gdf = extracts_to_geodataframe(extracts)

    return gdf


def _gather_all_openstreetmap_fr_urls(
    id_prefix: str, directory_url: str, pbar: tqdm
) -> list[Any]:  # pragma: no cover
    """
    Iterate OpenStreetMap.fr extracts service page.

    Works recursively, by scraping whole available directory.

    Args:
        id_prefix (str): Prefix to be applies to extracts names.
        directory_url (str): Directory URL to load.
        pbar (tqdm): Progress bar.

    Returns:
        list[Any]: List of osm.fr extracts urls objects for further processing.
    """
    from bs4 import BeautifulSoup

    pbar.set_description_str(id_prefix)
    extract_soup_objects = []

    result = requests.get(
        f"{OPENSTREETMAP_FR_EXTRACTS_INDEX_URL}{directory_url}",
        headers={"User-Agent": "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"},
    )
    soup = BeautifulSoup(result.text, "html.parser")

    extracts_urls = soup.find_all(string=re.compile("-latest\\.osm\\.pbf$"))
    pbar.total = (pbar.total or 0) + len(extracts_urls)
    pbar.refresh()
    extract_soup_objects.extend(
        [(extract_url, id_prefix, directory_url) for extract_url in extracts_urls]
    )

    directories = soup.find_all(src="/icons/folder.gif")
    for directory in directories:
        link = directory.find_parent("tr").find("a")
        name = link.text.replace("/", "")
        extract_soup_objects.extend(
            _gather_all_openstreetmap_fr_urls(
                id_prefix=f"{id_prefix}_{name}",
                directory_url=f"{directory_url}{link['href']}",
                pbar=pbar,
            )
        )

    return extract_soup_objects


def _parse_openstreetmap_fr_urls(
    pbar: tqdm, extract_soup_objects: list[Any]
) -> list[OpenStreetMapExtract]:
    extracts = []

    for soup_object, id_prefix, directory_url in extract_soup_objects:
        link = soup_object.find_parent("tr").find("a")
        name = link.text.replace("-latest.osm.pbf", "")
        polygon = parse_polygon_file(
            f"{OPENSTREETMAP_FR_POLYGONS_INDEX_URL}/{directory_url}{name}.poly"
        )
        if polygon is None:
            continue
        extracts.append(
            OpenStreetMapExtract(
                id=f"{id_prefix}_{name}",
                name=name,
                parent=id_prefix,
                url=f"{OPENSTREETMAP_FR_EXTRACTS_INDEX_URL}{directory_url}{link['href']}",
                geometry=polygon,
            )
        )
        pbar.set_description_str(id_prefix)
        pbar.update()

    return extracts
