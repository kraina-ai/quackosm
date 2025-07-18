"""
BBBike OpenStreetMap extracts.

This module contains wrapper for publically available BBBike download server.
"""

from typing import Optional

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

BBBIKE_EXTRACTS_INDEX_URL = "https://download.bbbike.org/osm/bbbike"
BBBIKE_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_bbbike_index"]


def _get_bbbike_index() -> gpd.GeoDataFrame:
    global BBBIKE_INDEX_GDF  # noqa: PLW0603

    if BBBIKE_INDEX_GDF is None:
        BBBIKE_INDEX_GDF = _load_bbbike_index()

    return BBBIKE_INDEX_GDF


@load_index_decorator(OsmExtractSource.bbbike)
def _load_bbbike_index() -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available extracts from BBBike download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    extracts = _iterate_bbbike_index()
    gdf = extracts_to_geodataframe(extracts)

    return gdf


def _iterate_bbbike_index() -> list[OpenStreetMapExtract]:  # pragma: no cover
    """
    Iterate OpenStreetMap.fr extracts service page.

    Works recursively, by scraping whole available directory.

    Returns:
        List[OpenStreetMapExtract]: List of loaded osm.fr extracts objects.
    """
    from bs4 import BeautifulSoup

    extracts = []
    result = requests.get(
        BBBIKE_EXTRACTS_INDEX_URL,
        headers={"User-Agent": "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"},
    )
    soup = BeautifulSoup(result.text, "html.parser")
    extract_names = [
        extract_href.text
        for extract_href in soup.select("tr.d > td > a")
        if extract_href.text != ".."
    ]

    bbbike_enum_value = OsmExtractSource.bbbike.value

    with tqdm(disable=FORCE_TERMINAL, desc=bbbike_enum_value, total=len(extract_names)) as pbar:
        for extract_name in extract_names:
            pbar.set_description(f"{bbbike_enum_value}_{extract_name}")
            poly_url = f"{BBBIKE_EXTRACTS_INDEX_URL}/{extract_name}/{extract_name}.poly"
            polygon = parse_polygon_file(poly_url)
            if polygon is None:
                continue
            pbf_url = f"{BBBIKE_EXTRACTS_INDEX_URL}/{extract_name}/{extract_name}.osm.pbf"
            extracts.append(
                OpenStreetMapExtract(
                    id=f"{bbbike_enum_value}_{extract_name}",
                    name=extract_name,
                    parent=bbbike_enum_value,
                    url=pbf_url,
                    geometry=polygon,
                )
            )
            pbar.update()

    return extracts
