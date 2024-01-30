"""
BBBike OpenStreetMap extracts.

This module contains wrapper for publically available BBBike download server.
"""

from dataclasses import asdict
from pathlib import Path
from typing import Optional

import geopandas as gpd
import requests
from tqdm import tqdm

from quackosm.osm_extracts._poly_parser import parse_polygon_file
from quackosm.osm_extracts.extract import OpenStreetMapExtract

BBBIKE_EXTRACTS_INDEX_URL = "https://download.bbbike.org/osm/bbbike"
BBBIKE_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_bbbike_index"]


def _get_bbbike_index() -> gpd.GeoDataFrame:
    global BBBIKE_INDEX_GDF  # noqa: PLW0603

    if BBBIKE_INDEX_GDF is None:
        BBBIKE_INDEX_GDF = _load_bbbike_index()

    return BBBIKE_INDEX_GDF


def _load_bbbike_index() -> gpd.GeoDataFrame:
    """
    Load available extracts from BBBike download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    save_path = Path("cache/bbbike_index.geojson")

    if save_path.exists():
        gdf = gpd.read_file(save_path)
    else:
        extracts = _iterate_bbbike_index()
        gdf = gpd.GeoDataFrame(
            data=[asdict(extract) for extract in extracts], geometry="geometry"
        ).set_crs("EPSG:4326")
        gdf["area"] = gdf.geometry.area
        gdf.sort_values(by="area", ignore_index=True, inplace=True)

        save_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(save_path, driver="GeoJSON")

    return gdf


def _iterate_bbbike_index() -> list[OpenStreetMapExtract]:
    """
    Iterate OpenStreetMap.fr extracts service page.

    Works recursively, by scraping whole available directory.

    Args:
        id_prefix (str): Prefix to be applies to extracts names.
        directory_url (str): Directory URL to load.
        return_extracts (bool): Whether to return collected extracts or not.
        pbar (tqdm): Progress bar.

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

    for extract_name in tqdm(extract_names, desc="Iterating BBBike index"):
        poly_url = f"{BBBIKE_EXTRACTS_INDEX_URL}/{extract_name}/{extract_name}.poly"
        polygon = parse_polygon_file(poly_url)
        if polygon is None:
            continue
        pbf_url = f"{BBBIKE_EXTRACTS_INDEX_URL}/{extract_name}/{extract_name}.osm.pbf"
        extracts.append(
            OpenStreetMapExtract(
                id=f"BBBike_{extract_name}",
                url=pbf_url,
                geometry=polygon,
            )
        )

    return extracts
