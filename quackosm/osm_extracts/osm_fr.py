"""
OpenStreetMap.fr extracts.

This module contains wrapper for publically available OpenStreetMap.fr download server.
"""

import os
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import requests
from tqdm import tqdm

from quackosm._constants import WGS84_CRS
from quackosm.osm_extracts._poly_parser import parse_polygon_file
from quackosm.osm_extracts.extract import OpenStreetMapExtract

OPENSTREETMAP_FR_POLYGONS_INDEX_URL = "https://download.openstreetmap.fr/polygons"
OPENSTREETMAP_FR_EXTRACTS_INDEX_URL = "https://download.openstreetmap.fr/extracts"
OPENSTREETMAP_FR_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_openstreetmap_fr_index"]


def _get_openstreetmap_fr_index() -> gpd.GeoDataFrame:
    global OPENSTREETMAP_FR_INDEX_GDF  # noqa: PLW0603

    if OPENSTREETMAP_FR_INDEX_GDF is None:
        OPENSTREETMAP_FR_INDEX_GDF = _load_openstreetmap_fr_index()

    return OPENSTREETMAP_FR_INDEX_GDF


def _load_openstreetmap_fr_index() -> gpd.GeoDataFrame:
    """
    Load available extracts from OpenStreetMap.fr download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    save_path = Path("cache/osm_fr_index.geojson")

    if save_path.exists():
        gdf = gpd.read_file(save_path)
    else:
        force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"
        extracts = []
        with tqdm(disable=True if force_terminal else None) as pbar:
            extract_soup_objects = _gather_all_openstreetmap_fr_urls("osm_fr", "/", pbar)
            pbar.set_description("osm_fr")
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
                        url=f"{OPENSTREETMAP_FR_EXTRACTS_INDEX_URL}{directory_url}{link['href']}",
                        geometry=polygon,
                    )
                )
                pbar.update()
        gdf = gpd.GeoDataFrame(
            data=[asdict(extract) for extract in extracts], geometry="geometry"
        ).set_crs(WGS84_CRS)
        gdf["area"] = gdf.geometry.area
        gdf.sort_values(by="area", ignore_index=True, inplace=True)

        save_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(save_path, driver="GeoJSON")

    return gdf


def _gather_all_openstreetmap_fr_urls(id_prefix: str, directory_url: str, pbar: tqdm) -> list[Any]:
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
