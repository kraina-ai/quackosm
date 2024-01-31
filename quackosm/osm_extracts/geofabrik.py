"""
Geofabrik OpenStreetMap extracts.

This module contains wrapper for publically available Geofabrik download server.
"""

import json
from pathlib import Path
from typing import Optional

import geopandas as gpd
import requests

GEOFABRIK_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
GEOFABRIK_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_geofabrik_index"]

def _get_geofabrik_index() -> gpd.GeoDataFrame:
    global GEOFABRIK_INDEX_GDF  # noqa: PLW0603

    if GEOFABRIK_INDEX_GDF is None:
        GEOFABRIK_INDEX_GDF = _load_geofabrik_index()

    return GEOFABRIK_INDEX_GDF


def _load_geofabrik_index() -> gpd.GeoDataFrame:
    """
    Load available extracts from GeoFabrik download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    save_path = Path("cache/geofabrik_index.geojson")

    if save_path.exists():
        gdf = gpd.read_file(save_path)
    else:
        result = requests.get(
            GEOFABRIK_INDEX_URL,
            headers={"User-Agent": "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"},
        )
        parsed_data = json.loads(result.text)
        gdf = gpd.GeoDataFrame.from_features(parsed_data["features"])
        gdf["area"] = gdf.geometry.area
        gdf.sort_values(by="area", ignore_index=True, inplace=True)
        gdf["url"] = gdf["urls"].apply(lambda d: d["pbf"])
        gdf["id"] = "Geofabrik_" + gdf["id"]
        gdf = gdf[["id", "name", "geometry", "area", "url"]]

        save_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(save_path, driver="GeoJSON")

    return gdf
