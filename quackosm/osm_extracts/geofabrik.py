"""
Geofabrik OpenStreetMap extracts.

This module contains wrapper for publically available Geofabrik download server.
"""

import json
import operator
from typing import Optional

import geopandas as gpd
import requests

from quackosm.osm_extracts.extract import OsmExtractSource, load_index_decorator

GEOFABRIK_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
GEOFABRIK_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

__all__ = ["_get_geofabrik_index"]


def _get_geofabrik_index() -> gpd.GeoDataFrame:
    global GEOFABRIK_INDEX_GDF  # noqa: PLW0603

    if GEOFABRIK_INDEX_GDF is None:
        GEOFABRIK_INDEX_GDF = _load_geofabrik_index()

    return GEOFABRIK_INDEX_GDF


@load_index_decorator(OsmExtractSource.geofabrik)
def _load_geofabrik_index() -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available extracts from GeoFabrik download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    result = requests.get(
        GEOFABRIK_INDEX_URL,
        headers={"User-Agent": "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"},
    )
    parsed_data = json.loads(result.text)
    gdf = gpd.GeoDataFrame.from_features(parsed_data["features"])
    gdf["url"] = gdf["urls"].apply(operator.itemgetter("pbf"))
    gdf["name"] = gdf["id"].str.replace("/", "_")
    gdf["parent"] = gdf["parent"].fillna(OsmExtractSource.geofabrik.value)

    # fix US extracts parent tree
    gdf.loc[gdf["id"].str.startswith("us/"), "parent"] = "us"

    return gdf
