"""
Movisda OpenStreetMap extracts.

This module contains wrappers for the publically available Movisda download server (
https://osm.download.movisda.io/).
Two sources are exposed: administrative boundaries (`Movisda-admin`)
and a regular geographic grid (`Movisda-grid`).
For both, a single GeoJSON file describes all available extracts and their geometries.
"""

from typing import Any, Optional

import geopandas as gpd
import requests

from quackosm._constants import OSM_EXTRACTS_REQUEST_TIMEOUT_SECONDS
from quackosm.osm_extracts.extract import (
    OpenStreetMapExtract,
    OsmExtractSource,
    extracts_to_geodataframe,
    load_index_decorator,
)

MOVISDA_ADMIN_GEOJSON_URL = "https://osm.download.movisda.io/admin/Admin-latest.geojson"
MOVISDA_ADMIN_PBF_BASE_URL = "https://osm.download.movisda.io/admin"
MOVISDA_GRID_GEOJSON_URL = "https://osm.download.movisda.io/grid/grid-latest.geojson"
MOVISDA_GRID_PBF_BASE_URL = "https://osm.download.movisda.io/grid"

MOVISDA_ADMIN_INDEX_GDF: Optional[gpd.GeoDataFrame] = None
MOVISDA_GRID_INDEX_GDF: Optional[gpd.GeoDataFrame] = None

_USER_AGENT = "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"

__all__ = ["_get_movisda_admin_index", "_get_movisda_grid_index"]


def _get_movisda_admin_index(**kwargs: Any) -> gpd.GeoDataFrame:
    global MOVISDA_ADMIN_INDEX_GDF  # noqa: PLW0603

    if MOVISDA_ADMIN_INDEX_GDF is None:
        MOVISDA_ADMIN_INDEX_GDF = _load_movisda_admin_index(**kwargs)

    return MOVISDA_ADMIN_INDEX_GDF


def _get_movisda_grid_index(**kwargs: Any) -> gpd.GeoDataFrame:
    global MOVISDA_GRID_INDEX_GDF  # noqa: PLW0603

    if MOVISDA_GRID_INDEX_GDF is None:
        MOVISDA_GRID_INDEX_GDF = _load_movisda_grid_index(**kwargs)

    return MOVISDA_GRID_INDEX_GDF


@load_index_decorator(OsmExtractSource.movisda_admin)
def _load_movisda_admin_index(**kwargs: Any) -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available administrative extracts from the Movisda download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    extracts = _iterate_movisda_geojson(
        geojson_url=MOVISDA_ADMIN_GEOJSON_URL,
        pbf_base_url=MOVISDA_ADMIN_PBF_BASE_URL,
        source_enum=OsmExtractSource.movisda_admin,
    )
    return extracts_to_geodataframe(extracts)


@load_index_decorator(OsmExtractSource.movisda_grid)
def _load_movisda_grid_index(**kwargs: Any) -> gpd.GeoDataFrame:  # pragma: no cover
    """
    Load available grid extracts from the Movisda download service.

    Returns:
        gpd.GeoDataFrame: Extracts index with metadata.
    """
    extracts = _iterate_movisda_geojson(
        geojson_url=MOVISDA_GRID_GEOJSON_URL,
        pbf_base_url=MOVISDA_GRID_PBF_BASE_URL,
        source_enum=OsmExtractSource.movisda_grid,
    )
    return extracts_to_geodataframe(extracts)


def _iterate_movisda_geojson(
    geojson_url: str, pbf_base_url: str, source_enum: OsmExtractSource
) -> list[OpenStreetMapExtract]:  # pragma: no cover
    """
    Parse a Movisda GeoJSON index into a list of extracts.

    The download URL is built from the `prefix` property of each feature (already containing the
    trailing dash, e.g. `AD-`) by appending `latest.osm.pbf`.

    Args:
        geojson_url (str): URL of the GeoJSON index file.
        pbf_base_url (str): Base URL for the PBF files.
        source_enum (OsmExtractSource): Source enum value used for ids and parent.

    Returns:
        list[OpenStreetMapExtract]: List of parsed extracts.
    """
    result = requests.get(
        geojson_url,
        headers={"User-Agent": _USER_AGENT},
        timeout=OSM_EXTRACTS_REQUEST_TIMEOUT_SECONDS,
    )
    result.raise_for_status()

    extracts = _parse_movisda_features(
        result.json(),
        pbf_base_url,
        source_enum.value,
        build_hierarchy=source_enum == OsmExtractSource.movisda_admin,
    )

    return extracts


def _parse_movisda_features(
    geojson_data: dict[str, Any],
    pbf_base_url: str,
    source_enum_value: str,
    build_hierarchy: bool,
) -> list[OpenStreetMapExtract]:
    """
    Build extracts from a parsed Movisda GeoJSON FeatureCollection.

    The download URL and id are built from the `prefix` property (already containing the trailing
    dash, e.g. `AD-`), by appending `latest.osm.pbf` / stripping the dash.

    For admin boundaries (``build_hierarchy=True``) the extracts are geographically nested, so the
    parent is derived from the ISO-style code structure - e.g. `RW-02` is nested under `RW`, which
    is nested under the source root. This keeps full names unique even when subdivisions share a
    name across countries (e.g. multiple "Eastern Province").

    For the grid (``build_hierarchy=False``) the tile code already encodes the resolution (10°
    tiles carry a `-10` suffix), so the code is used directly as the name and the extracts stay
    flat under the source root.

    Args:
        geojson_data (dict[str, Any]): Parsed GeoJSON content.
        pbf_base_url (str): Base URL for the PBF files.
        source_enum_value (str): Source enum value used for ids and parents.
        build_hierarchy (bool): Whether to derive a parent hierarchy from the code structure.

    Returns:
        list[OpenStreetMapExtract]: List of parsed extracts.
    """
    from shapely.geometry import shape

    features = geojson_data.get("features", [])
    available_codes = {str(feature["properties"]["prefix"]).rstrip("-") for feature in features}

    extracts = []
    for feature in features:
        properties = feature["properties"]
        prefix = str(properties["prefix"])
        code = prefix.rstrip("-")
        geometry = shape(feature["geometry"])

        if build_hierarchy:
            name = properties.get("name_en") or properties["name"]
            # Take the immediate (smallest) parent from the ISO-style code, e.g. `RW-02` -> `RW`.
            parent_code = code.rsplit("-", 1)[0] if "-" in code else None
            parent = (
                f"{source_enum_value}_{parent_code}"
                if parent_code and parent_code in available_codes
                else source_enum_value
            )
        else:
            name = code
            parent = source_enum_value

        extracts.append(
            OpenStreetMapExtract(
                id=f"{source_enum_value}_{code}",
                name=name,
                parent=parent,
                url=f"{pbf_base_url}/{prefix}latest.osm.pbf",
                geometry=geometry,
            )
        )

    return extracts
