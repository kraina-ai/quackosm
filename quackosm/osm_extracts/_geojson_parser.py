"""GeoJSON file parser function."""

from typing import TYPE_CHECKING, Any, Optional

import requests

from quackosm._constants import OSM_EXTRACTS_REQUEST_TIMEOUT_SECONDS

if TYPE_CHECKING:  # pragma: no cover
    from shapely.geometry.base import BaseGeometry

__all__ = ["parse_geojson_file"]

_USER_AGENT = "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"


def parse_geojson_file(geojson_url: str) -> Optional["BaseGeometry"]:  # pragma: no cover
    """
    Parse a GeoJSON file from URL into a single geometry describing the region extent.

    Args:
        geojson_url (str): URL to load a GeoJSON file.

    Returns:
        Optional[BaseGeometry]: Parsed geometry, or `None` if the request returns 404 not found
            or the file contains no geometry.
    """
    result = requests.get(
        geojson_url,
        headers={"User-Agent": _USER_AGENT},
        timeout=OSM_EXTRACTS_REQUEST_TIMEOUT_SECONDS,
    )
    if result.status_code == 404:
        return None
    result.raise_for_status()
    return parse_geojson(result.json())


def parse_geojson(data: dict[str, Any]) -> Optional["BaseGeometry"]:
    """
    Parse a parsed GeoJSON object into a single geometry.

    Supports a `FeatureCollection` (geometries are merged), a single `Feature`,
    or a raw geometry object.

    Args:
        data (dict[str, Any]): Parsed GeoJSON content.

    Returns:
        Optional[BaseGeometry]: Combined geometry, or `None` if there is no geometry.
    """
    from shapely.geometry import shape

    geojson_type = data.get("type")

    if geojson_type == "FeatureCollection":
        geometries = [
            shape(feature["geometry"])
            for feature in data.get("features", [])
            if feature.get("geometry") is not None
        ]
        if not geometries:
            return None
        if len(geometries) == 1:
            return geometries[0]
        from shapely.ops import unary_union

        return unary_union(geometries)

    if geojson_type == "Feature":
        geometry = data.get("geometry")
        return shape(geometry) if geometry is not None else None

    return shape(data)
