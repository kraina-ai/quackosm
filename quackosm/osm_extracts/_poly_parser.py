"""Poly file parser function."""

from typing import Any, Optional

import requests
from shapely.geometry import MultiPolygon

__all__ = ["parse_polygon_file"]


def parse_polygon_file(polygon_url: str) -> Optional[MultiPolygon]:  # pragma: no cover
    """
    Parse poly file from URL to geometry.

    Args:
        polygon_url (str): URL to load a poly file.

    Returns:
        Optional[MultiPolygon]: Parsed polygon.
            Empty if request returns 404 not found.
    """
    result = requests.get(
        polygon_url,
        headers={"User-Agent": "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"},
    )
    if result.status_code == 404:
        return None
    result.raise_for_status()
    poly = parse_poly(result.text.splitlines())
    return poly


def parse_poly(lines: list[str]) -> MultiPolygon:  # pragma: no cover
    """
    Parse an Osmosis polygon filter file.

    Accept a sequence of lines from a polygon file, return a shapely.geometry.MultiPolygon object.
    Based on: https://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Python_Parsing

    http://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Format
    """
    in_ring = False
    coords: list[Any] = []

    for index, line in enumerate(lines):
        if index == 0:
            # first line is junk.
            continue

        elif index == 1:
            # second line is the first polygon ring.
            coords.append([[], []])
            ring = coords[-1][0]
            in_ring = True

        elif in_ring and line.strip() == "END":
            # we are at the end of a ring, perhaps with more to come.
            in_ring = False

        elif in_ring:
            # we are in a ring and picking up new coordinates.
            ring.append(list(map(float, line.split())))

        elif not in_ring and line.strip() == "END":
            # we are at the end of the whole polygon.
            break

        elif not in_ring and line.startswith("!"):
            # we are at the start of a polygon part hole.
            coords[-1][1].append([])
            ring = coords[-1][1][-1]
            in_ring = True

        elif not in_ring:
            # we are at the start of a polygon part.
            coords.append([[], []])
            ring = coords[-1][0]
            in_ring = True

    return MultiPolygon(coords)
