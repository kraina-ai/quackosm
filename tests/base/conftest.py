"""Common components for tests."""

from pathlib import Path

from shapely import to_geojson, to_wkt
from shapely.geometry import Polygon, box

from quackosm.conftest import download_osm_extracts_indexes

__all__ = [
    "download_osm_extracts_indexes",
    "geometry_box",
    "geometry_wkt",
    "geometry_geojson",
    "geometry_boundary_file_path",
]


def geometry_box() -> Polygon:
    """Geometry box."""
    return box(
        minx=7.416486207767861,
        miny=43.7310867041912,
        maxx=7.421931388477276,
        maxy=43.73370705597216,
    )


def geometry_wkt() -> str:
    """Geometry box in WKT form."""
    return str(to_wkt(geometry_box()))


def geometry_geojson() -> str:
    """Geometry box in GeoJSON form."""
    return str(to_geojson(geometry_box()))


def geometry_boundary_file_path() -> str:
    """Geometry Monaco boundary file path."""
    return str(Path(__file__).parent.parent / "test_files" / "monaco_boundary.geojson")
