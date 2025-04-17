"""Geocoding module for getting a geometry from query using Nominatim."""

import rq_geo_toolkit.geocode  # noqa: E402, I001

rq_geo_toolkit.geocode.USER_AGENT = (
    "QuackOSM Python package (https://github.com/kraina-ai/quackosm)"
)
from rq_geo_toolkit.geocode import geocode_to_geometry  # noqa: E402, I001

__all__ = ["geocode_to_geometry"]
