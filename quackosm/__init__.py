"""
QuackOSM.

QuackOSM is a Python library used for reading pbf (ProtoBuffer) files with OpenStreetMap data using
DuckDB spatial extension without GDAL.
"""

from quackosm.functions import (
    convert_geometry_to_geodataframe,
    convert_geometry_to_parquet,
    convert_pbf_to_geodataframe,
    convert_pbf_to_parquet,
)
from quackosm.geocode import geocode_to_geometry
from quackosm.pbf_file_reader import PbfFileReader

__app_name__ = "QuackOSM"
__version__ = "0.8.2"

__all__ = [
    "PbfFileReader",
    "convert_pbf_to_parquet",
    "convert_geometry_to_parquet",
    "convert_pbf_to_geodataframe",
    "convert_geometry_to_geodataframe",
    "geocode_to_geometry",
]
