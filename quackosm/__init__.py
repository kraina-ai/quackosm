"""
QuackOSM.

QuackOSM is a Python library used for reading pbf (ProtoBuffer) files with OpenStreetMap data using
DuckDB spatial extension without GDAL.
"""

from quackosm.functions import (
    convert_geometry_to_gpq,
    convert_pbf_to_gpq,
    get_features_gdf,
    get_features_gdf_from_geometry,
)
from quackosm.pbf_file_reader import PbfFileReader

__app_name__ = "QuackOSM"
__version__ = "0.4.1"

__all__ = [
    "PbfFileReader",
    "convert_pbf_to_gpq",
    "convert_geometry_to_gpq",
    "get_features_gdf",
    "get_features_gdf_from_geometry",
]
