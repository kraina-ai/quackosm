"""
QuackOSM.

QuackOSM is a Python library used for reading pbf (ProtoBuffer) files with OpenStreetMap data using
DuckDB spatial extension without GDAL.
"""

from quackosm.functions import convert_pbf_to_gpq, get_features_gdf
from quackosm.pbf_file_reader import PbfFileReader

__app_name__ = "QuackOSM"
__version__ = "0.3.1"

__all__ = [
    "PbfFileReader",
    "convert_pbf_to_gpq",
    "get_features_gdf",
]
