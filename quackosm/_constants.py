"""Constants used across the project."""

from rq_geo_toolkit.constants import (
    GEOMETRY_COLUMN,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_VERSION,
)

WGS84_CRS = "EPSG:4326"

FEATURES_INDEX = "feature_id"

METADATA_TAGS_TO_IGNORE = [
    "area",
    "created_by",
    "converted_by",
    "source",
    "time",
    "ele",
    "note",
    "todo",
    "fixme",
    "FIXME",
    "openGeoDB:",
]

__all__ = [
    "FEATURES_INDEX",
    "GEOMETRY_COLUMN",
    "PARQUET_COMPRESSION",
    "PARQUET_COMPRESSION_LEVEL",
    "PARQUET_ROW_GROUP_SIZE",
    "PARQUET_VERSION",
    "WGS84_CRS",
]
