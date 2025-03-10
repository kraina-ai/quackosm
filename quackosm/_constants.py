"""Constants used across the project."""

import os

WGS84_CRS = "EPSG:4326"

FEATURES_INDEX = "feature_id"

GEOMETRY_COLUMN = "geometry"

PARQUET_ROW_GROUP_SIZE = 100_000

FORCE_TERMINAL = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"
