import json
from typing import Literal

from rq_geo_toolkit.duckdb import sql_escape


def get_geoparquet_metadata(
    geometry_types: list[str],
    bbox: tuple[float, float, float, float],
    encoding: Literal["WKB", "WKT"],
) -> str:
    from quackosm import __app_name__, __version__
    from quackosm._constants import GEOMETRY_COLUMN

    geo_metadata = {
        "version": "1.1.0",
        "primary_column": GEOMETRY_COLUMN,
        "columns": {
            GEOMETRY_COLUMN: {
                "encoding": encoding,
                "crs": _CRS_LONLAT,
                "geometry_types": geometry_types,
                "bbox": list(bbox),
            }
        },
        "creator": {"library": __app_name__, "version": __version__},
    }
    escaped_geo_metadata = sql_escape(json.dumps(geo_metadata))

    return "{ " + f"'geo': '{escaped_geo_metadata}'" + " }"


_CRS_LONLAT = {
    "$schema": "https://proj.org/schemas/v0.5/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84 longitude-latitude",
    "datum": {
        "type": "GeodeticReferenceFrame",
        "name": "World Geodetic System 1984",
        "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
        },
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}
