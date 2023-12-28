# type: ignore
# pragma: no cover
"""
GeoArrow IO helper module.

Will be removed after new version of https://github.com/geoarrow/geoarrow-python/ is released.
Those two functions: `read_geoparquet_table` and `write_geoparquet_table` are required in
QuackOSM library and this file is composed of files from current unreleased version of the library.
"""
import json

import geoarrow.pyarrow as _ga
import pyarrow.parquet as _pq
import pyarrow.types as _types
import pyarrow_hotfix as _  # noqa: F401
from geoarrow.pyarrow._compute import ensure_storage


def read_geoparquet_table(*args, **kwargs):
    """Read GeoParquet using PyArrow."""
    tab = _pq.read_table(*args, **kwargs)
    tab_metadata = tab.schema.metadata or {}
    if b"geo" in tab_metadata:
        geo_meta = json.loads(tab_metadata[b"geo"])
    else:
        geo_meta = {}

    # Remove "geo" schema metadata key since few transformations following
    # the read operation check that schema metadata is valid (e.g., column
    # subset or rename)
    non_geo_meta = {k: v for k, v in tab_metadata.items() if k != b"geo"}
    tab = tab.replace_schema_metadata(non_geo_meta)

    # Assign extension types to columns
    if "columns" in geo_meta:
        columns = geo_meta["columns"]
    else:
        columns = _geoparquet_guess_geometry_columns(tab.schema)

    return _geoparquet_table_to_geoarrow(tab, columns)


def write_geoparquet_table(
    table,
    *args,
    primary_geometry_column=None,
    geometry_columns=None,
    write_bbox=False,
    write_geometry_types=None,
    check_wkb=True,
    **kwargs,
):
    """Write GeoParquet using PyArrow."""
    geo_meta = _geoparquet_metadata_from_schema(
        table.schema,
        primary_geometry_column=primary_geometry_column,
        geometry_columns=geometry_columns,
        add_geometry_types=write_geometry_types,
    )

    # Note: this will also update geo_meta with geometry_types and bbox if requested
    for i, name in enumerate(table.schema.names):
        if name in geo_meta["columns"]:
            table = table.set_column(
                i,
                name,
                _geoparquet_encode_chunked_array(
                    table[i],
                    geo_meta["columns"][name],
                    add_geometry_types=write_geometry_types,
                    add_bbox=write_bbox,
                    check_wkb=check_wkb,
                ),
            )

    metadata = table.schema.metadata or {}
    metadata["geo"] = json.dumps(geo_meta)
    table = table.replace_schema_metadata(metadata)
    return _pq.write_table(table, *args, **kwargs)


def _geoparquet_guess_geometry_columns(schema):
    # Only attempt guessing the "geometry" or "geography" column
    columns = {}

    for name in ("geometry", "geography"):
        if name not in schema.names:
            continue

        spec = {}
        type = schema.field(name).type

        if _types.is_binary(type) or _types.is_large_binary(type):
            spec["encoding"] = "WKB"
        elif _types.is_string(type) or _types.is_large_string(type):
            # WKT is not actually a geoparquet encoding but the guidance on
            # putting geospatial things in parquet without metadata says you
            # can do it and this is the internal sentinel for that case.
            spec["encoding"] = "WKT"

        # A column named "geography" has spherical edges according to the
        # compatible Parquet guidance.
        if name == "geography":
            spec["edges"] = "spherical"

        columns[name] = spec

    return columns


def _geoparquet_chunked_array_to_geoarrow(item, spec):
    # If item was written as a GeoArrow extension type to the Parquet file,
    # ignore any information in the column spec
    if isinstance(item.type, _ga.GeometryExtensionType):
        return item

    if "encoding" not in spec:
        raise ValueError("Invalid GeoParquet column specification: missing 'encoding'")

    encoding = spec["encoding"]
    if encoding in ("WKB", "WKT"):
        item = _ga.array(item)
    else:
        raise ValueError(f"Invalid GeoParquet encoding value: '{encoding}'")

    if "crs" not in spec:
        crs = json.dumps(_CRS_LONLAT).encode("UTF-8")
    else:
        crs = json.dumps(spec["crs"]).encode("UTF-8")

    if "edges" not in spec or spec["edges"] == "planar":
        edge_type = None
    elif spec["edges"] == "spherical":
        edge_type = _ga.EdgeType.SPHERICAL
    else:
        raise ValueError("Invalid GeoParquet column edges value")

    if crs is not None:
        item = _ga.with_crs(item, crs)

    if edge_type is not None:
        item = _ga.with_edge_type(item, edge_type)

    return item


def _geoparquet_table_to_geoarrow(tab, columns):
    tab_names = set(tab.schema.names)
    for col_name, spec in columns.items():
        # col_name might not exist if only a subset of columns were read from file
        if col_name not in tab_names:
            continue

        col_i = tab.schema.get_field_index(col_name)
        new_geometry = _geoparquet_chunked_array_to_geoarrow(tab[col_i], spec)
        tab = tab.set_column(col_i, col_name, new_geometry)

    return tab


def _geoparquet_guess_primary_geometry_column(schema, primary_geometry_column=None):
    if primary_geometry_column is not None:
        return primary_geometry_column

    # If there's a "geometry" or "geography" column, pick that one
    if "geometry" in schema.names:
        return "geometry"
    elif "geography" in schema.names:
        return "geography"

    # Otherwise, pick the first thing we know is actually geometry
    for name, type in zip(schema.names, schema.types):
        if isinstance(type, _ga.GeometryExtensionType):
            return name

    raise ValueError("write_geoparquet_table() requires source with at least one geometry column")


def _geoparquet_column_spec_from_type(type, add_geometry_types=None):
    # We always encode to WKB since it's the only supported value
    spec = {"encoding": "WKB", "geometry_types": []}

    # Pass along extra information from GeoArrow extension type metadata
    if isinstance(type, _ga.GeometryExtensionType):
        if type.crs_type == _ga.CrsType.PROJJSON:
            spec["crs"] = json.loads(type.crs)
        elif type.crs_type == _ga.CrsType.NONE:
            spec["crs"] = None
        else:
            import pyproj

            spec["crs"] = pyproj.CRS(type.crs).to_json_dict()

        if type.edge_type == _ga.EdgeType.SPHERICAL:
            spec["edges"] = "spherical"

        # GeoArrow-encoded types can confidently declare a single geometry type
        maybe_known_geometry_type = type.geometry_type
        maybe_known_dimensions = type.dimensions
        if (
            add_geometry_types is not False
            and maybe_known_geometry_type != _ga.GeometryType.GEOMETRY
            and maybe_known_dimensions != _ga.Dimensions.UNKNOWN
        ):
            geometry_type = _GEOPARQUET_GEOMETRY_TYPE_LABELS[maybe_known_geometry_type]
            dimensions = _GEOPARQUET_DIMENSION_LABELS[maybe_known_dimensions]
            spec["geometry_types"] = [f"{geometry_type}{dimensions}"]

    return spec


def _geoparquet_columns_from_schema(
    schema, geometry_columns=None, primary_geometry_column=None, add_geometry_types=None
):
    schema_names = schema.names
    schema_types = schema.types

    if geometry_columns is None:
        geometry_columns = set()
        if primary_geometry_column is not None:
            geometry_columns.add(primary_geometry_column)

        for name, type in zip(schema_names, schema_types):
            if isinstance(type, _ga.GeometryExtensionType):
                geometry_columns.add(name)
    else:
        geometry_columns = set(geometry_columns)

    specs = {}
    for name, type in zip(schema_names, schema_types):
        if name in geometry_columns:
            specs[name] = _geoparquet_column_spec_from_type(
                type, add_geometry_types=add_geometry_types
            )

    return specs


def _geoparquet_metadata_from_schema(
    schema, geometry_columns=None, primary_geometry_column=None, add_geometry_types=None
):
    primary_geometry_column = _geoparquet_guess_primary_geometry_column(
        schema, primary_geometry_column
    )
    columns = _geoparquet_columns_from_schema(
        schema, geometry_columns, add_geometry_types=add_geometry_types
    )
    return {
        "version": "1.0.0",
        "primary_column": primary_geometry_column,
        "columns": columns,
    }


def _geoparquet_update_spec_geometry_types(item, spec):
    geometry_type_labels = []
    for element in _ga.unique_geometry_types(item).to_pylist():
        geometry_type = _GEOPARQUET_GEOMETRY_TYPE_LABELS[element["geometry_type"]]
        dimensions = _GEOPARQUET_DIMENSION_LABELS[element["dimensions"]]
        geometry_type_labels.append(f"{geometry_type}{dimensions}")

    spec["geometry_types"] = geometry_type_labels


def _geoparquet_update_spec_bbox(item, spec):
    box = _ga.box_agg(item).as_py()
    spec["bbox"] = [box["xmin"], box["ymin"], box["xmax"], box["ymax"]]


def _geoparquet_encode_chunked_array(
    item, spec, add_geometry_types=None, add_bbox=False, check_wkb=True
):
    # ...because we're currently only ever encoding using WKB
    if spec["encoding"] == "WKB":
        item_out = _ga.as_wkb(item)
    else:
        encoding = spec["encoding"]
        raise ValueError(f"Expected column encoding 'WKB' but got '{encoding}'")

    # For everything except a well-known text-encoded column, we want to do
    # calculations on the pre-WKB-encoded value.
    if spec["encoding"] == "WKT":
        item_calc = item_out
    else:
        item_calc = item

    # geometry_types that are fixed at the data type level have already been
    # added to the spec in an earlier step. The unique_geometry_types()
    # function is sufficiently optimized such that this potential
    # re-computation is not expensive.
    if add_geometry_types is True:
        _geoparquet_update_spec_geometry_types(item_calc, spec)

    if add_bbox:
        _geoparquet_update_spec_bbox(item_calc, spec)

    return ensure_storage(item_out)


_GEOPARQUET_GEOMETRY_TYPE_LABELS = [
    "Geometry",
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
]

_GEOPARQUET_DIMENSION_LABELS = [None, "", " Z", " M", " ZM"]

_CRS_LONLAT = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [
            {
                "name": "World Geodetic System 1984 (Transit)",
                "id": {"authority": "EPSG", "code": 1166},
            },
            {
                "name": "World Geodetic System 1984 (G730)",
                "id": {"authority": "EPSG", "code": 1152},
            },
            {
                "name": "World Geodetic System 1984 (G873)",
                "id": {"authority": "EPSG", "code": 1153},
            },
            {
                "name": "World Geodetic System 1984 (G1150)",
                "id": {"authority": "EPSG", "code": 1154},
            },
            {
                "name": "World Geodetic System 1984 (G1674)",
                "id": {"authority": "EPSG", "code": 1155},
            },
            {
                "name": "World Geodetic System 1984 (G1762)",
                "id": {"authority": "EPSG", "code": 1156},
            },
            {
                "name": "World Geodetic System 1984 (G2139)",
                "id": {"authority": "EPSG", "code": 1309},
            },
        ],
        "ellipsoid": {
            "name": "WGS 84",
            "semi_major_axis": 6378137,
            "inverse_flattening": 298.257223563,
        },
        "accuracy": "2.0",
        "id": {"authority": "EPSG", "code": 6326},
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
    "scope": "Not known.",
    "area": "World.",
    "bbox": {
        "south_latitude": -90,
        "west_longitude": -180,
        "north_latitude": 90,
        "east_longitude": 180,
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}
