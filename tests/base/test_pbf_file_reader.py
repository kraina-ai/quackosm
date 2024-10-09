"""Tests for PbfFileReader."""

import json
import random
import urllib.request
import warnings
from functools import partial
from itertools import permutations
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union, cast
from unittest import TestCase

import duckdb
import geoarrow.pyarrow as ga
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parametrization import Parametrization as P
from pytest_mock import MockerFixture
from shapely import from_wkt, get_coordinates, hausdorff_distance
from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    box,
    polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from quackosm import (
    convert_geometry_to_parquet,
    convert_pbf_to_geodataframe,
    convert_pbf_to_parquet,
    functions,
)
from quackosm._constants import FEATURES_INDEX, GEOMETRY_COLUMN, WGS84_CRS
from quackosm._exceptions import (
    GeometryNotCoveredError,
    GeometryNotCoveredWarning,
    InvalidGeometryFilter,
)
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.cli import (
    GeocodeGeometryParser,
    GeohashGeometryParser,
    H3GeometryParser,
    S2GeometryParser,
)
from quackosm.osm_extracts import OsmExtractSource
from quackosm.pbf_file_reader import PbfFileReader
from tests.base.conftest import GEOFABRIK_LAYERS, HEX2VEC_FILTER, geometry_box

ut = TestCase()
LFS_DIRECTORY_URL = "https://github.com/kraina-ai/srai-test-files/raw/main/files/"


@pytest.mark.parametrize("tags_filter", [None, HEX2VEC_FILTER, GEOFABRIK_LAYERS])  # type: ignore
@pytest.mark.parametrize("explode_tags", [None, True, False])  # type: ignore
@pytest.mark.parametrize("keep_all_tags", [True, False])  # type: ignore
@pytest.mark.parametrize("save_as_wkt", [True, False])  # type: ignore
def test_pbf_to_geoparquet_parsing(
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]],
    explode_tags: Optional[bool],
    keep_all_tags: bool,
    save_as_wkt: bool,
):
    """Test if pbf to geoparquet conversion works."""
    pbf_file = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    result = PbfFileReader(tags_filter=tags_filter).convert_pbf_to_parquet(
        pbf_path=pbf_file,
        ignore_cache=True,
        explode_tags=explode_tags,
        keep_all_tags=keep_all_tags,
        save_as_wkt=save_as_wkt,
    )

    if save_as_wkt:
        tab = pq.read_table(result)
        assert tab.column("geometry").type == ga.wkt()
    else:
        tab = pq.read_table(result)
        assert b"geo" in tab.schema.metadata

        decoded_geo_schema = json.loads(tab.schema.metadata[b"geo"].decode("utf-8"))
        assert GEOMETRY_COLUMN == decoded_geo_schema["primary_column"]
        assert GEOMETRY_COLUMN in decoded_geo_schema["columns"]


@pytest.mark.parametrize(
    "result_file_path",
    [None, "quackosm.db", "files/quackosm.db", f"files/{random.getrandbits(128)}/quackosm.db"],
) # type: ignore
@pytest.mark.parametrize("table_name", [None, "quackosm", "osm_features"]) # type: ignore
def test_pbf_reader_duckdb_export(result_file_path: Optional[str], table_name: Optional[str]):
    """Test proper DuckDB export file generation."""
    pbf_file = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    result_path = PbfFileReader().convert_pbf_to_duckdb(
        pbf_path=pbf_file,
        result_file_path=result_file_path,
        duckdb_table_name=table_name,
        ignore_cache=True,
    )

    assert result_path.exists(), "DuckDB file doesn't exist"
    with duckdb.connect(str(result_path)) as con:
        existing_tables = [row[0] for row in con.sql("SHOW TABLES;").fetchall()]
        assert table_name or "quackosm" in existing_tables

    result_path.unlink()


def test_pbf_reader_url_path():  # type: ignore
    """Test proper URL detection in `PbfFileReader`."""
    file_name = "https://download.geofabrik.de/europe/monaco-latest.osm.pbf"
    features_gdf = PbfFileReader().convert_pbf_to_geodataframe(
        pbf_path=file_name, explode_tags=True, ignore_cache=True
    )
    assert len(features_gdf) > 0


def test_pbf_reader_geometry_filtering():  # type: ignore
    """Test proper spatial data filtering in `PbfFileReader`."""
    file_name = "d17f922ed15e9609013a6b895e1e7af2d49158f03586f2c675d17b760af3452e.osm.pbf"
    features_gdf = PbfFileReader(
        tags_filter=HEX2VEC_FILTER, geometry_filter=Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
    ).convert_pbf_to_geodataframe(
        pbf_path=[Path(__file__).parent.parent / "test_files" / file_name],
        explode_tags=True,
        ignore_cache=True,
    )
    assert len(features_gdf) == 0


@pytest.mark.parametrize(
    "geometry",
    [
        geometry_box(),
        from_wkt(
            "POLYGON ((-43.064 29.673, -43.064 29.644, -43.017 29.644,"
            " -43.017 29.673, -43.064 29.673))"
        ),
    ],
)  # type: ignore
def test_geometry_hash_calculation(geometry: BaseGeometry):
    """Test if geometry hash is orientation-agnostic."""
    if isinstance(geometry, Polygon):
        oriented_a = polygon.orient(geometry, sign=1.0)
        oriented_b = polygon.orient(geometry, sign=-1.0)
    elif isinstance(geometry, LinearRing):
        oriented_a = geometry
        oriented_b = LinearRing(list(geometry.coords)[::-1])

    assert (
        PbfFileReader(geometry_filter=oriented_a)._get_oriented_geometry_filter()
        == PbfFileReader(geometry_filter=oriented_b)._get_oriented_geometry_filter()
    )

    assert (
        PbfFileReader(geometry_filter=oriented_a)._generate_geometry_hash()
        == PbfFileReader(geometry_filter=oriented_b)._generate_geometry_hash()
    )


@pytest.mark.parametrize("verbosity_mode", ["silent", "transient", "verbose"])  # type: ignore
def test_verbosity_mode(verbosity_mode: Literal["silent", "transient", "verbose"]) -> None:
    """Test if runs properly with different verbosity modes."""
    pbf_file = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    convert_pbf_to_parquet(
        pbf_file,
        geometry_filter=GeocodeGeometryParser().convert("Monaco-Ville, Monaco"),  # type: ignore
        tags_filter={"amenity": True},
        verbosity_mode=verbosity_mode,
        ignore_cache=True,
    )


def test_multipart_geometry_hash_calculation() -> None:
    """Test if geometry hash is orientation-agnostic."""
    geom_1 = geometry_box()
    geom_2 = box(minx=0, miny=0, maxx=1, maxy=1)
    geom_3 = GeocodeGeometryParser().convert("Monaco-Ville, Monaco")  # type: ignore
    geom_4 = H3GeometryParser().convert("8a3969a40ac7fff,893969a4037ffff")  # type: ignore

    geoms = [
        GeometryCollection(combination)
        for combination in permutations([geom_1, geom_2, geom_3, geom_4], 4)
    ]

    hashes = [
        PbfFileReader(geometry_filter=geom_filter)._generate_geometry_hash()
        for geom_filter in geoms
    ]

    assert all(i == hashes[0] for i in hashes)


def test_unique_osm_ids_duplicated_file():  # type: ignore
    """Test if function returns results without duplicated features."""
    monaco_file_path = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    result_gdf = PbfFileReader().convert_pbf_to_geodataframe(
        pbf_path=[monaco_file_path, monaco_file_path], ignore_cache=True
    )

    single_result_gdf = PbfFileReader().convert_pbf_to_geodataframe(
        pbf_path=[monaco_file_path], ignore_cache=True
    )

    assert result_gdf.index.is_unique
    assert len(result_gdf.index) == len(single_result_gdf.index)


def test_unique_osm_ids_real_example():  # type: ignore
    """Test if function returns results without duplicated features."""
    andorra_geometry = from_wkt(
        "POLYGON ((1.382599544073372 42.67676873293743, 1.382599544073372 42.40065303248514,"
        " 1.8092269635579328 42.40065303248514, 1.8092269635579328 42.67676873293743,"
        " 1.382599544073372 42.67676873293743))"
    )
    result_gdf = PbfFileReader(
        geometry_filter=andorra_geometry, osm_extract_source=OsmExtractSource.any
    ).convert_geometry_to_geodataframe(ignore_cache=True)

    assert result_gdf.index.is_unique


def test_antwerpen_and_brussels_invalid_linear_ring() -> None:
    """Test if properly filters out invalid linear rings."""
    antwerpen_and_brussels_geometry = from_wkt(
        "POLYGON ((4.331278527313799 51.173447782908625,"
        " 4.331278527313799 50.89211829585622, 4.413826045759777 50.89211829585622,"
        " 4.413826045759777 51.173447782908625, 4.331278527313799 51.173447782908625))"
    )

    result_gdf = PbfFileReader(
        geometry_filter=antwerpen_and_brussels_geometry, osm_extract_source=OsmExtractSource.bbbike
    ).convert_geometry_to_geodataframe(ignore_cache=True)

    assert result_gdf.index.is_unique


@pytest.mark.parametrize("operation_mode", ["gdf", "gpq"])  # type: ignore
@pytest.mark.parametrize("patch_methods", [0, 1, 2])  # type: ignore
def test_combining_files_different_techniques(
    mocker: MockerFixture, operation_mode: str, patch_methods: int
) -> None:
    """Test if all files merging techniques work as expected in debug mode."""
    if patch_methods > 0:
        # Leave _drop_duplicated_features_in_joined_table as backup
        mocker.patch(
            "quackosm.pbf_file_reader.PbfFileReader._drop_duplicated_features_in_pyarrow_table",
            side_effect=pa.ArrowInvalid(),
        )

    if patch_methods > 1:
        # Leave _drop_duplicated_features_in_joined_table_one_by_one as backup
        mocker.patch(
            "quackosm.pbf_file_reader.PbfFileReader._drop_duplicated_features_in_joined_table",
            side_effect=MemoryError(),
        )

    if operation_mode == "gdf":
        monaco_file_path = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
        result_gdf = convert_pbf_to_geodataframe(
            pbf_path=[
                monaco_file_path,
                monaco_file_path,
            ],
            ignore_cache=True,
            debug_memory=True,
            debug_times=True,
            verbosity_mode="verbose",
        )
        single_result_gdf = convert_pbf_to_geodataframe(
            pbf_path=[monaco_file_path], ignore_cache=True
        )

        assert result_gdf.index.is_unique
        assert len(result_gdf.index) == len(single_result_gdf.index)
    elif operation_mode == "gpq":
        result = convert_geometry_to_parquet(
            geometry_filter=from_wkt(
                "POLYGON ((4.331278527313799 51.173447782908625,"
                " 4.331278527313799 50.89211829585622, 4.413826045759777 50.89211829585622,"
                " 4.413826045759777 51.173447782908625, 4.331278527313799 51.173447782908625))"
            ),
            osm_extract_source="BBBike",
            ignore_cache=True,
            debug_memory=True,
            debug_times=True,
            verbosity_mode="verbose",
        )
        assert gpd.read_parquet(result).set_index("feature_id").index.is_unique

        tab = pq.read_table(result)
        assert b"geo" in tab.schema.metadata

        decoded_geo_schema = json.loads(tab.schema.metadata[b"geo"].decode("utf-8"))
        assert GEOMETRY_COLUMN == decoded_geo_schema["primary_column"]
        assert GEOMETRY_COLUMN in decoded_geo_schema["columns"]
    else:
        raise ValueError("Wrong operation_mode value.")


def test_schema_unification_real_example():  # type: ignore
    """
    Test if function returns results with unified schema without errors.

    Extracted from issue https://github.com/kraina-ai/quackosm/issues/42
    """
    geo = box(minx=-85.904275, miny=38.056361, maxx=-85.502994, maxy=38.383253)
    tags: OsmTagsFilter = {"military": ["airfield"], "leisure": ["park"]}
    result_gdf = PbfFileReader(
        tags_filter=tags, geometry_filter=geo, osm_extract_source=OsmExtractSource.geofabrik
    ).convert_geometry_to_geodataframe(explode_tags=True)
    assert result_gdf.index.is_unique


@pytest.mark.parametrize(  # type: ignore
    "filter_osm_ids,expected_result_length",
    [
        (
            [
                "way/1101364465",
                "way/1031859267",
                "node/10187594406",
                "way/248632173",
                "node/7573557755",
                "way/183199499",
                "way/171570637",
                "way/1113528087",
                "way/1113528092",
                "way/259888097",
            ],
            10,
        ),
        (["way/0", "node/0", "relation/0"], 0),
    ],
)
def test_pbf_reader_features_ids_filtering(filter_osm_ids: list[str], expected_result_length: int):
    """Test proper features ids filtering in `PbfFileReader`."""
    file_name = "d17f922ed15e9609013a6b895e1e7af2d49158f03586f2c675d17b760af3452e.osm.pbf"
    features_gdf = PbfFileReader().convert_pbf_to_geodataframe(
        pbf_path=[Path(__file__).parent.parent / "test_files" / file_name],
        ignore_cache=True,
        filter_osm_ids=filter_osm_ids,
    )
    assert len(features_gdf) == expected_result_length


@pytest.mark.parametrize(
    "expectation,allow_uncovered_geometry",
    [
        (pytest.raises(GeometryNotCoveredError), False),
        (pytest.warns(GeometryNotCoveredWarning), True),
    ],
)  # type: ignore
def test_uncovered_geometry_extract(expectation, allow_uncovered_geometry: bool):
    """Test if raises errors as expected when geometry can't be covered."""
    with expectation:
        geometry = from_wkt(
            "POLYGON ((-43.064 29.673, -43.064 29.644, -43.017 29.644,"
            " -43.017 29.673, -43.064 29.673))"
        )
        features_gdf = PbfFileReader(
            geometry_filter=geometry, allow_uncovered_geometry=allow_uncovered_geometry
        ).convert_geometry_to_geodataframe(ignore_cache=True)
        assert len(features_gdf) == 0


@pytest.mark.parametrize(  # type: ignore
    "geometry",
    [
        box(
            minx=7.416486207767861,
            miny=43.7310867041912,
            maxx=7.421931388477276,
            maxy=43.73370705597216,
        ),
        GeohashGeometryParser().convert("spv2bc"),  # type: ignore
        GeohashGeometryParser().convert("spv2bc,spv2bfr"),  # type: ignore
        H3GeometryParser().convert("8a3969a40ac7fff"),  # type: ignore
        H3GeometryParser().convert("8a3969a40ac7fff,893969a4037ffff"),  # type: ignore
        S2GeometryParser().convert("12cdc28bc"),  # type: ignore
        S2GeometryParser().convert("12cdc28bc,12cdc28f"),  # type: ignore
        GeocodeGeometryParser().convert("Monaco-Ville, Monaco"),  # type: ignore
    ],
)
def test_valid_geometries(geometry: BaseGeometry):
    """Test if geometry filters as loaded properly."""
    PbfFileReader(geometry_filter=geometry)


@pytest.mark.parametrize(  # type: ignore
    "geometry",
    [
        Point(10, 5),
        box(
            minx=7.416486207767861,
            miny=43.7310867041912,
            maxx=7.421931388477276,
            maxy=43.73370705597216,
        ).boundary,
        Point(10, 5).boundary,
        MultiPoint([(1, 2), (3, 4)]),
        LineString([(1, 2), (3, 4)]),
        GeometryCollection(
            [
                box(
                    minx=7.416486207767861,
                    miny=43.7310867041912,
                    maxx=7.421931388477276,
                    maxy=43.73370705597216,
                ),
                Point(10, 5),
            ]
        ),
    ],
)
def test_invalid_geometries(geometry: BaseGeometry):
    """Test if invalid geometry filters raise errors."""
    with pytest.raises(InvalidGeometryFilter):
        PbfFileReader(geometry_filter=geometry)


def test_empty_columns_dropping() -> None:
    """Test if dropping empty columns work."""
    monaco_file_path = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    result = convert_pbf_to_geodataframe(
        monaco_file_path, ignore_cache=True, tags_filter=GEOFABRIK_LAYERS, explode_tags=True
    )
    assert len(result.columns) == 28, result.columns
    assert "unkown_roads" not in result.columns


def test_geoparquet_deprecation_warning() -> None:
    """Test if warning is properly displayed."""
    monaco_file_path = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    result_path = convert_pbf_to_parquet(
        monaco_file_path,
        ignore_cache=True,
    )
    result_path.replace(result_path.with_suffix(".geoparquet"))
    with pytest.warns(DeprecationWarning):
        convert_pbf_to_parquet(monaco_file_path, ignore_cache=False)

    with pytest.warns(DeprecationWarning):
        convert_pbf_to_geodataframe(monaco_file_path, ignore_cache=False)


@pytest.mark.parametrize(  # type: ignore
    "geometry",
    [
        box(
            minx=7.416486207767861,
            miny=43.7310867041912,
            maxx=7.421931388477276,
            maxy=43.73370705597216,
        ),
        GeohashGeometryParser().convert("spv2bc"),  # type: ignore
        GeohashGeometryParser().convert("spv2bc,spv2bfr"),  # type: ignore
        H3GeometryParser().convert("8a3969a40ac7fff"),  # type: ignore
        H3GeometryParser().convert("8a3969a40ac7fff,893969a4037ffff"),  # type: ignore
        S2GeometryParser().convert("12cdc28bc"),  # type: ignore
        S2GeometryParser().convert("12cdc28bc,12cdc28f"),  # type: ignore
        GeocodeGeometryParser().convert("Monaco-Ville, Monaco"),  # type: ignore
    ],
)
def test_geometry_orienting(geometry: BaseGeometry):
    """Test if geometry orienting works properly."""
    oriented_geometry = cast(
        BaseGeometry, PbfFileReader(geometry_filter=geometry)._get_oriented_geometry_filter()
    )
    intersection_area = geometry.intersection(oriented_geometry).area
    iou = intersection_area / (geometry.area + oriented_geometry.area - intersection_area)
    ut.assertAlmostEqual(iou, 1, delta=1e-4)


@pytest.mark.parametrize(  # type: ignore
    "func,new_function_name",
    [
        (
            partial(
                functions.get_features_gdf,
                file_paths=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_geodataframe",
        ),
        (
            partial(
                functions.get_features_gdf,
                pbf_path=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_geodataframe",
        ),
        (
            partial(functions.get_features_gdf_from_geometry, geometry_filter=geometry_box()),
            "convert_geometry_to_geodataframe",
        ),
        (
            partial(
                functions.convert_pbf_to_gpq,
                pbf_path=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_parquet",
        ),
        (
            partial(
                functions.convert_geometry_to_gpq,
                geometry_filter=geometry_box(),
            ),
            "convert_geometry_to_parquet",
        ),
        (
            partial(
                PbfFileReader().get_features_gdf,
                file_paths=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_geodataframe",
        ),
        (
            partial(
                PbfFileReader().get_features_gdf,
                pbf_path=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_geodataframe",
        ),
        (
            PbfFileReader(geometry_filter=geometry_box()).get_features_gdf_from_geometry,
            "convert_geometry_to_geodataframe",
        ),
        (
            partial(
                PbfFileReader().convert_pbf_to_gpq,
                pbf_path=Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf",
            ),
            "convert_pbf_to_parquet",
        ),
        (
            PbfFileReader(geometry_filter=geometry_box()).convert_geometry_filter_to_gpq,
            "convert_geometry_to_parquet",
        ),
    ],
)
def test_deprecation(func: Callable[[], Any], new_function_name: str):
    """Test if deprecation works."""
    with pytest.warns(FutureWarning) as record:
        func()

    assert new_function_name in str(record[0].message)


def check_if_relation_in_osm_is_valid_based_on_tags(pbf_file: str, relation_id: str) -> bool:
    """Check if given relation in OSM is valid."""
    duckdb.load_extension("spatial")
    return cast(
        bool,
        duckdb.sql(
            f"SELECT list_contains(ref_roles, 'outer') FROM ST_READOSM('{pbf_file}') "
            "WHERE kind = 'relation' AND len(refs) > 0 AND list_contains(map_keys(tags), 'type') "
            "AND list_has_any(map_extract(tags, 'type'), ['boundary', 'multipolygon']) "
            f"AND id = {relation_id}"
        ).fetchone()[0],
    )


def check_if_relation_in_osm_is_valid_based_on_geometry(pbf_file: str, relation_id: str) -> bool:
    """
    Check if given relation in OSM is valid.

    Reconstructs full geometry for a single ID and check if there is at least one outer geometry.
    Sometimes
    """
    duckdb.load_extension("spatial")
    return cast(
        bool,
        duckdb.sql(
            f"""
            WITH required_relation AS (
                SELECT
                    r.id
                FROM ST_ReadOsm('{pbf_file}') r
                WHERE r.kind = 'relation'
                    AND len(r.refs) > 0
                    AND list_contains(map_keys(r.tags), 'type')
                    AND list_has_any(
                        map_extract(r.tags, 'type'),
                        ['boundary', 'multipolygon']
                    )
                    AND r.id = {relation_id}
            ),
            unnested_relation_refs AS (
                SELECT
                    r.id,
                    UNNEST(refs) as ref,
                    UNNEST(ref_types) as ref_type,
                    UNNEST(ref_roles) as ref_role,
                    UNNEST(range(length(refs))) as ref_idx
                FROM ST_ReadOsm('{pbf_file}') r
                SEMI JOIN required_relation rr
                ON r.id = rr.id
            ),
            unnested_relation_way_refs AS (
                SELECT id, ref, COALESCE(ref_role, 'outer') as ref_role, ref_idx
                FROM unnested_relation_refs
                WHERE ref_type = 'way'
            ),
            unnested_relations AS (
                SELECT
                    r.id,
                    COALESCE(r.ref_role, 'outer') as ref_role,
                    r.ref,
                FROM unnested_relation_way_refs r
            ),
            unnested_way_refs AS (
                SELECT
                    w.id,
                    UNNEST(refs) as ref,
                    UNNEST(ref_types) as ref_type,
                    UNNEST(range(length(refs))) as ref_idx
                FROM ST_ReadOsm('{pbf_file}') w
                SEMI JOIN unnested_relation_way_refs urwr
                ON urwr.ref = w.id
                WHERE w.kind = 'way'
            ),
            nodes_geometries AS (
                SELECT
                    n.id,
                    ST_POINT(n.lon, n.lat) geom
                FROM ST_ReadOsm('{pbf_file}') n
                SEMI JOIN unnested_way_refs uwr
                ON uwr.ref = n.id
                WHERE n.kind = 'node'
            ),
            way_geometries AS (
                SELECT uwr.id, ST_MakeLine(LIST(n.geom ORDER BY ref_idx ASC)) linestring
                FROM unnested_way_refs uwr
                JOIN nodes_geometries n
                ON uwr.ref = n.id
                GROUP BY uwr.id
            ),
            any_outer_refs AS (
                SELECT id, bool_or(ref_role == 'outer') any_outer_refs
                FROM unnested_relations
                GROUP BY id
            ),
            relations_with_geometries AS (
                SELECT
                    x.id,
                    CASE WHEN aor.any_outer_refs
                        THEN x.ref_role ELSE 'outer'
                    END as ref_role,
                    x.geom geometry,
                    row_number() OVER (PARTITION BY x.id) as geometry_id
                FROM (
                    SELECT
                        unnested_relations.id,
                        unnested_relations.ref_role,
                        UNNEST(
                            ST_Dump(ST_LineMerge(ST_Collect(list(way_geometries.linestring)))),
                            recursive := true
                        ),
                    FROM unnested_relations
                    JOIN way_geometries ON way_geometries.id = unnested_relations.ref
                    GROUP BY unnested_relations.id, unnested_relations.ref_role
                ) x
                JOIN any_outer_refs aor ON aor.id = x.id
                WHERE ST_NPoints(ST_RemoveRepeatedPoints(geom)) >= 4
            ),
            valid_relations AS (
                SELECT id, is_valid
                FROM (
                    SELECT
                        id,
                        bool_and(
                            ST_Equals(ST_StartPoint(geometry), ST_EndPoint(geometry))
                        ) is_valid
                    FROM relations_with_geometries
                    WHERE ref_role = 'outer'
                    GROUP BY id
                )
                WHERE is_valid = true
            )
            SELECT COUNT(*) > 0 AS 'any_valid_outer_geometry'
            FROM valid_relations
            """
        ).fetchone()[0],
    )


def get_tags_from_osm_element(pbf_file: str, feature_id: str) -> dict[str, str]:
    """Check if given relation in OSM is valid."""
    duckdb.load_extension("spatial")
    kind, osm_id = feature_id.split("/", 2)
    raw_tags = duckdb.sql(
        f"SELECT tags FROM ST_READOSM('{pbf_file}') WHERE kind = '{kind}' AND id = {osm_id}"
    ).fetchone()[0]
    return cast(dict[str, str], raw_tags)


def extract_polygons_from_geometry(geometry: BaseGeometry) -> list[Union[Polygon, MultiPolygon]]:
    """Extract only Polygons and MultiPolygons from the geometry."""
    polygon_geometries = []
    if geometry.geom_type in ("Polygon", "MultiPolygon"):
        polygon_geometries.append(geometry)
    elif geometry.geom_type in ("GeometryCollection"):
        polygon_geometries.extend(
            sub_geom
            for sub_geom in geometry.geoms
            if sub_geom.geom_type in ("Polygon", "MultiPolygon")
        )
    return polygon_geometries


@P.parameters("extract_name")  # type: ignore
@P.case("Bahamas", "bahamas")  # type: ignore
@P.case("Cambodia", "cambodia")  # type: ignore
@P.case("Cyprus", "cyprus")  # type: ignore
@P.case("El Salvador", "el-salvador")  # type: ignore
@P.case("Fiji", "fiji")  # type: ignore
@P.case("Greenland", "greenland")  # type: ignore
@P.case("Kiribati", "kiribati")  # type: ignore
@P.case("Maldives", "maldives")  # type: ignore
@P.case("Mauritius", "mauritius")  # type: ignore
@P.case("Monaco", "monaco")  # type: ignore
@P.case("Panama", "panama")  # type: ignore
@P.case("Seychelles", "seychelles")  # type: ignore
def test_gdal_parity(extract_name: str) -> None:
    """
    Test if loaded data is similar to GDAL results.

    Test downloads prepared pbf files and parsed geoparquet using GDAL from kraina-ai/srai-test-
    files repository.
    """
    pbf_file_download_url = LFS_DIRECTORY_URL + f"{extract_name}-latest.osm.pbf"
    files_directory = Path(__file__).parent.parent / "files"
    files_directory.mkdir(exist_ok=True, parents=True)
    pbf_file_path = files_directory / f"{extract_name}.osm.pbf"
    urllib.request.urlretrieve(pbf_file_download_url, pbf_file_path)
    gpq_file_download_url = LFS_DIRECTORY_URL + f"{extract_name}-latest.geoparquet"
    gpq_file_path = files_directory / f"{extract_name}.parquet"
    urllib.request.urlretrieve(gpq_file_download_url, gpq_file_path)

    reader = PbfFileReader()
    duckdb_gdf = reader.convert_pbf_to_geodataframe(
        [pbf_file_path], explode_tags=False, ignore_cache=True
    )
    gdal_gdf = gpd.read_parquet(gpq_file_path)
    gdal_gdf["tags"] = gdal_gdf["tags"].apply(json.loads)

    gdal_index = gdal_gdf.index
    duckdb_index = duckdb_gdf.index

    missing_in_duckdb = gdal_index.difference(duckdb_index)
    # Get missing non relation features with at least one non-area tag value
    non_relations_missing_in_duckdb = [
        feature_id
        for feature_id in missing_in_duckdb
        if not feature_id.startswith("relation/")
        and any(True for k in gdal_gdf.loc[feature_id].tags.keys() if k != "area")
    ]
    valid_relations_missing_in_duckdb = [
        feature_id
        for feature_id in missing_in_duckdb
        if feature_id.startswith("relation/")
        and check_if_relation_in_osm_is_valid_based_on_tags(
            str(pbf_file_path), feature_id.replace("relation/", "")
        )
        and check_if_relation_in_osm_is_valid_based_on_geometry(
            str(pbf_file_path), feature_id.replace("relation/", "")
        )
    ]

    invalid_relations_missing_in_duckdb = missing_in_duckdb.difference(
        non_relations_missing_in_duckdb
    ).difference(valid_relations_missing_in_duckdb)

    assert (
        not non_relations_missing_in_duckdb
    ), f"Missing non relation features in PbfFileReader ({non_relations_missing_in_duckdb})"

    assert (
        not valid_relations_missing_in_duckdb
    ), f"Missing valid relation features in PbfFileReader ({valid_relations_missing_in_duckdb})"

    if len(invalid_relations_missing_in_duckdb) > 0:
        warnings.warn(
            "Invalid relations exists in OSM GDAL data extract"
            f" ({invalid_relations_missing_in_duckdb})",
            stacklevel=1,
        )

    invalid_features = []

    common_index = gdal_index.difference(invalid_relations_missing_in_duckdb)
    joined_df = pd.DataFrame(
        dict(
            duckdb_tags=duckdb_gdf.loc[common_index].tags,
            source_tags=gdal_gdf.loc[common_index].tags,
            duckdb_geometry=duckdb_gdf.loc[common_index].geometry,
            gdal_geometry=gdal_gdf.loc[common_index].geometry,
        ),
        index=common_index,
    )

    # Check tags
    joined_df["tags_keys_difference"] = joined_df.apply(
        lambda x: set(x.duckdb_tags.keys())
        .symmetric_difference(x.source_tags.keys())
        .difference(["area"]),
        axis=1,
    )

    # If difference - compare tags with source data.
    # Sometimes GDAL copies tags from members to a parent.
    mismatched_rows = joined_df["tags_keys_difference"].str.len() != 0
    if mismatched_rows.any():
        joined_df.loc[mismatched_rows, "source_tags"] = [
            get_tags_from_osm_element(str(pbf_file_path), row_index)
            for row_index in joined_df.loc[mismatched_rows].index
        ]

        joined_df.loc[mismatched_rows, "tags_keys_difference"] = joined_df.loc[
            mismatched_rows
        ].apply(
            lambda x: set(x.duckdb_tags.keys())
            .symmetric_difference(x.source_tags.keys())
            .difference(["area"]),
            axis=1,
        )

    for row_index in common_index:
        tags_keys_difference = joined_df.loc[row_index, "tags_keys_difference"]
        duckdb_tags = joined_df.loc[row_index, "duckdb_tags"]
        source_tags = joined_df.loc[row_index, "source_tags"]
        assert not tags_keys_difference, (
            f"Tags keys aren't equal. ({row_index}, {tags_keys_difference},"
            f" {duckdb_tags.keys()}, {source_tags.keys()})"
        )
        ut.assertDictEqual(
            {k: v for k, v in duckdb_tags.items() if k != "area"},
            {k: v for k, v in source_tags.items() if k != "area"},
            f"Tags aren't equal. ({row_index})",
        )

    invalid_geometries_df = joined_df

    invalid_geometries_df["duckdb_geometry_type"] = invalid_geometries_df.apply(
        lambda x: x.duckdb_geometry.geom_type,
        axis=1,
    )
    invalid_geometries_df["gdal_geometry_type"] = invalid_geometries_df.apply(
        lambda x: x.gdal_geometry.geom_type,
        axis=1,
    )
    invalid_geometries_df["duckdb_geometry_num_points"] = invalid_geometries_df[
        "duckdb_geometry"
    ].apply(lambda x: len(get_coordinates(x)))
    invalid_geometries_df["gdal_geometry_num_points"] = invalid_geometries_df[
        "gdal_geometry"
    ].apply(lambda x: len(get_coordinates(x)))

    # Check if both geometries are closed or open
    invalid_geometries_df["duckdb_is_closed"] = invalid_geometries_df["duckdb_geometry"].apply(
        lambda x: x.is_closed
    )
    invalid_geometries_df["gdal_is_closed"] = invalid_geometries_df["gdal_geometry"].apply(
        lambda x: x.is_closed
    )
    invalid_geometries_df["geometry_both_closed_or_not"] = (
        invalid_geometries_df["duckdb_is_closed"] == invalid_geometries_df["gdal_is_closed"]
    )

    tolerance = 0.5 * 10 ** (-6)
    # Check if geometries are almost equal - same geom type, same points
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_both_closed_or_not"], "geometry_almost_equals"
    ] = (
        gpd.GeoSeries(
            invalid_geometries_df.loc[
                invalid_geometries_df["geometry_both_closed_or_not"], "duckdb_geometry"
            ],
        )
        .set_crs(WGS84_CRS)
        .geom_equals_exact(
            gpd.GeoSeries(
                invalid_geometries_df.loc[
                    invalid_geometries_df["geometry_both_closed_or_not"], "gdal_geometry"
                ],
            ).set_crs(WGS84_CRS),
            tolerance=tolerance,
        )
    )
    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["geometry_both_closed_or_not"]
            & invalid_geometries_df["geometry_almost_equals"]
        )
    ]
    if invalid_geometries_df.empty:
        return

    # Check geometries equality - same geom type, same points
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_both_closed_or_not"], "geometry_equals"
    ] = (
        gpd.GeoSeries(
            invalid_geometries_df.loc[
                invalid_geometries_df["geometry_both_closed_or_not"], "duckdb_geometry"
            ],
        )
        .set_crs(WGS84_CRS)
        .geom_equals(
            gpd.GeoSeries(
                invalid_geometries_df.loc[
                    invalid_geometries_df["geometry_both_closed_or_not"], "gdal_geometry"
                ],
            ).set_crs(WGS84_CRS)
        )
    )
    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["geometry_both_closed_or_not"]
            & invalid_geometries_df["geometry_equals"]
        )
    ]
    if invalid_geometries_df.empty:
        return

    # Check geometries overlap if polygons - slight misalingment between points,
    # but marginal
    matching_polygon_geometries_mask = (
        invalid_geometries_df["geometry_both_closed_or_not"]
        & gpd.GeoSeries(invalid_geometries_df["duckdb_geometry"]).geom_type.isin(
            ("Polygon", "MultiPolygon", "GeometryCollection")
        )
        & gpd.GeoSeries(invalid_geometries_df["gdal_geometry"]).geom_type.isin(
            ("Polygon", "MultiPolygon", "GeometryCollection")
        )
    )
    invalid_geometries_df.loc[matching_polygon_geometries_mask, "geometry_intersection_area"] = (
        gpd.GeoSeries(
            invalid_geometries_df.loc[matching_polygon_geometries_mask, "duckdb_geometry"]
        )
        .set_crs(WGS84_CRS)
        .intersection(
            gpd.GeoSeries(
                invalid_geometries_df.loc[matching_polygon_geometries_mask, "gdal_geometry"]
            ).set_crs(WGS84_CRS),
        )
        .area
    )

    invalid_geometries_df.loc[
        matching_polygon_geometries_mask, "iou_metric"
    ] = invalid_geometries_df.loc[
        matching_polygon_geometries_mask, "geometry_intersection_area"
    ] / (
        gpd.GeoSeries(
            invalid_geometries_df.loc[matching_polygon_geometries_mask, "duckdb_geometry"]
        )
        .set_crs(WGS84_CRS)
        .area
        + gpd.GeoSeries(
            invalid_geometries_df.loc[matching_polygon_geometries_mask, "gdal_geometry"]
        )
        .set_crs(WGS84_CRS)
        .area
        - invalid_geometries_df.loc[matching_polygon_geometries_mask, "geometry_intersection_area"]
    )

    invalid_geometries_df.loc[matching_polygon_geometries_mask, "geometry_iou_near_one"] = (
        invalid_geometries_df.loc[matching_polygon_geometries_mask, "iou_metric"] >= (1 - tolerance)
    )
    invalid_geometries_df = invalid_geometries_df.loc[
        ~(matching_polygon_geometries_mask & invalid_geometries_df["geometry_iou_near_one"])
    ]
    if invalid_geometries_df.empty:
        return

    # Check if points lay near each other - regardless of geometry type
    # (Polygon vs LineString)
    invalid_geometries_df["hausdorff_distance_value"] = invalid_geometries_df.apply(
        lambda x: hausdorff_distance(x.duckdb_geometry, x.gdal_geometry, densify=0.5), axis=1
    )
    invalid_geometries_df["geometry_close_hausdorff_distance"] = (
        invalid_geometries_df["hausdorff_distance_value"] < 1e-10
    )

    # Check if geometries are the same type and close, but different number of points
    # where duckdb version is simplified
    # duckdb_geometry_num_points
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"],
        "is_duckdb_geometry_the_same_type_but_different_number_of_points",
    ] = invalid_geometries_df.loc[invalid_geometries_df["geometry_close_hausdorff_distance"]].apply(
        lambda x: x.duckdb_geometry_type == x.gdal_geometry_type
        and x.duckdb_geometry_num_points != x.gdal_geometry_num_points,
        axis=1,
    )
    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["geometry_close_hausdorff_distance"]
            & invalid_geometries_df[
                "is_duckdb_geometry_the_same_type_but_different_number_of_points"
            ]
        )
    ]

    # Check if GDAL geometry is a linestring while DuckDB geometry is a polygon
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"],
        "is_duckdb_polygon_and_gdal_linestring",
    ] = invalid_geometries_df.loc[invalid_geometries_df["geometry_close_hausdorff_distance"]].apply(
        lambda x: x.duckdb_geometry_type
        in (
            "Polygon",
            "MultiPolygon",
        )
        and x.gdal_geometry_type in ("LineString", "MultiLineString"),
        axis=1,
    )

    # Check if DuckDB geometry can be a polygon and not a linestring
    # based on features config
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_polygon_and_gdal_linestring"],
        "is_proper_filter_tag_value",
    ] = invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_polygon_and_gdal_linestring"],
        "duckdb_tags",
    ].apply(
        lambda x: any(
            (tag in reader.osm_way_polygon_features_config.all)
            or (
                tag in reader.osm_way_polygon_features_config.allowlist
                and value in reader.osm_way_polygon_features_config.allowlist[tag]
            )
            or (
                tag in reader.osm_way_polygon_features_config.denylist
                and value not in reader.osm_way_polygon_features_config.denylist[tag]
            )
            for tag, value in x.items()
        )
    )

    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["geometry_close_hausdorff_distance"]
            & invalid_geometries_df["is_duckdb_polygon_and_gdal_linestring"]
            & invalid_geometries_df["is_proper_filter_tag_value"]
        )
    ]
    if invalid_geometries_df.empty:
        return

    # Check if GDAL geometry is a polygon while DuckDB geometry is a linestring
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"],
        "is_duckdb_linestring_and_gdal_polygon",
    ] = invalid_geometries_df.loc[invalid_geometries_df["geometry_close_hausdorff_distance"]].apply(
        lambda x: x.duckdb_geometry_type
        in (
            "LineString",
            "MultiLineString",
        )
        and x.gdal_geometry_type in ("Polygon", "MultiPolygon"),
        axis=1,
    )

    # Check if DuckDB geometry should be a linestring and not a polygon
    # based on features config
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_linestring_and_gdal_polygon"],
        "is_not_in_filter_tag_value",
    ] = invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_linestring_and_gdal_polygon"],
        "duckdb_tags",
    ].apply(
        lambda x: any(
            (tag not in reader.osm_way_polygon_features_config.all)
            and (
                tag not in reader.osm_way_polygon_features_config.allowlist
                or (
                    tag in reader.osm_way_polygon_features_config.allowlist
                    and value not in reader.osm_way_polygon_features_config.allowlist[tag]
                )
            )
            and (
                tag not in reader.osm_way_polygon_features_config.denylist
                or (
                    tag in reader.osm_way_polygon_features_config.denylist
                    and value in reader.osm_way_polygon_features_config.denylist[tag]
                )
            )
            for tag, value in x.items()
        )
    )

    # Check if DuckDB geometry should be a linestring and not a polygon
    # based on minimal number of points
    invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_linestring_and_gdal_polygon"],
        "has_less_than_4_points",
    ] = invalid_geometries_df.loc[
        invalid_geometries_df["geometry_close_hausdorff_distance"]
        & invalid_geometries_df["is_duckdb_linestring_and_gdal_polygon"]
    ].apply(
        lambda x: x.duckdb_geometry_num_points < 4, axis=1
    )

    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["geometry_close_hausdorff_distance"]
            & invalid_geometries_df["is_duckdb_linestring_and_gdal_polygon"]
            & (
                invalid_geometries_df["is_not_in_filter_tag_value"]
                | invalid_geometries_df["has_less_than_4_points"]
            )
        )
    ]
    if invalid_geometries_df.empty:
        return

    # Sometimes GDAL parses geometries incorrectly because of errors in OSM data
    # Examples of errors:
    # - overlapping inner ring with outer ring
    # - intersecting outer rings
    # - intersecting inner rings
    # - inner ring outside outer geometry
    # If we detect thattaht the difference between those geometries
    # lie inside the exterior of the geometry, we can assume that the OSM geometry
    # is improperly defined.
    invalid_geometries_df["duckdb_unioned_geometry_without_holes"] = invalid_geometries_df[
        "duckdb_geometry"
    ].apply(
        lambda x: (
            _remove_interiors(unary_union(polygons))
            if (polygons := extract_polygons_from_geometry(x))
            else None
        )
    )
    invalid_geometries_df["gdal_unioned_geometry_without_holes"] = invalid_geometries_df[
        "gdal_geometry"
    ].apply(
        lambda x: (
            _remove_interiors(unary_union(polygons))
            if (polygons := extract_polygons_from_geometry(x))
            else None
        )
    )
    invalid_geometries_df["both_polygon_geometries"] = (
        ~pd.isna(invalid_geometries_df["duckdb_unioned_geometry_without_holes"])
    ) & (~pd.isna(invalid_geometries_df["gdal_unioned_geometry_without_holes"]))

    # Check if the differences doesn't extend both geometries,
    # only one sided difference can be accepted
    invalid_geometries_df.loc[
        invalid_geometries_df["both_polygon_geometries"], "duckdb_geometry_fully_covered_by_gdal"
    ] = gpd.GeoSeries(
        invalid_geometries_df.loc[
            invalid_geometries_df["both_polygon_geometries"],
            "duckdb_unioned_geometry_without_holes",
        ]
    ).covered_by(
        gpd.GeoSeries(
            invalid_geometries_df.loc[
                invalid_geometries_df["both_polygon_geometries"],
                "gdal_unioned_geometry_without_holes",
            ]
        )
    )

    invalid_geometries_df.loc[
        invalid_geometries_df["both_polygon_geometries"], "gdal_geometry_fully_covered_by_duckdb"
    ] = gpd.GeoSeries(
        invalid_geometries_df.loc[
            invalid_geometries_df["both_polygon_geometries"], "gdal_unioned_geometry_without_holes"
        ]
    ).covered_by(
        gpd.GeoSeries(
            invalid_geometries_df.loc[
                invalid_geometries_df["both_polygon_geometries"],
                "duckdb_unioned_geometry_without_holes",
            ]
        )
    )

    invalid_geometries_df = invalid_geometries_df.loc[
        ~(
            invalid_geometries_df["duckdb_geometry_fully_covered_by_gdal"]
            | invalid_geometries_df["gdal_geometry_fully_covered_by_duckdb"]
        )
    ]
    if invalid_geometries_df.empty:
        return

    invalid_features = (
        invalid_geometries_df.drop(
            columns=["duckdb_tags", "source_tags", "duckdb_geometry", "gdal_geometry"]
        )
        .reset_index()
        .to_dict(orient="records")
    )

    assert not invalid_features, (
        f"Geometries aren't equal - ({[t[FEATURES_INDEX] for t in invalid_features]}). Full debug"
        f" output: ({invalid_features})"
    )


# https://stackoverflow.com/a/70387141/7766101
def _remove_interiors(geometry: Union[Polygon, MultiPolygon]) -> Polygon:
    if isinstance(geometry, MultiPolygon):
        return unary_union([_remove_interiors(sub_polygon) for sub_polygon in geometry.geoms])
    if geometry.interiors:
        return Polygon(list(geometry.exterior.coords))
    return geometry
