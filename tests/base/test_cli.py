"""Tests for CLI."""

import uuid
from pathlib import Path
from typing import Optional

import pytest
from parametrization import Parametrization as P
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from quackosm import __app_name__, __version__, cli
from quackosm.osm_extracts.extract import OsmExtractSource
from tests.base.conftest import geometry_boundary_file_path, geometry_geojson, geometry_wkt

runner = CliRunner()


def monaco_pbf_file_path() -> str:
    """Monaco PBF file path."""
    return str(Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf")


@pytest.fixture()  # type: ignore
def monaco_pbf_file_path_fixture() -> str:
    """Monaco PBF file path fixture."""
    return monaco_pbf_file_path()


def osm_tags_filter_file_path() -> str:
    """OSM tags filter file path."""
    return str(Path(__file__).parent.parent / "test_files" / "osm_tags_filter.json")


def osm_way_config_file_path() -> str:
    """OSM way features config file path."""
    return str(Path(__file__).parent.parent.parent / "quackosm" / "osm_way_polygon_features.json")


def random_str() -> str:
    """Return random string."""
    return str(uuid.uuid4())


def test_version() -> None:
    """Test if version is properly returned."""
    result = runner.invoke(cli.app, ["--version"])

    assert result.exit_code == 0
    assert f"{__app_name__} {__version__}\n" in result.stdout


def test_pbf_file_or_geometry_filter_is_required() -> None:
    """Test if cannot run without pbf file and without geometry filter."""
    result = runner.invoke(
        cli.app,
    )

    assert result.exit_code == 2
    assert "Missing argument 'PBF file path'." in (result.stdout or result.stderr)


def test_basic_run(monaco_pbf_file_path_fixture: str) -> None:
    """Test if runs properly without options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path_fixture])

    assert result.exit_code == 0
    assert str(Path("files/monaco_nofilter_noclip_compact_sorted.parquet")) in result.stdout


def test_silent_mode(monaco_pbf_file_path_fixture: str) -> None:
    """Test if runs properly without reporting status."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path_fixture, "--ignore-cache", "--silent"])

    assert result.exit_code == 0
    assert str(Path("files/monaco_nofilter_noclip_compact_sorted.parquet")) == result.stdout.strip()


def test_transient_mode(monaco_pbf_file_path_fixture: str) -> None:
    """Test if runs properly without reporting status."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path_fixture, "--ignore-cache", "--transient"])
    output_lines = result.stdout.strip().split("\n")
    assert result.exit_code == 0
    assert len(result.stdout.strip().split("\n")) == 2
    assert "Finished operation in" in output_lines[0]
    assert str(Path("files/monaco_nofilter_noclip_compact_sorted.parquet")) == output_lines[1]


@P.parameters("args", "expected_result")  # type: ignore
@P.case(
    "Explode",
    ["--explode-tags"],
    "files/monaco_nofilter_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case("Explode short", ["--explode"], "files/monaco_nofilter_noclip_exploded_sorted.parquet")  # type: ignore
@P.case("Compact", ["--compact-tags"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("Compact short", ["--compact"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case(
    "Working directory",
    ["--working-directory", "files/workdir"],
    "files/workdir/monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case("Ignore cache", ["--ignore-cache"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("Ignore cache short", ["--no-cache"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("Output", ["--output", "files/monaco_output.parquet"], "files/monaco_output.parquet")  # type: ignore
@P.case("Output short", ["-o", "files/monaco_output.parquet"], "files/monaco_output.parquet")  # type: ignore
@P.case(
    "DuckDB explicit export",
    ["--duckdb"],
    "files/monaco_nofilter_noclip_compact_sorted.duckdb",
)  # type: ignore
@P.case(
    "DuckDB explicit export with table name",
    ["--duckdb", "--duckdb-table-name", "test"],
    "files/monaco_nofilter_noclip_compact_sorted.duckdb",
)  # type: ignore
@P.case("Silent", ["--silent"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("Transient", ["--transient"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("Explicit sort", ["--sort"], "files/monaco_nofilter_noclip_compact_sorted.parquet")  # type: ignore
@P.case("No sort", ["--no-sort"], "files/monaco_nofilter_noclip_compact.parquet")  # type: ignore
@P.case(
    "Keep metadata tags",
    ["--keep-metadata-tags"],
    "files/monaco_d38c1671_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Skip metadata tags",
    ["--ignore-metadata-tags"],
    "files/monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Output with working directory",
    ["--working-directory", "files/workdir", "-o", "files/monaco_output.parquet"],
    "files/monaco_output.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/monaco_a9dd1c3c_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter compact",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/monaco_a9dd1c3c_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter file",
    [
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
    "files/monaco_a9dd1c3c_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter file compact",
    [
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
        "--compact",
    ],
    "files/monaco_a9dd1c3c_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped",
    [
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
    ],
    "files/monaco_654daac5_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped compact",
    [
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
        "--compact",
    ],
    "files/monaco_654daac5_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry WKT filter",
    ["--geom-filter-wkt", geometry_wkt()],
    "files/monaco_nofilter_09c3fc04_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/monaco_nofilter_82c0fdfa_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry file filter",
    ["--geom-filter-file", geometry_boundary_file_path()],
    "files/monaco_nofilter_6a869bcf_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/monaco_nofilter_e7f0b78a_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter",
    ["--geom-filter-index-geohash", "spv2bc"],
    "files/monaco_nofilter_c08889e8_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter multiple",
    ["--geom-filter-index-geohash", "spv2bc,spv2bfr"],
    "files/monaco_nofilter_1bd33e0a_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter",
    ["--geom-filter-index-h3", "8a3969a40ac7fff"],
    "files/monaco_nofilter_01d82ad1_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter multiple",
    ["--geom-filter-index-h3", "8a3969a40ac7fff,893969a4037ffff"],
    "files/monaco_nofilter_c3252c58_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter",
    ["--geom-filter-index-s2", "12cdc28bc"],
    "files/monaco_nofilter_5c3d61eb_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter multiple",
    ["--geom-filter-index-s2", "12cdc28bc,12cdc28f"],
    "files/monaco_nofilter_cda5d65e_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    ["--filter-osm-ids", "way/94399646,node/3617982224,relation/36990"],
    "files/monaco_nofilter_noclip_compact_c740a159_sorted.parquet",
)  # type: ignore
@P.case(
    "Custom SQL filter",
    ["--custom-sql-filter", "cardinality(tags) = 5"],
    "files/monaco_138f715c_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Custom SQL filter",
    ["--custom-sql-filter", "tags['highway'][1] = 'primary'"],
    "files/monaco_e1a5cc12_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Keep all tags",
    [
        "--keep-all-tags",
    ],
    "files/monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/monaco_a9dd1c3c_alltags_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags compact",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/monaco_a9dd1c3c_alltags_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags exploded",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--explode",
    ],
    "files/monaco_a9dd1c3c_alltags_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    ["--osm-way-polygon-config", osm_way_config_file_path()],
    "files/monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case("WKT", ["--wkt-result"], "files/monaco_nofilter_noclip_compact_wkt.parquet")  # type: ignore
@P.case("WKT short", ["--wkt"], "files/monaco_nofilter_noclip_compact_wkt.parquet")  # type: ignore
def test_proper_args_with_pbf(
    monaco_pbf_file_path_fixture: str, args: list[str], expected_result: str
) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path_fixture, *args])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


@P.parameters("args", "expected_result")  # type: ignore
@P.case(
    "Geometry BBOX filter",
    ["--geom-filter-bbox", "7.416486,43.731086,7.421931,43.733707"],
    "files/b9115f99_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry WKT filter",
    ["--geom-filter-wkt", geometry_wkt()],
    "files/09c3fc04_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/82c0fdfa_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/e7f0b78a_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter",
    ["--geom-filter-index-geohash", "spv2bc"],
    "files/c08889e8_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter multiple",
    ["--geom-filter-index-geohash", "spv2bc,spv2bfr"],
    "files/1bd33e0a_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter",
    ["--geom-filter-index-h3", "8a3969a40ac7fff"],
    "files/01d82ad1_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter multiple",
    ["--geom-filter-index-h3", "8a3969a40ac7fff,893969a4037ffff"],
    "files/c3252c58_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter",
    ["--geom-filter-index-s2", "12cdc28bc"],
    "files/5c3d61eb_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter multiple",
    ["--geom-filter-index-s2", "12cdc28bc,12cdc28f"],
    "files/cda5d65e_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Geometry file filter with different OSM source",
    ["--geom-filter-file", geometry_boundary_file_path(), "--osm-extract-source", "OSMfr"],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Explode",
    ["--geom-filter-file", geometry_boundary_file_path(), "--explode-tags"],
    "files/6a869bcf_nofilter_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "Compact",
    ["--geom-filter-file", geometry_boundary_file_path(), "--compact-tags"],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Ignore cache",
    ["--geom-filter-file", geometry_boundary_file_path(), "--ignore-cache"],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Explicit sort",
    ["--geom-filter-file", geometry_boundary_file_path(), "--sort"],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "No sort",
    ["--geom-filter-file", geometry_boundary_file_path(), "--no-sort"],
    "files/6a869bcf_nofilter_compact.parquet",
)  # type: ignore
@P.case(
    "Keep metadata tags",
    ["--geom-filter-file", geometry_boundary_file_path(), "--keep-metadata-tags"],
    "files/6a869bcf_d38c1671_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Skip metadata tags",
    ["--geom-filter-file", geometry_boundary_file_path(), "--ignore-metadata-tags"],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Working directory",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--working-directory",
        "files/workdir",
        "--ignore-cache",
    ],
    "files/workdir/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Output",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--output",
        "files/monaco_output.parquet",
    ],
    "files/monaco_output.parquet",
)  # type: ignore
@P.case(
    "Output with working directory",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--working-directory",
        "files/workdir",
        "-o",
        "files/monaco_output.parquet",
    ],
    "files/monaco_output.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/6a869bcf_a9dd1c3c_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter compact",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/6a869bcf_a9dd1c3c_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
    ],
    "files/6a869bcf_654daac5_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped compact",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
        "--compact",
    ],
    "files/6a869bcf_654daac5_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--filter-osm-ids",
        "way/94399646,node/3617982224,relation/36990",
    ],
    "files/6a869bcf_nofilter_compact_c740a159_sorted.parquet",
)  # type: ignore
@P.case(
    "Keep all tags",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
    ],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/6a869bcf_a9dd1c3c_alltags_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags compact",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/6a869bcf_a9dd1c3c_alltags_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags exploded",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--explode",
    ],
    "files/6a869bcf_a9dd1c3c_alltags_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-way-polygon-config",
        osm_way_config_file_path(),
    ],
    "files/6a869bcf_nofilter_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Allow not covered geometry",
    [
        "--geom-filter-wkt",
        (
            "POLYGON ((-43.064 29.673, -43.064 29.644, -43.017 29.644,"
            " -43.017 29.673, -43.064 29.673))"
        ),
        "--allow-uncovered-geometry",
        "--ignore-cache",
    ],
    "files/fa44926c_nofilter_compact_sorted.parquet",
)  # type: ignore
def test_proper_args_with_geometry_filter(args: list[str], expected_result: str) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [*args, "--osm-extract-source", "any"])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


@P.parameters("args", "expected_result")  # type: ignore
@P.case(
    "Full name",
    [
        "--osm-extract-query",
        "geofabrik_europe_monaco",
    ],
    "files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Find in geofabrik",
    [
        "--osm-extract-query",
        "Monaco",
        "--osm-extract-source",
        "Geofabrik",
    ],
    "files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Explode",
    ["--osm-extract-query", "geofabrik_europe_monaco", "--explode-tags"],
    "files/geofabrik_europe_monaco_nofilter_noclip_exploded_sorted.parquet",
)  # type: ignore
@P.case(
    "Compact",
    [
        "--osm-extract-query",
        "geofabrik_europe_monaco",
        "--compact-tags",
    ],
    "files/geofabrik_europe_monaco_nofilter_noclip_compact_sorted.parquet",
)  # type: ignore
@P.case(
    "Output",
    [
        "--osm-extract-query",
        "geofabrik_europe_monaco",
        "--ignore-cache",
        "--output",
        "files/gfbrk_monaco_output.parquet",
    ],
    "files/gfbrk_monaco_output.parquet",
)  # type: ignore
@P.case(
    "OSM tags filter and geometry filter",
    [
        "--osm-extract-query",
        "geofabrik_europe_monaco",
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/geofabrik_europe_monaco_a9dd1c3c_6a869bcf_exploded_sorted.parquet",
)  # type: ignore
def test_proper_args_with_osm_extract(args: list[str], expected_result: str) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [*args])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


def test_proper_args_with_pbf_url() -> None:
    """Test if runs properly with an url path."""
    result = runner.invoke(cli.app, ["https://download.geofabrik.de/europe/monaco-latest.osm.pbf"])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path("files/monaco-latest_nofilter_noclip_compact_sorted.parquet")) in result.stdout


@P.parameters("args")  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        '{"building": True, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        '{"building": true, highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter wrong type",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        (
            '{"super_group": {"group": {"building": true, "highway": ["primary", "secondary"],'
            ' "amenity": "bench"} } }'
        ),
    ],
)  # type: ignore
@P.case(
    "OSM tags filter wrong type",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        '{"group": [{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}] }',
    ],
)  # type: ignore
@P.case(
    "OSM tags two filters",
    [
        monaco_pbf_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
)  # type: ignore
@P.case(
    "OSM tags nonexistent file filter",
    [monaco_pbf_file_path(), "--osm-tags-filter-file", "nonexistent_json_file.json"],
)  # type: ignore
@P.case("Geometry WKT filter with GeoJSON", ["--geom-filter-wkt", geometry_geojson()])  # type: ignore
@P.case("Geometry GeoJSON filter with WKT", ["--geom-filter-geojson", geometry_wkt()])  # type: ignore
@P.case("Geometry Geohash filter with random string", ["--geom-filter-index-geohash", random_str()])  # type: ignore
@P.case("Geometry H3 filter with Geohash", ["--geom-filter-index-h3", "spv2bc"])  # type: ignore
@P.case("Geometry H3 filter with S2", ["--geom-filter-index-h3", "12cdc28bc"])  # type: ignore
@P.case("Geometry H3 filter with random string", ["--geom-filter-index-h3", random_str()])  # type: ignore
@P.case("Geometry S2 filter with random string", ["--geom-filter-index-s2", random_str()])  # type: ignore
@P.case("Geometry BBOX filter with wrong values", ["--geom-filter-bbox", random_str()])  # type: ignore
@P.case("Geometry BBOX filter with spaces", ["--geom-filter-bbox", "10,", "-5,", "6,", "17"])  # type: ignore
@P.case(
    "Geometry two filters",
    ["--geom-filter-geojson", geometry_geojson(), "--geom-filter-wkt", geometry_wkt()],
)  # type: ignore
@P.case(
    "Geometry nonexistent file filter",
    ["--geom-filter-file", "nonexistent_geojson_file.geojson"],
)  # type: ignore
@P.case(
    "Geometry wrong file filter",
    ["--geom-filter-file", osm_tags_filter_file_path()],
)  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "w/124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "w124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "way124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "n/124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "n124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "node124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "r/124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "r124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "relation124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "node/124;way/124;relation/124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "node/124|way/124|relation/124"])  # type: ignore
@P.case("Filter OSM", [monaco_pbf_file_path(), "--filter-osm-ids", "node/124 way/124 relation/124"])  # type: ignore
@P.case("Custom SQL filter", [monaco_pbf_file_path(), "--custom-sql-filter", random_str()])  # type: ignore
@P.case(
    "Custom SQL filter", [monaco_pbf_file_path(), "--custom-sql-filter", "cardinality(map) = 5"]
)  # type: ignore
@P.case(
    "Custom SQL filter",
    [monaco_pbf_file_path(), "--custom-sql-filter", "map['highway'][1] = 'primary'"],
)  # type: ignore
@P.case(
    "OSM way polygon config",
    [monaco_pbf_file_path(), "--osm-way-polygon-config", "nonexistent_json_file.json"],
)  # type: ignore
@P.case("Silent and transient", [monaco_pbf_file_path(), "--silent", "--transient"])  # type: ignore
@P.case("OSM extracts with multiple matches", ["--osm-extract-query", "Monaco"])  # type: ignore
@P.case("OSM extracts with zero matches - some close matches", ["--osm-extract-query", "Prlnd"])  # type: ignore
@P.case(
    "OSM extracts with zero matches - without close matches",
    [
        "--osm-extract-query",
        "nonexistent_extract",
    ],
)  # type: ignore
@P.case(
    "OSM extracts with zero matches and duckdb export",
    [
        "--duckdb",
        "--osm-extract-query",
        "quack_extract",
    ],
)  # type: ignore
@P.case(
    "Wrong IoU threshold value",
    [
        "--iou-threshold",
        "1.2",
    ],
)  # type: ignore
@P.case(
    "Wrong IoU threshold value",
    [
        "--iou-threshold",
        "-0.2",
    ],
)  # type: ignore
@P.case(
    "Wrong parquet version",
    [
        "--parquet-version",
        "v0",
    ],
)  # type: ignore
def test_wrong_args(args: list[str], capsys: pytest.CaptureFixture) -> None:
    """Test if doesn't run properly with options."""
    # Fix for the I/O error from the Click repository
    # https://github.com/pallets/click/issues/824#issuecomment-1583293065
    with capsys.disabled():
        result = runner.invoke(cli.app, args)
        assert result.exit_code != 0


@pytest.mark.parametrize(
    "osm_source",
    [None, *list(OsmExtractSource)],
)  # type: ignore
@pytest.mark.parametrize(
    "command",
    ["show-extracts", "show-osm-extracts"],
)  # type: ignore
def test_displaying_osm_extracts(
    command: str,
    osm_source: Optional[OsmExtractSource],
    mocker: MockerFixture,
    capsys: pytest.CaptureFixture,
) -> None:
    """Test if displaying OSM extracts works."""
    with capsys.disabled():
        osm_source_command = ["--osm-extract-source", osm_source.value] if osm_source else []
        result = runner.invoke(cli.app, [f"--{command}", *osm_source_command])
        output = result.stdout

        assert result.exit_code == 0
        assert len(output) > 0

        osm_sources_without_any = [src for src in OsmExtractSource if src != OsmExtractSource.any]

        if osm_source == OsmExtractSource.any or not osm_source:
            assert output.startswith("All extracts")
            assert all(src.value in output for src in osm_sources_without_any)
        else:
            assert output.startswith(osm_source.value)

        lines = output.lower().split("\n")

        assert all(
            any(src.value.lower() in line for src in osm_sources_without_any)
            for line in lines
            if len(line.strip()) > 0 and line != "all extracts"
        )
