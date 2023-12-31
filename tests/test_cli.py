"""Tests for CLI."""

from pathlib import Path

import pytest
from parametrization import Parametrization as P
from shapely import to_geojson, to_wkt
from shapely.geometry import Polygon, box
from typer.testing import CliRunner

from quackosm import __app_name__, __version__, cli

runner = CliRunner()


@pytest.fixture()  # type: ignore
def monaco_pbf_file_path() -> str:
    """Monaco PBF file path fixture."""
    return str(Path(__file__).parent / "test_files" / "monaco.osm.pbf")


def geometry_box() -> Polygon:
    """Geometry box."""
    return box(
        minx=7.416486207767861,
        miny=43.7310867041912,
        maxx=7.421931388477276,
        maxy=43.73370705597216,
    )


def geometry_wkt() -> str:
    """Geometry box in WKT form."""
    return str(to_wkt(geometry_box()))


def geometry_geojson() -> str:
    """Geometry box in GeoJSON form."""
    return str(to_geojson(geometry_box()))


def geometry_boundary_file_path() -> str:
    """Geometry Monaco boundary file path."""
    return str(Path(__file__).parent / "test_files" / "monaco_boundary.geojson")


def osm_way_config_file_path() -> str:
    """OSM way features config file path."""
    return str(Path(__file__).parent.parent / "quackosm" / "osm_way_polygon_features.json")


def test_version() -> None:
    """Test if version is properly returned."""
    result = runner.invoke(cli.app, ["--version"])

    assert result.exit_code == 0
    assert f"{__app_name__} {__version__}\n" in result.stdout


def test_pbf_file_is_required() -> None:
    """Test if cannot run without pbf file."""
    result = runner.invoke(
        cli.app,
    )

    assert result.exit_code == 2
    assert "Missing argument 'PBF file path'." in result.stdout


def test_basic_run(monaco_pbf_file_path: str) -> None:
    """Test if runs properly without options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path])

    assert result.exit_code == 0
    assert str(Path("files/monaco_nofilter_noclip_compact.geoparquet")) in result.stdout


@P.parameters("args", "expected_result")  # type: ignore
@P.case(
    "Explode",
    ["--explode-tags"],
    "files/monaco_nofilter_noclip_exploded.geoparquet",
)  # type: ignore
@P.case("Explode short", ["--explode"], "files/monaco_nofilter_noclip_exploded.geoparquet")  # type: ignore
@P.case("Compact", ["--compact-tags"], "files/monaco_nofilter_noclip_compact.geoparquet")  # type: ignore
@P.case("Compact short", ["--compact"], "files/monaco_nofilter_noclip_compact.geoparquet")  # type: ignore
@P.case(
    "Working directory",
    ["--working-directory", "files/workdir"],
    "files/workdir/monaco_nofilter_noclip_compact.geoparquet",
)  # type: ignore
@P.case("Ignore cache", ["--ignore-cache"], "files/monaco_nofilter_noclip_compact.geoparquet")  # type: ignore
@P.case("Ignore cache short", ["--no-cache"], "files/monaco_nofilter_noclip_compact.geoparquet")  # type: ignore
@P.case("Output", ["--output", "files/monaco_output.geoparquet"], "files/monaco_output.geoparquet")  # type: ignore
@P.case("Output short", ["-o", "files/monaco_output.geoparquet"], "files/monaco_output.geoparquet")  # type: ignore
@P.case(
    "Output with working directory",
    ["--working-directory", "files/workdir", "-o", "files/monaco_output.geoparquet"],
    "files/monaco_output.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter compact",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry WKT filter",
    ["--geom-filter-wkt", geometry_wkt()],
    "files/monaco_nofilter_430020b6b1ba7bef8ea919b2fb4472dab2972c70a2abae253760a56c29f449c4_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/monaco_nofilter_425d42d3ce2360a6fab066b8c322da29e0df53b75c617b7e4f891ef4d7691f8e_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry file filter",
    ["--geom-filter-file", geometry_boundary_file_path()],
    "files/monaco_nofilter_faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_compact.geoparquet",
)  # type: ignore
@P.case(
    "Filter OSM",
    [
        "--filter-osm-id",
        "way/94399646",
        "--filter",
        "node/3617982224",
        "--filter",
        "relation/36990",
    ],
    "files/monaco_nofilter_noclip_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.geoparquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    ["--osm-way-polygon-config", osm_way_config_file_path()],
    "files/monaco_nofilter_noclip_compact.geoparquet",
)  # type: ignore
def test_proper_args(monaco_pbf_file_path: str, args: list[str], expected_result: str) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path, *args])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


@P.parameters("args")  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--osm-tags-filter",
        '{"building": True, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--osm-tags-filter",
        '{"building": true, highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"',
    ],
)  # type: ignore
@P.case("Geometry WKT filter with GeoJSON", ["--geom-filter-wkt", geometry_geojson()])  # type: ignore
@P.case("Geometry GeoJSON filter with WKT", ["--geom-filter-geojson", geometry_wkt()])  # type: ignore
@P.case(
    "Geometry two filters",
    ["--geom-filter-geojson", geometry_geojson(), "--geom-filter-wkt", geometry_wkt()],
)  # type: ignore
@P.case(
    "Geometry nonexistent file filter",
    ["--geom-filter-file", "nonexistent_geojson_file.geojson"],
)  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "w/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "w124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "way124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "n/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "n124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "node124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "r/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "r124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-id", "relation124"])  # type: ignore
@P.case("OSM way polygon config", ["--osm-way-polygon-config", "nonexistent_json_file.json"])  # type: ignore
def test_wrong_args(monaco_pbf_file_path: str, args: list[str]) -> None:
    """Test if doesn't run properly with options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path, *args])

    assert result.exit_code != 0
