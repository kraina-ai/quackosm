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
    return str(Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf")


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
    return str(Path(__file__).parent.parent / "test_files" / "monaco_boundary.geojson")


def osm_tags_filter_file_path() -> str:
    """OSM tags filter file path."""
    return str(Path(__file__).parent.parent / "test_files" / "osm_tags_filter.json")


def osm_way_config_file_path() -> str:
    """OSM way features config file path."""
    return str(Path(__file__).parent.parent.parent / "quackosm" / "osm_way_polygon_features.json")


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
    "OSM tags filter file",
    [
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter file compact",
    [
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
        "--compact",
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped",
    [
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
    ],
    "files/monaco_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_noclip_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped compact",
    [
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
        "--compact",
    ],
    "files/monaco_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_noclip_compact.geoparquet",
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
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/monaco_nofilter_e7f0b78a0fdc16c4db31c9767fa4e639eadaa8e83a9b90e07b521f4925cdf4b3_compact.geoparquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    [
        "--filter-osm-ids",
        "way/94399646,node/3617982224,relation/36990"
    ],
    "files/monaco_nofilter_noclip_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.geoparquet",
)  # type: ignore
@P.case(
    "Keep all tags",
    [
        "--keep-all-tags",
    ],
    "files/monaco_nofilter_noclip_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_noclip_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags compact",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--compact",
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_noclip_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter with keep all tags exploded",
    [
        "--keep-all-tags",
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--explode",
    ],
    "files/monaco_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_noclip_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    ["--osm-way-polygon-config", osm_way_config_file_path()],
    "files/monaco_nofilter_noclip_compact.geoparquet",
)  # type: ignore
def test_proper_args_with_pbf(
    monaco_pbf_file_path: str, args: list[str], expected_result: str
) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [monaco_pbf_file_path, *args])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


@P.parameters("args", "expected_result")  # type: ignore
@P.case(
    "Geometry WKT filter",
    ["--geom-filter-wkt", geometry_wkt()],
    "files/430020b6b1ba7bef8ea919b2fb4472dab2972c70a2abae253760a56c29f449c4_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/425d42d3ce2360a6fab066b8c322da29e0df53b75c617b7e4f891ef4d7691f8e_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/e7f0b78a0fdc16c4db31c9767fa4e639eadaa8e83a9b90e07b521f4925cdf4b3_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry file filter with different OSM source",
    ["--geom-filter-file", geometry_boundary_file_path(), "--osm-extract-source", "OSMfr"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Explode",
    ["--geom-filter-file", geometry_boundary_file_path(), "--explode-tags"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_exploded.geoparquet",
)  # type: ignore
@P.case(
    "Explode short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--explode"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_exploded.geoparquet",
)  # type: ignore
@P.case(
    "Compact",
    ["--geom-filter-file", geometry_boundary_file_path(), "--compact-tags"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Compact short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--compact"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Working directory",
    ["--geom-filter-file", geometry_boundary_file_path(), "--working-directory", "files/workdir"],
    "files/workdir/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Ignore cache",
    ["--geom-filter-file", geometry_boundary_file_path(), "--ignore-cache"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Ignore cache short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--no-cache"],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Output",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--output",
        "files/monaco_output.geoparquet",
    ],
    "files/monaco_output.geoparquet",
)  # type: ignore
@P.case(
    "Output short",
    ["--geom-filter-file", geometry_boundary_file_path(), "-o", "files/monaco_output.geoparquet"],
    "files/monaco_output.geoparquet",
)  # type: ignore
@P.case(
    "Output with working directory",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--working-directory",
        "files/workdir",
        "-o",
        "files/monaco_output.geoparquet",
    ],
    "files/monaco_output.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_exploded.geoparquet",
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
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter file",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter file compact",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
        "--compact",
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_exploded.geoparquet",
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
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_compact.geoparquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--filter-osm-ids",
        "way/94399646,node/3617982224,relation/36990",
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.geoparquet",
)  # type: ignore
@P.case(
    "Keep all tags",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
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
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_compact.geoparquet",
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
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_compact.geoparquet",
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
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-way-polygon-config",
        osm_way_config_file_path(),
    ],
    "files/faa97944af11ef7ce600da6d737b5dd94393fd48c3d8f853eacff3b2b46376c9_nofilter_compact.geoparquet",
)  # type: ignore
def test_proper_args_without_pbf(args: list[str], expected_result: str) -> None:
    """Test if runs properly with options."""
    result = runner.invoke(cli.app, [*args])
    print(result.stdout)

    assert result.exit_code == 0
    assert str(Path(expected_result)) in result.stdout


@P.parameters("args")  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        "--osm-tags-filter",
        '{"building": True, "highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        "--osm-tags-filter",
        '{"building": true, highway": ["primary", "secondary"], "amenity": "bench"}',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter malfunctioned JSON",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"',
    ],
)  # type: ignore
@P.case(
    "OSM tags filter wrong type",
    [
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
        "--osm-tags-filter",
        '{"group": [{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}] }',
    ],
)  # type: ignore
@P.case(
    "OSM tags two filters",
    [
        "--osm-tags-filter",
        '{"building": true, "highway": ["primary", "secondary"], "amenity": "bench"}',
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
)  # type: ignore
@P.case(
    "OSM tags nonexistent file filter",
    ["--osm-tags-filter-file", "nonexistent_json_file.json"],
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
@P.case(
    "Geometry wrong file filter",
    ["--geom-filter-file", osm_tags_filter_file_path()],
)  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "w/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "w124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "way124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "n/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "n124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "node124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "r/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "r124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "relation124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "node/124;way/124;relation/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "node/124|way/124|relation/124"])  # type: ignore
@P.case("Filter OSM", ["--filter-osm-ids", "node/124 way/124 relation/124"])  # type: ignore
@P.case("OSM way polygon config", ["--osm-way-polygon-config", "nonexistent_json_file.json"])  # type: ignore
def test_wrong_args(
    monaco_pbf_file_path: str, args: list[str], capsys: pytest.CaptureFixture
) -> None:
    """Test if doesn't run properly with options."""
    # Fix for the I/O error from the Click repository
    # https://github.com/pallets/click/issues/824#issuecomment-1583293065
    with capsys.disabled():
        result = runner.invoke(cli.app, [monaco_pbf_file_path, *args])
        assert result.exit_code != 0
