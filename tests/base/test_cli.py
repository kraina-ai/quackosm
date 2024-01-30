"""Tests for CLI."""

import uuid
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
    "files/monaco_nofilter_09c3fc0471538594b784be7c52782837c7a26753c2b26097b780581fa0a6bfc6_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/monaco_nofilter_5cfa6125bdd0e82004a18ca8a350ca44a457d2d4873b6f0af76e05ce674d6555_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry file filter",
    ["--geom-filter-file", geometry_boundary_file_path()],
    "files/monaco_nofilter_662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/monaco_nofilter_e7f0b78a0fdc16c4db31c9767fa4e639eadaa8e83a9b90e07b521f4925cdf4b3_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter",
    ["--geom-filter-index-geohash", "spv2bc"],
    "files/monaco_nofilter_9df2c7e6a4bc7f99ae2870c05eccaf3e047366339cd618554b0896ab320bfa98_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter multiple",
    ["--geom-filter-index-geohash", "spv2bc,spv2bfr"],
    "files/monaco_nofilter_87a73bb5abc026719ea5cb6fa2a97f46c94da8be2fe20a6d1fb981ea55bde719_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter",
    ["--geom-filter-index-h3", "8a3969a40ac7fff"],
    "files/monaco_nofilter_54d1c946b6bd3a6bb48b972bb52fe4e69ab4ac54ec8be32cdfcc298f1d788c93_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter multiple",
    ["--geom-filter-index-h3", "8a3969a40ac7fff,893969a4037ffff"],
    "files/monaco_nofilter_82f79c1b2ce9f7ad8546b82a63b35deeea225563fee14101b316769ed9fa9522_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter",
    ["--geom-filter-index-s2", "12cdc28bc"],
    "files/monaco_nofilter_03d2f71601164201e02dc205202db3fc58d5b6636ace06237fa797b5c6d47191_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter multiple",
    ["--geom-filter-index-s2", "12cdc28bc,12cdc28f"],
    "files/monaco_nofilter_92c9283112ac84537d0bf52f5510fb5246104076f82d69b2b9dfd70f0e7ddf03_compact.geoparquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    ["--filter-osm-ids", "way/94399646,node/3617982224,relation/36990"],
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
    "files/09c3fc0471538594b784be7c52782837c7a26753c2b26097b780581fa0a6bfc6_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry GeoJSON filter",
    ["--geom-filter-geojson", geometry_geojson()],
    "files/5cfa6125bdd0e82004a18ca8a350ca44a457d2d4873b6f0af76e05ce674d6555_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry geocode filter",
    ["--geom-filter-geocode", "Monaco-Ville, Monaco"],
    "files/e7f0b78a0fdc16c4db31c9767fa4e639eadaa8e83a9b90e07b521f4925cdf4b3_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter",
    ["--geom-filter-index-geohash", "spv2bc"],
    "files/9df2c7e6a4bc7f99ae2870c05eccaf3e047366339cd618554b0896ab320bfa98_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry Geohash filter multiple",
    ["--geom-filter-index-geohash", "spv2bc,spv2bfr"],
    "files/87a73bb5abc026719ea5cb6fa2a97f46c94da8be2fe20a6d1fb981ea55bde719_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter",
    ["--geom-filter-index-h3", "8a3969a40ac7fff"],
    "files/54d1c946b6bd3a6bb48b972bb52fe4e69ab4ac54ec8be32cdfcc298f1d788c93_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry H3 filter multiple",
    ["--geom-filter-index-h3", "8a3969a40ac7fff,893969a4037ffff"],
    "files/82f79c1b2ce9f7ad8546b82a63b35deeea225563fee14101b316769ed9fa9522_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter",
    ["--geom-filter-index-s2", "12cdc28bc"],
    "files/03d2f71601164201e02dc205202db3fc58d5b6636ace06237fa797b5c6d47191_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry S2 filter multiple",
    ["--geom-filter-index-s2", "12cdc28bc,12cdc28f"],
    "files/92c9283112ac84537d0bf52f5510fb5246104076f82d69b2b9dfd70f0e7ddf03_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Geometry file filter with different OSM source",
    ["--geom-filter-file", geometry_boundary_file_path(), "--osm-extract-source", "OSMfr"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Explode",
    ["--geom-filter-file", geometry_boundary_file_path(), "--explode-tags"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_exploded.geoparquet",
)  # type: ignore
@P.case(
    "Explode short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--explode"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_exploded.geoparquet",
)  # type: ignore
@P.case(
    "Compact",
    ["--geom-filter-file", geometry_boundary_file_path(), "--compact-tags"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Compact short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--compact"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Working directory",
    ["--geom-filter-file", geometry_boundary_file_path(), "--working-directory", "files/workdir"],
    "files/workdir/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Ignore cache",
    ["--geom-filter-file", geometry_boundary_file_path(), "--ignore-cache"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
)  # type: ignore
@P.case(
    "Ignore cache short",
    ["--geom-filter-file", geometry_boundary_file_path(), "--no-cache"],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_exploded.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter file",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter-file",
        osm_tags_filter_file_path(),
    ],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_exploded.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_compact.geoparquet",
)  # type: ignore
@P.case(
    "OSM tags filter grouped",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-tags-filter",
        '{"group": {"building": true, "highway": ["primary", "secondary"], "amenity": "bench"} }',
    ],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_exploded.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_654daac5550b95c8c0e3c57a75a1e16dfa638946461e0977af8f9ca98039db06_compact.geoparquet",
)  # type: ignore
@P.case(
    "Filter OSM features IDs",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--filter-osm-ids",
        "way/94399646,node/3617982224,relation/36990",
    ],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.geoparquet",
)  # type: ignore
@P.case(
    "Keep all tags",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--keep-all-tags",
    ],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_compact.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_compact.geoparquet",
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
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_exploded.geoparquet",
)  # type: ignore
@P.case(
    "OSM way polygon config",
    [
        "--geom-filter-file",
        geometry_boundary_file_path(),
        "--osm-way-polygon-config",
        osm_way_config_file_path(),
    ],
    "files/662c38fcc55281495384eda7f626fc5c8340efbd905f3b9fb1779f76438b9c4c_nofilter_compact.geoparquet",
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
@P.case("Geometry Geohash filter with random string", ["--geom-filter-index-geohash", random_str()])  # type: ignore
@P.case("Geometry H3 filter with Geohash", ["--geom-filter-index-h3", "spv2bc"])  # type: ignore
@P.case("Geometry H3 filter with S2", ["--geom-filter-index-h3", "12cdc28bc"])  # type: ignore
@P.case("Geometry H3 filter with random string", ["--geom-filter-index-h3", random_str()])  # type: ignore
@P.case("Geometry S2 filter with random string", ["--geom-filter-index-s2", random_str()])  # type: ignore
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
