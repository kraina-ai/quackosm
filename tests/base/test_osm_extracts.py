"""Tests related to OSM extracts."""

import datetime
import tempfile
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Any
from unittest import TestCase

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta
from parametrization import Parametrization as P
from pytest_mock import MockerFixture
from rich.console import Console
from shapely import box, from_wkt
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from tqdm import tqdm

from quackosm._exceptions import (
    GeometryNotCoveredError,
    GeometryNotCoveredWarning,
    MissingOsmCacheWarning,
    OldOsmCacheWarning,
    OsmExtractIndexOutdatedWarning,
    OsmExtractMultipleMatchesError,
    OsmExtractUnavailableWarning,
    OsmExtractZeroMatchesError,
)
from quackosm.geocode import geocode_to_geometry
from quackosm.osm_extracts import (
    OsmExtractSource,
    _get_index_for_sources,
    _resolve_extract_sources,
    clear_osm_index_cache,
    display_available_extracts,
    download_extracts_pbf_files,
    find_and_download_extracts_pbf_files,
    find_smallest_containing_extracts,
    find_smallest_containing_extracts_total,
    get_extract_by_query,
)
from quackosm.osm_extracts._geojson_parser import parse_geojson
from quackosm.osm_extracts.bbbike import (
    BBBIKE_EXTRACTS_INDEX_URL,
    _load_bbbike_index,
)
from quackosm.osm_extracts.extract import (
    OpenStreetMapExtract,
    _download_precalculated_index_from_github,
    _ensure_valid_geometries,
    _get_full_file_name_function,
    _get_global_cache_file_path,
    _get_local_cache_file_path,
    _migrate_legacy_geojson_cache,
)
from quackosm.osm_extracts.extracts_tree import get_available_extracts_as_rich_tree
from quackosm.osm_extracts.geo2day import _find_subregion_links
from quackosm.osm_extracts.geofabrik import _load_geofabrik_index, _parse_geofabrik_index
from quackosm.osm_extracts.movisda import (
    MOVISDA_ADMIN_PBF_BASE_URL,
    _parse_movisda_features,
)
from quackosm.osm_extracts.osm_fr import OPENSTREETMAP_FR_EXTRACTS_INDEX_URL

ut = TestCase()


@P.parameters("value")  # type: ignore
@P.case(
    "Base any",
    "any",
)  # type: ignore
@P.case(
    "Case insensitive any",
    "aNy",
)  # type: ignore
@P.case(
    "Base Geofabrik",
    "Geofabrik",
)  # type: ignore
@P.case(
    "Case insensitive Geofabrik",
    "GEOFABRIK",
)  # type: ignore
@P.case(
    "OSM fr without underscore",
    "osmfr",
)  # type: ignore
def test_proper_osm_extract_source(value: str) -> None:
    """Test if OsmExtractSource is parsed correctly."""
    OsmExtractSource(value)


def test_wrong_osm_extract_source():  # type: ignore
    """Test if cannot load incorrect OsmExtractSource."""
    with pytest.raises(ValueError):
        OsmExtractSource("test_source")


@P.parameters("source", "geometry", "expected_extract_id")  # type: ignore
@P.case(
    "Vatican - any",
    "any",
    from_wkt(
        "POLYGON ((12.450637854252449 41.904910802544634, 12.450637854252449 41.901790362263796,"
        " 12.455878610023916 41.901790362263796, 12.455878610023916 41.904910802544634,"
        " 12.450637854252449 41.904910802544634))"
    ),
    "GEO2Day_europe_vatican_city",
)  # type: ignore
@P.case(
    "Vatican - Geofabrik",
    "Geofabrik",
    from_wkt(
        "POLYGON ((12.450637854252449 41.904910802544634, 12.450637854252449 41.901790362263796,"
        " 12.455878610023916 41.901790362263796, 12.455878610023916 41.904910802544634,"
        " 12.450637854252449 41.904910802544634))"
    ),
    "Geofabrik_centro",
)  # type: ignore
@P.case(
    "London - any",
    "any",
    from_wkt(
        "POLYGON ((-0.1514787822171684 51.49843445562462, -0.1514787822171684 51.48926140694954,"
        " -0.1293785532031677 51.48926140694954, -0.1293785532031677 51.49843445562462,"
        " -0.1514787822171684 51.49843445562462))"
    ),
    "Geofabrik_greater-london",
)  # type: ignore
@P.case(
    "London - BBBike",
    "BBBike",
    from_wkt(
        "POLYGON ((-0.1514787822171684 51.49843445562462, -0.1514787822171684 51.48926140694954,"
        " -0.1293785532031677 51.48926140694954, -0.1293785532031677 51.49843445562462,"
        " -0.1514787822171684 51.49843445562462))"
    ),
    "BBBike_London",
)  # type: ignore
@P.case(
    "Vancouver - any",
    "any",
    from_wkt(
        "POLYGON ((-123.15817514738828 49.29493379142323, -123.15817514738828 49.23700029433431,"
        " -123.07449492760279 49.23700029433431, -123.07449492760279 49.29493379142323,"
        " -123.15817514738828 49.29493379142323))"
    ),
    "BBBike_Vancouver",
)  # type: ignore
@P.case(
    "Vancouver - OSM.fr",
    "osmfr",
    from_wkt(
        "POLYGON ((-123.15817514738828 49.29493379142323, -123.15817514738828 49.23700029433431,"
        " -123.07449492760279 49.23700029433431, -123.07449492760279 49.29493379142323,"
        " -123.15817514738828 49.29493379142323))"
    ),
    "osmfr_north-america_canada_british_columbia",
)  # type: ignore
def test_single_smallest_extract(
    source: str, geometry: BaseGeometry, expected_extract_id: str
) -> None:
    """Test if extracts matching works correctly for geometries within borders."""
    extracts = find_smallest_containing_extracts(geometry, source)
    assert len(extracts) == 1
    assert extracts[0].id == expected_extract_id, f"{extracts[0].id} vs {expected_extract_id}"


@P.parameters(
    "source", "geometry", "geometry_coverage_iou_threshold", "expected_extract_file_names"
)  # type: ignore
@P.case(
    "Andorra bbox, osmfr, iou default",
    "osmfr",
    from_wkt(
        "POLYGON ((1.382599544073372 42.67676873293743, 1.382599544073372 42.40065303248514,"
        " 1.8092269635579328 42.40065303248514, 1.8092269635579328 42.67676873293743,"
        " 1.382599544073372 42.67676873293743))"
    ),
    0.01,
    [
        "osmfr_europe_spain_catalunya_lleida",
        "osmfr_europe_france_midi_pyrenees_ariege",
        "osmfr_europe_france_languedoc_roussillon_pyrenees_orientales",
        "osmfr_europe_andorra",
    ],
)  # type: ignore
@P.case(
    "Andorra bbox, any, iou 0",
    "any",
    from_wkt(
        "POLYGON ((1.382599544073372 42.67676873293743, 1.382599544073372 42.40065303248514,"
        " 1.8092269635579328 42.40065303248514, 1.8092269635579328 42.67676873293743,"
        " 1.382599544073372 42.67676873293743))"
    ),
    0,
    [
        "movisda-grid_n42w001",
    ],
)  # type: ignore
@P.case(
    "Andorra geocode, geofabrik, iou default",
    "geofabrik",
    geocode_to_geometry("Andorra"),
    0.01,
    ["geofabrik_europe_andorra"],
)  # type: ignore
@P.case(
    "Andorra geocode, osmfr, iou 0",
    "osmfr",
    geocode_to_geometry("Andorra"),
    0,
    ["osmfr_europe_andorra"],
)  # type: ignore
@P.case(
    "CZ/PL/DE bbox, any, iou 0",
    "any",
    box(14.456635, 50.686018, 15.247650, 51.140586),
    0,
    ["movisda-grid_n51w015", "bbbike_goerlitz", "geo2day_europe_czech_republic_liberecky"],
)  # type: ignore
def test_multiple_smallest_extracts(
    source: str,
    geometry: BaseGeometry,
    geometry_coverage_iou_threshold: float,
    expected_extract_file_names: list[str],
) -> None:
    """Test if extracts matching works correctly for geometries between borders."""
    extracts = find_smallest_containing_extracts(
        geometry, source, geometry_coverage_iou_threshold=geometry_coverage_iou_threshold
    )
    ut.assertListEqual([extract.file_name for extract in extracts], expected_extract_file_names)


@pytest.mark.parametrize(
    "expectation,allow_uncovered_geometry,geometry_coverage_iou_threshold",
    [
        (pytest.raises(GeometryNotCoveredError), False, 0.01),
        (pytest.raises(ValueError), False, -0.1),
        (pytest.raises(ValueError), False, 1.2),
        (pytest.raises(ValueError), True, 1.2),
        (pytest.warns(GeometryNotCoveredWarning), True, 0.01),
    ],
)  # type: ignore
def test_uncovered_geometry_extract(
    expectation: Any, allow_uncovered_geometry: bool, geometry_coverage_iou_threshold: float
) -> None:
    """Test if raises errors as expected when geometry can't be covered."""
    with expectation:
        geometry = from_wkt(
            "POLYGON ((-43.064 29.673, -43.064 29.644, -43.017 29.644,"
            " -43.017 29.673, -43.064 29.673))"
        )
        find_smallest_containing_extracts_total(
            geometry=geometry,
            allow_uncovered_geometry=allow_uncovered_geometry,
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
        )


def test_excluded_extracts_ids() -> None:
    """Test if excluded extracts are skipped and coverage is recalculated."""
    geometry = geocode_to_geometry("Andorra")

    extracts = find_smallest_containing_extracts(geometry, "geofabrik")
    ut.assertListEqual([extract.file_name for extract in extracts], ["geofabrik_europe_andorra"])

    excluded_extracts_ids = {extracts[0].id}
    fallback_extracts = find_smallest_containing_extracts(
        geometry, "geofabrik", excluded_extracts_ids=excluded_extracts_ids
    )

    fallback_extracts_ids = {extract.id for extract in fallback_extracts}
    assert excluded_extracts_ids.isdisjoint(fallback_extracts_ids)
    assert len(fallback_extracts) >= 1


def test_find_and_download_excludes_unavailable_extracts(mocker: MockerFixture) -> None:
    """Test if unavailable extracts are excluded and the coverage is recalculated."""
    from requests.exceptions import HTTPError

    geometry = geocode_to_geometry("Andorra")
    matching_extracts = find_smallest_containing_extracts(geometry, "geofabrik")
    failing_extract_id = matching_extracts[0].id

    def fake_download(
        extract: OpenStreetMapExtract, download_directory: Path, progressbar: bool = True
    ) -> Path:
        if extract.id == failing_extract_id:
            raise HTTPError("Extract unavailable")
        return Path(download_directory) / f"{extract.file_name}.osm.pbf"

    mocker.patch("quackosm.osm_extracts._download_single_extract", side_effect=fake_download)

    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.warns(OsmExtractUnavailableWarning):
            result = find_and_download_extracts_pbf_files(geometry, "geofabrik", tmp_dir)

    result_extracts_ids = {extract.id for extract, _ in result}
    assert failing_extract_id not in result_extracts_ids
    assert len(result) >= 1
    assert all(isinstance(pbf_path, Path) for _, pbf_path in result)


def test_download_extracts_pbf_files_raises_on_unavailable(mocker: MockerFixture) -> None:
    """Test if the public download function keeps raising on errors (back-compat)."""
    from requests.exceptions import HTTPError

    extract = OpenStreetMapExtract(
        id="test_extract",
        name="test_extract",
        parent="",
        url="http://example.com/test_extract.osm.pbf",
        geometry=box(0, 0, 1, 1),
        file_name="test_extract",
    )
    mocker.patch(
        "quackosm.osm_extracts._download_single_extract",
        side_effect=HTTPError("Extract unavailable"),
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(HTTPError):
            download_extracts_pbf_files([extract], Path(tmp_dir))


@pytest.mark.parametrize(
    "source,expected_sources",
    [
        ("geofabrik", [OsmExtractSource.geofabrik]),
        (OsmExtractSource.bbbike, [OsmExtractSource.bbbike]),
        ("GEO2Day", [OsmExtractSource.geo2day]),
        ("movisda-admin", [OsmExtractSource.movisda_admin]),
        (
            "any",
            [
                OsmExtractSource.bbbike,
                OsmExtractSource.geofabrik,
                OsmExtractSource.osm_fr,
                OsmExtractSource.geo2day,
                OsmExtractSource.movisda_admin,
                OsmExtractSource.movisda_grid,
            ],
        ),
        (
            "GEO2Day,Movisda-grid",
            [OsmExtractSource.geo2day, OsmExtractSource.movisda_grid],
        ),
        (["bbbike", "osmfr"], [OsmExtractSource.bbbike, OsmExtractSource.osm_fr]),
        (
            [OsmExtractSource.bbbike, OsmExtractSource.osm_fr],
            [OsmExtractSource.bbbike, OsmExtractSource.osm_fr],
        ),
        ("bbbike,osmfr", [OsmExtractSource.bbbike, OsmExtractSource.osm_fr]),
        ("bbbike, osmfr, bbbike", [OsmExtractSource.bbbike, OsmExtractSource.osm_fr]),
        (
            ["geofabrik", "any"],
            [
                OsmExtractSource.geofabrik,
                OsmExtractSource.bbbike,
                OsmExtractSource.osm_fr,
                OsmExtractSource.geo2day,
                OsmExtractSource.movisda_admin,
                OsmExtractSource.movisda_grid,
            ],
        ),
        ("BBBike", [OsmExtractSource.bbbike]),
    ],
)  # type: ignore
def test_resolve_extract_sources(source: Any, expected_sources: list[OsmExtractSource]) -> None:
    """Test if source specifications are normalized into concrete sources."""
    resolved_sources = _resolve_extract_sources(source)
    # Order is not significant (the combined index is sorted by area downstream),
    # but the result must be deduplicated.
    assert len(resolved_sources) == len(set(resolved_sources))
    assert set(resolved_sources) == set(expected_sources)


@pytest.mark.parametrize("source", ["", "nonexistent_source"])  # type: ignore
def test_resolve_extract_sources_raises_on_invalid(source: str) -> None:
    """Test if invalid or empty source specifications raise ValueError."""
    with pytest.raises(ValueError):
        _resolve_extract_sources(source)


def test_get_index_for_multiple_sources(mocker: MockerFixture) -> None:
    """Test if indexes for multiple sources are concatenated."""
    import geopandas as gpd

    def fake_index(source_name: str) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            {"id": [source_name], "area": [1.0]},
            geometry=[box(0, 0, 1, 1)],
            crs="EPSG:4326",
        )

    mocker.patch.dict(
        "quackosm.osm_extracts.OSM_EXTRACT_SOURCE_INDEX_FUNCTION",
        {
            OsmExtractSource.bbbike: lambda: fake_index("bbbike"),
            OsmExtractSource.geofabrik: lambda: fake_index("geofabrik"),
            OsmExtractSource.osm_fr: lambda: fake_index("osmfr"),
        },
        clear=True,
    )

    single_index = _get_index_for_sources("bbbike")
    assert list(single_index["id"]) == ["bbbike"]

    combined_index = _get_index_for_sources(["bbbike", "osmfr"])
    assert set(combined_index["id"]) == {"bbbike", "osmfr"}


def test_proper_cache_saving() -> None:
    """Test if file is saved in cache properly."""
    save_path = _get_global_cache_file_path(OsmExtractSource.geofabrik)
    loaded_index = _load_geofabrik_index()
    assert save_path.exists()
    assert len(loaded_index.columns) == 7


@pytest.mark.parametrize(
    "geojson_data,expected_geometry",
    [
        (
            {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": mapping(box(0, 0, 1, 1)), "properties": {}}
                ],
            },
            box(0, 0, 1, 1),
        ),
        (
            {"type": "Feature", "geometry": mapping(box(2, 2, 3, 3)), "properties": {}},
            box(2, 2, 3, 3),
        ),
        (mapping(box(4, 4, 5, 5)), box(4, 4, 5, 5)),
    ],
)  # type: ignore
def test_parse_geojson(geojson_data: Any, expected_geometry: BaseGeometry) -> None:
    """Test if GeoJSON Feature/FeatureCollection/geometry is parsed into a single geometry."""
    parsed_geometry = parse_geojson(geojson_data)
    assert parsed_geometry is not None
    assert parsed_geometry.equals(expected_geometry)


def test_parse_geojson_empty_feature_collection() -> None:
    """Test if an empty FeatureCollection returns None."""
    assert parse_geojson({"type": "FeatureCollection", "features": []}) is None


def test_movisda_parse_features() -> None:
    """Test if Movisda features are parsed into extracts with correct download URLs."""
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"prefix": "AD-"},
                "geometry": mapping(box(1, 1, 2, 2)),
            },
            {
                "type": "Feature",
                "properties": {"prefix": "N52W016-"},
                "geometry": mapping(box(3, 3, 4, 4)),
            },
        ],
    }
    extracts = _parse_movisda_features(
        geojson_data, MOVISDA_ADMIN_PBF_BASE_URL, OsmExtractSource.movisda_admin.value
    )

    assert extracts[0].id == "Movisda-admin_AD"
    assert extracts[0].name == "AD"
    assert extracts[0].parent == "Movisda-admin"
    assert extracts[0].url == "https://osm.download.movisda.io/admin/AD-latest.osm.pbf"
    assert extracts[0].geometry.equals(box(1, 1, 2, 2))
    assert extracts[1].url == "https://osm.download.movisda.io/admin/N52W016-latest.osm.pbf"


def test_geo2day_find_subregion_links() -> None:
    """Test if only direct (one level deeper) sub-region links are detected."""
    from bs4 import BeautifulSoup

    home_html = (
        '<a href="https://geo2day.com/europe.html">Europe</a>'
        '<a href="#">self</a>'
        '<a href="https://geo2day.com/">Home</a>'
    )
    assert _find_subregion_links(
        "https://geo2day.com/", BeautifulSoup(home_html, "html.parser")
    ) == [("https://geo2day.com/europe.html", "europe")]

    germany_html = (
        '<a href="https://geo2day.com/europe.html">Europe (breadcrumb)</a>'
        '<a href="https://geo2day.com/europe/germany/bayern.html">Bavaria</a>'
    )
    assert _find_subregion_links(
        "https://geo2day.com/europe/germany.html", BeautifulSoup(germany_html, "html.parser")
    ) == [("https://geo2day.com/europe/germany/bayern.html", "bayern")]


def test_geo2day_two_phase_gather_and_parse(mocker: MockerFixture) -> None:
    """Test if regions are first enumerated (with total) and then parsed into extracts."""
    import quackosm.osm_extracts.geo2day as geo2day_module

    pages = {
        "https://geo2day.com/": '<a href="https://geo2day.com/europe.html">Europe</a>',
        "https://geo2day.com/europe.html": (
            '<a href="https://geo2day.com/europe.html">self</a>'
            '<a href="https://geo2day.com/europe/poland.html">Poland</a>'
        ),
        "https://geo2day.com/europe/poland.html": (
            '<a href="https://geo2day.com/europe.html">parent</a>'
        ),
    }

    def fake_get(url: str, headers: Any = None) -> Any:
        response = mocker.Mock()
        response.status_code = 200
        response.raise_for_status = lambda: None
        response.text = pages.get(url, "")
        return response

    mocker.patch("quackosm.osm_extracts.geo2day.requests.get", side_effect=fake_get)
    mocker.patch.object(geo2day_module, "parse_geojson_file", return_value=box(0, 0, 1, 1))

    with tqdm(disable=True) as pbar:
        # Phase 1: enumerate regions and set the progress bar total (no geometry downloaded yet).
        region_objects = geo2day_module._gather_all_geo2day_urls(
            "GEO2Day", "https://geo2day.com/", pbar
        )
        assert pbar.total == 2
        assert {region[0] for region in region_objects} == {
            "GEO2Day_europe",
            "GEO2Day_europe_poland",
        }

        # Phase 2: download geometries and build extracts.
        extracts = geo2day_module._parse_geo2day_urls(pbar=pbar, region_objects=region_objects)

    by_id = {extract.id: extract for extract in extracts}
    assert by_id["GEO2Day_europe"].parent == "GEO2Day"
    assert by_id["GEO2Day_europe"].url == "https://geo2day.com/europe.pbf"
    assert by_id["GEO2Day_europe_poland"].parent == "GEO2Day_europe"
    assert by_id["GEO2Day_europe_poland"].url == "https://geo2day.com/europe/poland.pbf"


def test_geofabrik_parse_index() -> None:
    """Test if a Geofabrik index-v1.json payload is parsed into extracts with proper ids."""
    parsed_data = {
        "features": [
            {
                "type": "Feature",
                "geometry": mapping(box(1, 42, 2, 43)),
                "properties": {
                    "id": "andorra",
                    "parent": "europe",
                    "name": "Andorra",
                    "urls": {"pbf": "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"},
                },
            },
            {
                "type": "Feature",
                "geometry": mapping(box(-10, 35, 40, 70)),
                "properties": {
                    "id": "europe",
                    "name": "Europe",
                    "urls": {"pbf": "https://download.geofabrik.de/europe-latest.osm.pbf"},
                },
            },
            {
                "type": "Feature",
                "geometry": mapping(box(-125, 32, -114, 42)),
                "properties": {
                    "id": "us/california",
                    "parent": "us",
                    "name": "California",
                    "urls": {
                        "pbf": (
                            "https://download.geofabrik.de/north-america/us/"
                            "california-latest.osm.pbf"
                        )
                    },
                },
            },
        ]
    }

    gdf = _parse_geofabrik_index(parsed_data).set_index("id")

    assert gdf.loc["Geofabrik_andorra", "name"] == "andorra"
    assert gdf.loc["Geofabrik_andorra", "parent"] == "Geofabrik_europe"
    assert (
        gdf.loc["Geofabrik_andorra", "url"]
        == "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
    )
    # Missing parent resolves to the source root.
    assert gdf.loc["Geofabrik_europe", "parent"] == "Geofabrik"
    # US sub-extracts have their parent forced to the `us` node.
    assert gdf.loc["Geofabrik_us/california", "parent"] == "Geofabrik_us"


def test_bbbike_iterate_index(mocker: MockerFixture) -> None:
    """Test if the BBBike directory listing and CSV fallback are parsed into extracts."""
    import quackosm.osm_extracts.bbbike as bbbike_module

    index_html = (
        "<table>"
        '<tr class="d"><td><a href="../">..</a></td></tr>'
        '<tr class="d"><td><a href="Aachen/">Aachen</a></td></tr>'
        '<tr class="d"><td><a href="Berlin/">Berlin</a></td></tr>'
        "</table>"
    )
    csv_text = "Berlin:0:1:2:3:4:13.0 52.3 13.8 52.7:rest\n"

    def fake_get(url: str, headers: Any = None) -> Any:
        response = mocker.Mock()
        response.status_code = 200
        response.raise_for_status = lambda: None
        response.text = index_html if url == BBBIKE_EXTRACTS_INDEX_URL else csv_text
        return response

    def fake_poly(url: str) -> Any:
        # Aachen has a poly file; Berlin falls back to the CSV bounding box.
        return box(6.0, 50.7, 6.2, 50.9) if "Aachen" in url else None

    mocker.patch("quackosm.osm_extracts.bbbike.requests.get", side_effect=fake_get)
    mocker.patch.object(bbbike_module, "parse_polygon_file", side_effect=fake_poly)

    extracts = bbbike_module._iterate_bbbike_index()
    by_id = {extract.id: extract for extract in extracts}

    assert set(by_id) == {"BBBike_Aachen", "BBBike_Berlin"}
    assert by_id["BBBike_Aachen"].parent == "BBBike"
    assert (
        by_id["BBBike_Aachen"].url == "https://download.bbbike.org/osm/bbbike/Aachen/Aachen.osm.pbf"
    )
    assert by_id["BBBike_Aachen"].geometry.equals(box(6.0, 50.7, 6.2, 50.9))
    # Berlin uses the CSV bounding box fallback.
    assert by_id["BBBike_Berlin"].geometry.equals(box(13.0, 52.3, 13.8, 52.7))


def test_osm_fr_gather_and_parse(mocker: MockerFixture) -> None:
    """Test if OSM.fr directory pages are enumerated then parsed into extracts."""
    import quackosm.osm_extracts.osm_fr as osm_fr_module

    root_html = (
        "<table>"
        '<tr><td><img src="/icons/folder.gif"></td>'
        '<td><a href="europe/">europe/</a></td></tr>'
        "</table>"
    )
    europe_html = (
        '<table><tr><td><a href="monaco-latest.osm.pbf">monaco-latest.osm.pbf</a></td></tr></table>'
    )

    def fake_get(url: str, headers: Any = None) -> Any:
        response = mocker.Mock()
        response.status_code = 200
        response.raise_for_status = lambda: None
        if url == f"{OPENSTREETMAP_FR_EXTRACTS_INDEX_URL}/":
            response.text = root_html
        elif url == f"{OPENSTREETMAP_FR_EXTRACTS_INDEX_URL}/europe/":
            response.text = europe_html
        else:
            response.text = ""
        return response

    mocker.patch("quackosm.osm_extracts.osm_fr.requests.get", side_effect=fake_get)
    mocker.patch.object(osm_fr_module, "parse_polygon_file", return_value=box(7.4, 43.7, 7.5, 43.8))

    with tqdm(disable=True) as pbar:
        # Phase 1: enumerate directory pages (one PBF discovered) and set the total.
        soup_objects = osm_fr_module._gather_all_openstreetmap_fr_urls("osmfr", "/", pbar)
        assert pbar.total == 1

        # Phase 2: download geometries and build extracts.
        extracts = osm_fr_module._parse_openstreetmap_fr_urls(
            pbar=pbar, extract_soup_objects=soup_objects
        )

    assert len(extracts) == 1
    extract = extracts[0]
    assert extract.id == "osmfr_europe_monaco"
    assert extract.name == "monaco"
    assert extract.parent == "osmfr_europe"
    assert extract.url == "https://download.openstreetmap.fr/extracts/europe/monaco-latest.osm.pbf"
    assert extract.geometry.equals(box(7.4, 43.7, 7.5, 43.8))


def test_wrong_cached_index() -> None:
    """Test if cached file with missing columns is redownloaded again."""
    save_path = _get_global_cache_file_path(OsmExtractSource.geofabrik)
    column_to_remove = "id"

    # load index first time
    first_index = _load_geofabrik_index()

    # remove the column and replace the file
    first_index.drop(columns=[column_to_remove]).to_parquet(
        save_path, compression="zstd", compression_level=3
    )

    with pytest.warns(OsmExtractIndexOutdatedWarning):
        # load index again
        second_index = _load_geofabrik_index()

    assert column_to_remove in second_index.columns


def test_proper_full_name() -> None:
    """Test if full names for extracts are properly generated."""
    test_index = pd.DataFrame({"id": ["1", "2"], "name": ["one", "two"], "parent": ["x", "1"]})
    prepared_function = _get_full_file_name_function(test_index)
    assert prepared_function("2") == "x_one_two"


@P.parameters("query", "source", "expectation", "matched_id", "exception_values")  # type: ignore
@P.case(
    "Proper full file name Geobfabrik",
    "geofabrik_north-america_us",
    "geofabrik",
    does_not_raise(),
    "Geofabrik_us",
    [],
)  # type: ignore
@P.case(
    "Proper full file name any",
    "geofabrik_north-america_us",
    "any",
    does_not_raise(),
    "Geofabrik_us",
    [],
)  # type: ignore
@P.case(
    "Proper name bbbike",
    "London",
    "BBBike",
    does_not_raise(),
    "BBBike_London",
    [],
)  # type: ignore
@P.case(
    "Proper name bbbike - upper case",
    "LONDON",
    "BBBike",
    does_not_raise(),
    "BBBike_London",
    [],
)  # type: ignore
@P.case(
    "Proper name any - with whitespaces",
    "   tete  ",
    "any",
    does_not_raise(),
    "osmfr_africa_mozambique_tete",
    [],
)  # type: ignore
@P.case(
    "Wrong query multiple matches - single source",
    "northeast",
    "any",
    pytest.raises(OsmExtractMultipleMatchesError),
    "",
    [
        "geo2day_south_america_brazil_northeast",
        "osmfr_north-america_us-midwest_illinois_northeast",
        "osmfr_north-america_us-south_florida_northeast",
        "osmfr_north-america_us-south_georgia_northeast",
        "osmfr_north-america_us-south_north-carolina_northeast",
        "osmfr_north-america_us-west_colorado_northeast",
        "osmfr_south-america_brazil_northeast",
    ],
)  # type: ignore
@P.case(
    "Wrong query multiple matches - multiple sources",
    "asia",
    "any",
    pytest.raises(OsmExtractMultipleMatchesError),
    "",
    ["geo2day_asia", "geofabrik_asia", "osmfr_asia"],
)  # type: ignore
@P.case(
    "Wrong query zero matches with suggestions - north",
    "nrth",
    "any",
    pytest.raises(OsmExtractZeroMatchesError),
    "",
    [
        "osmfr_north-america_us-midwest_illinois_north",
        "osmfr_north-america_us-south_texas_north",
        "geo2day_south_america_brazil_north",
        "osmfr_south-america_brazil_north",
    ],
)  # type: ignore
@P.case(
    "Wrong query zero matches with suggestions - prlnd",
    "prlnd",
    "any",
    pytest.raises(OsmExtractZeroMatchesError),
    "",
    [
        "bbbike_portland",
        "osmfr_europe_poland",
        "geo2day_europe_poland",
        "geofabrik_europe_poland",
    ],
)  # type: ignore
@P.case(
    "Wrong query zero matches without suggestions",
    "empty_extract",
    "any",
    pytest.raises(OsmExtractZeroMatchesError),
    "",
    [],
)  # type: ignore
def test_extracts_finding(
    query: str, source: str, expectation: Any, matched_id: str, exception_values: list[str]
) -> None:
    """Test if extracts finding by name works."""
    with expectation as exception_info:
        extract = get_extract_by_query(query, source)
        # if properly found - check id
        assert extract.id == matched_id

    # if threw exception - check resulting arrays
    if exception_info is not None:
        print(exception_info.value.matching_full_names)
        print(exception_values)
        ut.assertListEqual(exception_info.value.matching_full_names, exception_values)


@pytest.mark.parametrize(
    "use_full_names",
    [False, True],
)  # type: ignore
@pytest.mark.parametrize(
    "osm_source",
    list(OsmExtractSource),
)  # type: ignore
def test_extracts_tree_printing(
    capfd: Any, mocker: MockerFixture, osm_source: OsmExtractSource, use_full_names: bool
) -> None:
    """Test if displaying available extracts works."""
    mocker.patch("rich.get_console", return_value=Console(width=999))
    display_available_extracts(osm_source, use_full_names)
    output, error_output = capfd.readouterr()

    assert len(output) > 0

    osm_sources_without_any = [src for src in OsmExtractSource if src != OsmExtractSource.any]

    if osm_source == OsmExtractSource.any:
        assert output.startswith("All extracts")
        assert all(src.value in output for src in osm_sources_without_any)
    else:
        assert output.startswith(osm_source.value)

    if use_full_names:
        lines = output.lower().split("\n")

        assert all(
            any(src.value.lower() in line for src in osm_sources_without_any)
            for line in lines
            if len(line.strip()) > 0 and line != "all extracts"
        )

    assert error_output == ""


def _count_tree_nodes(tree: Any) -> int:
    """Count all descendant nodes of a Rich tree."""
    return len(tree.children) + sum(_count_tree_nodes(child) for child in tree.children)


def _render_rich_tree(tree: Any) -> str:
    """Render a Rich tree to plain text."""
    console = Console(width=999)
    with console.capture() as capture:
        console.print(tree)
    return str(capture.get())


def test_extracts_tree_structure_and_loose_parents() -> None:
    """Test if the tree nests children under parents and attaches loose parents."""
    import geopandas as gpd

    index = gpd.GeoDataFrame(
        [
            {
                "id": "BBBike_a",
                "name": "a",
                "file_name": "bbbike_a",
                "parent": "BBBike",
                "area": 2.0,
                "url": "http://x/a",
            },
            {
                "id": "BBBike_a_x",
                "name": "x",
                "file_name": "bbbike_a_x",
                "parent": "BBBike_a",
                "area": 1.0,
                "url": "http://x/x",
            },
            {
                "id": "BBBike_b",
                "name": "b",
                "file_name": "bbbike_b",
                "parent": "BBBike",
                "area": 3.0,
                "url": "http://x/b",
            },
            {
                "id": "BBBike_orphan",
                "name": "orphan",
                "file_name": "bbbike_orphan",
                "parent": "BBBike_missing",
                "area": 1.0,
                "url": "http://x/o",
            },
        ],
        geometry=[box(0, 0, 1, 1)] * 4,
        crs="EPSG:4326",
    )

    tree = get_available_extracts_as_rich_tree(
        OsmExtractSource.bbbike, {OsmExtractSource.bbbike: lambda: index}, use_full_names=True
    )
    rendered = _render_rich_tree(tree)

    for token in ("bbbike_a", "bbbike_a_x", "bbbike_b", "bbbike_orphan", "BBBike_missing"):
        assert token in rendered, token
    # Children sorted by name: a before b.
    assert rendered.index("bbbike_a") < rendered.index("bbbike_b")
    # Nodes: a, x (under a), b, BBBike_missing (loose), orphan (under loose) -> 5.
    assert _count_tree_nodes(tree) == 5


def test_extracts_tree_builds_for_large_flat_index() -> None:
    """Test if a large flat index builds quickly (guards against O(N^2) tree building)."""
    import geopandas as gpd

    number_of_tiles = 10000
    index = gpd.GeoDataFrame(
        [
            {
                "id": f"Movisda-grid_{i}",
                "name": f"N{i:05d}",
                "file_name": f"movisda-grid_n{i}",
                "parent": "Movisda-grid",
                "area": float(i % 1000 + 1),
                "url": f"http://x/{i}",
            }
            for i in range(number_of_tiles)
        ],
        geometry=[box(0, 0, 1, 1)] * number_of_tiles,
        crs="EPSG:4326",
    )

    tree = get_available_extracts_as_rich_tree(
        OsmExtractSource.movisda_grid, {OsmExtractSource.movisda_grid: lambda: index}
    )
    assert _count_tree_nodes(tree) == number_of_tiles


def test_generate_index_warning(mocker: MockerFixture) -> None:
    """Test if index generation results in warning."""
    extract_source = OsmExtractSource.bbbike
    global_path = _get_global_cache_file_path(extract_source)
    local_path = _get_local_cache_file_path(extract_source)

    move_global_path = global_path.exists()
    move_local_path = local_path.exists()

    if move_global_path:
        global_moved_path = global_path.with_name("bbbike_index_moved.parquet")
        global_path.rename(global_moved_path)

    if move_local_path:
        local_moved_path = local_path.with_name("bbbike_index_moved.parquet")
        local_path.rename(local_moved_path)

    try:
        mocker.patch(
            "quackosm.osm_extracts.bbbike._iterate_bbbike_index",
            return_value=[
                OpenStreetMapExtract(
                    id="bbbike_test",
                    name="test",
                    parent="bbbike",
                    url="test_url",
                    geometry=box(0, 0, 1, 1),
                )
            ],
        )
        mocker.patch("quackosm.osm_extracts.bbbike.BBBIKE_INDEX_GDF", new=None)
        with pytest.warns(MissingOsmCacheWarning):
            _load_bbbike_index(force_recalculation=True)

    finally:
        if move_global_path:
            global_path.unlink(missing_ok=True)
            global_moved_path.rename(global_path)
            global_moved_path.unlink(missing_ok=True)

        if move_local_path:
            local_path.unlink(missing_ok=True)
            local_moved_path.rename(local_path)
            local_moved_path.unlink(missing_ok=True)


def test_old_index_warning(mocker: MockerFixture) -> None:
    """Test if old index results in warning."""
    extract_source = OsmExtractSource.bbbike

    mocker.patch(
        "quackosm.osm_extracts.bbbike._iterate_bbbike_index",
        return_value=[
            OpenStreetMapExtract(
                id="bbbike_test",
                name="test",
                parent="bbbike",
                url="test_url",
                geometry=box(0, 0, 1, 1),
            )
        ],
    )
    mocker.patch(
        "quackosm.osm_extracts.extract._get_file_creation_date",
        return_value=datetime.datetime.now() - relativedelta(years=1, days=1),
    )
    mocker.patch("quackosm.osm_extracts.bbbike.BBBIKE_INDEX_GDF", new=None)

    with pytest.warns(OldOsmCacheWarning):
        display_available_extracts(source=extract_source)


def test_cache_clearing() -> None:
    """Test if cache clearing works."""
    extract_source = OsmExtractSource.bbbike
    global_path = _get_global_cache_file_path(extract_source)
    local_path = _get_local_cache_file_path(extract_source)

    move_global_path = global_path.exists()
    move_local_path = local_path.exists()

    if move_global_path:
        global_moved_path = global_path.with_name("bbbike_index_moved.parquet")
        global_path.rename(global_moved_path)

    if move_local_path:
        local_moved_path = local_path.with_name("bbbike_index_moved.parquet")
        local_path.rename(local_moved_path)

    clear_osm_index_cache(extract_source)

    assert not global_path.exists()
    assert not local_path.exists()

    if move_global_path:
        global_moved_path.rename(global_path)
        global_moved_path.unlink(missing_ok=True)

    if move_local_path:
        local_moved_path.rename(local_path)
        local_moved_path.unlink(missing_ok=True)


def test_index_download() -> None:
    """Test if downloading precalculated OSM index from Github works."""
    global_bbbike_cache_file_path = _get_global_cache_file_path(OsmExtractSource.bbbike)
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent.resolve()) as tmp_dir_name:
        tmp_file_path = Path(tmp_dir_name) / global_bbbike_cache_file_path.name
        _download_precalculated_index_from_github(tmp_file_path)

        clear_osm_index_cache(OsmExtractSource.bbbike)
        assert not global_bbbike_cache_file_path.exists()
        _load_bbbike_index(force_recalculation=False)

        assert tmp_file_path.read_bytes() == global_bbbike_cache_file_path.read_bytes(), (
            "Mismatch between downloaded and local index files."
        )


def test_ensure_valid_geometries() -> None:
    """Test if invalid index geometries are repaired and overlay ops no longer raise."""
    import geopandas as gpd

    # A self-intersecting "bowtie" polygon is topologically invalid.
    bowtie = from_wkt("POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))")
    index = gpd.GeoDataFrame(
        {"id": ["bowtie", "valid"]},
        geometry=[bowtie, box(2, 2, 3, 3)],
        crs="EPSG:4326",
    )
    assert not index.geometry.is_valid.all()

    fixed_index = _ensure_valid_geometries(index)

    assert fixed_index.geometry.is_valid.all()
    # Intersection would raise a GEOSException on the invalid input - must work now.
    fixed_index.geometry.intersection(box(0, 0, 3, 3))


def test_migrate_legacy_geojson_cache(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test if a legacy GeoJSON cache is converted to parquet and the legacy file removed."""
    import geopandas as gpd

    global_parquet_path = tmp_path / "bbbike_index.parquet"
    local_parquet_path = tmp_path / "local" / "bbbike_index.parquet"
    mocker.patch(
        "quackosm.osm_extracts.extract._get_global_cache_file_path",
        return_value=global_parquet_path,
    )
    mocker.patch(
        "quackosm.osm_extracts.extract._get_local_cache_file_path",
        return_value=local_parquet_path,
    )

    legacy_path = global_parquet_path.with_suffix(".geojson")
    legacy_gdf = gpd.GeoDataFrame(
        {"id": ["x"], "area": [1.0]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
    )
    legacy_gdf.to_file(legacy_path, driver="GeoJSON")

    _migrate_legacy_geojson_cache(OsmExtractSource.bbbike)

    assert global_parquet_path.exists()
    assert not legacy_path.exists()
    migrated_gdf = gpd.read_parquet(global_parquet_path)
    assert set(migrated_gdf.columns) == {"id", "area", "geometry"}
    assert migrated_gdf.geometry.iloc[0].equals(box(0, 0, 1, 1))


def test_clear_removes_legacy_geojson_cache(mocker: MockerFixture, tmp_path: Path) -> None:
    """Test if clearing the cache also removes a legacy GeoJSON file."""
    import geopandas as gpd

    global_parquet_path = tmp_path / "bbbike_index.parquet"
    local_parquet_path = tmp_path / "local" / "bbbike_index.parquet"
    mocker.patch(
        "quackosm.osm_extracts.extract._get_global_cache_file_path",
        return_value=global_parquet_path,
    )
    mocker.patch(
        "quackosm.osm_extracts.extract._get_local_cache_file_path",
        return_value=local_parquet_path,
    )

    legacy_path = global_parquet_path.with_suffix(".geojson")
    gdf = gpd.GeoDataFrame(
        {"id": ["x"], "area": [1.0]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
    )
    gdf.to_file(legacy_path, driver="GeoJSON")
    gdf.to_parquet(global_parquet_path, compression="zstd", compression_level=3)

    clear_osm_index_cache(OsmExtractSource.bbbike)

    assert not global_parquet_path.exists()
    assert not legacy_path.exists()
