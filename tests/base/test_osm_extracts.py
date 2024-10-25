"""Tests related to OSM extracts."""

from contextlib import nullcontext as does_not_raise
from typing import Any
from unittest import TestCase

import pandas as pd
import pytest
from parametrization import Parametrization as P
from pytest_mock import MockerFixture
from rich.console import Console
from shapely import from_wkt
from shapely.geometry.base import BaseGeometry

from quackosm._exceptions import (
    GeometryNotCoveredError,
    GeometryNotCoveredWarning,
    OsmExtractIndexOutdatedWarning,
    OsmExtractMultipleMatchesError,
    OsmExtractZeroMatchesError,
)
from quackosm.geocode import geocode_to_geometry
from quackosm.osm_extracts import (
    OsmExtractSource,
    display_available_extracts,
    find_smallest_containing_extracts,
    find_smallest_containing_extracts_total,
    get_extract_by_query,
)
from quackosm.osm_extracts.extract import _get_full_file_name_function, _get_global_cache_file_path
from quackosm.osm_extracts.geofabrik import _load_geofabrik_index

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
def test_proper_osm_extract_source(value: str):
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
    "osmfr_europe_vatican_city",
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
def test_single_smallest_extract(source: str, geometry: BaseGeometry, expected_extract_id: str):
    """Test if extracts matching works correctly for geometries within borders."""
    extracts = find_smallest_containing_extracts(geometry, source)
    assert len(extracts) == 1
    assert extracts[0].id == expected_extract_id, f"{extracts[0].id} vs {expected_extract_id}"


@P.parameters(
    "source", "geometry", "geometry_coverage_iou_threshold", "expected_extract_file_names"
)  # type: ignore
@P.case(
    "Andorra bbox, any, iou default",
    "any",
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
        "geofabrik_europe_andorra",
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
        "osmfr_europe_spain_catalunya_lleida",
        "osmfr_europe_spain_catalunya_girona",
        "osmfr_europe_france_midi_pyrenees_ariege",
        "osmfr_europe_france_languedoc_roussillon_pyrenees_orientales",
        "geofabrik_europe_andorra",
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
    ["osmfr_europe"],
)  # type: ignore
def test_multiple_smallest_extracts(
    source: str,
    geometry: BaseGeometry,
    geometry_coverage_iou_threshold: float,
    expected_extract_file_names: list[str],
):
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
    expectation, allow_uncovered_geometry: bool, geometry_coverage_iou_threshold: float
):
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


def test_proper_cache_saving() -> None:
    """Test if file is saved in cache properly."""
    save_path = _get_global_cache_file_path(OsmExtractSource.geofabrik)
    loaded_index = _load_geofabrik_index()
    assert save_path.exists()
    assert len(loaded_index.columns) == 7


def test_wrong_cached_index() -> None:
    """Test if cached file with missing columns is redownloaded again."""
    save_path = _get_global_cache_file_path(OsmExtractSource.geofabrik)
    column_to_remove = "id"

    # load index first time
    first_index = _load_geofabrik_index()

    # remove the column and replace the file
    first_index.drop(columns=[column_to_remove]).to_file(save_path, driver="GeoJSON")

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
    ["geofabrik_asia", "osmfr_asia"],
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
    capfd, mocker: MockerFixture, osm_source: OsmExtractSource, use_full_names: bool
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
