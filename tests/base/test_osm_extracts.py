"""Tests related to OSM extracts."""

from unittest import TestCase

import pytest
from parametrization import Parametrization as P
from shapely import from_wkt
from shapely.geometry.base import BaseGeometry

from quackosm.osm_extracts import OsmExtractSource, find_smallest_containing_extract

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
    "osm_fr_europe_vatican_city",
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
    "osm_fr_north-america_canada_british_columbia",
)  # type: ignore
def test_single_smallest_extract(source: str, geometry: BaseGeometry, expected_extract_id: str):
    """Test if extracts matching works correctly for geometries within borders."""
    extracts = find_smallest_containing_extract(geometry, source)
    assert len(extracts) == 1
    assert extracts[0].id == expected_extract_id


@P.parameters("source", "geometry", "expected_extract_ids")  # type: ignore
@P.case(
    "Andorra - any",
    "any",
    from_wkt(
        "POLYGON ((1.382599544073372 42.67676873293743, 1.382599544073372 42.40065303248514,"
        " 1.8092269635579328 42.40065303248514, 1.8092269635579328 42.67676873293743,"
        " 1.382599544073372 42.67676873293743))"
    ),
    [
        "osm_fr_europe_spain_catalunya_lleida",
        "osm_fr_europe_spain_catalunya_girona",
        "osm_fr_europe_france_midi_pyrenees_ariege",
        "osm_fr_europe_france_languedoc_roussillon_pyrenees_orientales",
        "Geofabrik_andorra",
    ],
)  # type: ignore
def test_multiple_smallest_extracts(
    source: str, geometry: BaseGeometry, expected_extract_ids: list[str]
):
    """Test if extracts matching works correctly for geometries between borders."""
    extracts = find_smallest_containing_extract(geometry, source)
    assert len(extracts) == len(expected_extract_ids)
    ut.assertListEqual([extract.id for extract in extracts], expected_extract_ids)
