"""Tests for PbfFileReader OSM tags filtering."""

from collections.abc import Iterable
from pathlib import Path
from typing import Union
from unittest import TestCase

import pytest
from srai.loaders.osm_loaders.filters import GEOFABRIK_LAYERS, HEX2VEC_FILTER

from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter, merge_osm_tags_filter
from quackosm.pbf_file_reader import PbfFileReader

ut = TestCase()


@pytest.mark.parametrize(  # type: ignore
    "osm_tags_filter,expected_result_filter",
    [
        ({"tag_a": True}, {"tag_a": True}),
        ({"tag_a": "A"}, {"tag_a": "A"}),
        ({"tag_a": ["A"]}, {"tag_a": ["A"]}),
        ({}, {}),
        ({"group_a": {}}, {}),
        ({"group_a": {"tag_a": True}}, {"tag_a": True}),
        ({"group_a": {"tag_a": "A"}}, {"tag_a": ["A"]}),
        ({"group_a": {"tag_a": ["A"]}}, {"tag_a": ["A"]}),
        ({"group_a": {"tag_a": "A", "tag_b": "B"}}, {"tag_a": ["A"], "tag_b": ["B"]}),
        ({"group_a": {"tag_a": ["A"], "tag_b": ["B"]}}, {"tag_a": ["A"], "tag_b": ["B"]}),
        (
            {
                "group_a": {"tag_a": "A", "tag_b": "B"},
                "group_b": {"tag_a": "A", "tag_b": "B"},
            },
            {"tag_a": ["A"], "tag_b": ["B"]},
        ),
        (
            {
                "group_a": {"tag_a": "A", "tag_b": "B"},
                "group_b": {"tag_c": "C", "tag_d": "D"},
            },
            {"tag_a": ["A"], "tag_b": ["B"], "tag_c": ["C"], "tag_d": ["D"]},
        ),
        (
            {
                "group_a": {"tag_a": "A", "tag_b": "B"},
                "group_b": {"tag_a": "C", "tag_b": "D"},
            },
            {"tag_a": ["A", "C"], "tag_b": ["B", "D"]},
        ),
        (
            {
                "group_a": {"tag_a": "A", "tag_b": "B"},
                "group_b": {"tag_a": ["C", "D"], "tag_b": "E"},
            },
            {"tag_a": ["A", "C", "D"], "tag_b": ["B", "E"]},
        ),
        (
            {
                "group_a": {"tag_a": "A", "tag_b": "B"},
                "group_b": {"tag_a": ["C", "D"], "tag_b": True},
            },
            {"tag_a": ["A", "C", "D"], "tag_b": True},
        ),
        (
            {
                "group_a": {"tag_a": ["A", "C"], "tag_b": ["B", "E"]},
                "group_b": {"tag_a": ["C", "D"], "tag_b": ["B"]},
            },
            {"tag_a": ["A", "C", "D"], "tag_b": ["B", "E"]},
        ),
        ([{"tag_a": True}], {"tag_a": True}),
        ([{"tag_a": "A"}], {"tag_a": ["A"]}),
        ([{"tag_a": ["A"]}], {"tag_a": ["A"]}),
        ([{}], {}),
        ([{"group_a": {}}], {}),
        (
            [{"tag_a": "A", "tag_b": "B"}, {"tag_a": "A", "tag_b": "B"}],
            {"tag_a": ["A"], "tag_b": ["B"]},
        ),
        (
            [
                {
                    "group_a": {"tag_a": "A", "tag_b": "B"},
                    "group_b": {"tag_a": "A", "tag_b": "B"},
                },
                {"tag_a": "A", "tag_b": "B"},
            ],
            {"tag_a": ["A"], "tag_b": ["B"]},
        ),
        (
            [
                {
                    "group_a": {"tag_a": "A", "tag_b": "B"},
                    "group_b": {"tag_a": "A", "tag_b": "B"},
                },
                {
                    "group_a": {"tag_a": "A", "tag_b": "B"},
                    "group_b": {"tag_a": "A", "tag_b": "B"},
                },
            ],
            {"tag_a": ["A"], "tag_b": ["B"]},
        ),
        ([{}, {}], {}),
        ([{"group_a": {}}, {"group_a": {}}], {}),
        ([{"group_a": {}}, {}], {}),
        (
            [{"tag_a": "A", "tag_b": "B"}, {"tag_c": "C", "tag_d": "D"}],
            {"tag_a": ["A"], "tag_b": ["B"], "tag_c": ["C"], "tag_d": ["D"]},
        ),
    ],
)
def test_merging_correct_osm_tags_filters(
    osm_tags_filter: Union[
        GroupedOsmTagsFilter, OsmTagsFilter, Iterable[GroupedOsmTagsFilter], Iterable[OsmTagsFilter]
    ],
    expected_result_filter: OsmTagsFilter,
) -> None:
    """Test merging grouped tags filter into a base osm filter."""
    merged_filters = merge_osm_tags_filter(osm_tags_filter)
    ut.assertDictEqual(expected_result_filter, merged_filters)


@pytest.mark.parametrize(  # type: ignore
    "osm_tags_filter",
    [
        {
            "group_a": {"tag_a": True},
            "group_b": {"tag_a": False},
        },
        {
            "group_a": {"tag_a": ["A"], "tag_b": ["B"]},
            "group_b": {"tag_a": False, "tag_c": ["C"]},
        },
        {
            "group_a": {"tag_a": ["A"], "tag_b": ["B"]},
            "group_b": {"tag_a": ["C", "D"], "tag_b": False},
        },
        [{"tag_a": True}, {"tag_a": False}],
        [{"tag_a": False}, {"tag_a": True}],
        [{"tag_a": "A"}, {"tag_a": False}],
        [{"tag_a": ["A"]}, {"tag_a": False}],
    ],
)
def test_merging_incorrect_osm_tags_filters(
    osm_tags_filter: Union[
        GroupedOsmTagsFilter, OsmTagsFilter, Iterable[GroupedOsmTagsFilter], Iterable[OsmTagsFilter]
    ],
) -> None:
    """Test merging grouped tags filter into a base osm filter."""
    with pytest.raises(ValueError):
        merge_osm_tags_filter(osm_tags_filter)


@pytest.mark.parametrize(  # type: ignore
    "test_file_name,query,expected_result_length,expected_features_columns_length",
    [
        (
            "d17f922ed15e9609013a6b895e1e7af2d49158f03586f2c675d17b760af3452e.osm.pbf",
            None,
            678,
            271,
        ),
        (
            "eb2848d259345ce7dfe8af34fd1ab24503bb0b952e04e872c87c55550fa50fbf.osm.pbf",
            None,
            1,
            22,
        ),
        ("529cdcbb7a3cc103658ef31b39bed24984e421127d319c867edf2f86ff3bb098.osm.pbf", None, 0, 0),
        (
            "d17f922ed15e9609013a6b895e1e7af2d49158f03586f2c675d17b760af3452e.osm.pbf",
            HEX2VEC_FILTER,
            97,
            10,
        ),
        (
            "eb2848d259345ce7dfe8af34fd1ab24503bb0b952e04e872c87c55550fa50fbf.osm.pbf",
            HEX2VEC_FILTER,
            0,
            0,
        ),
        (
            "d17f922ed15e9609013a6b895e1e7af2d49158f03586f2c675d17b760af3452e.osm.pbf",
            GEOFABRIK_LAYERS,
            433,
            22,
        ),
        (
            "eb2848d259345ce7dfe8af34fd1ab24503bb0b952e04e872c87c55550fa50fbf.osm.pbf",
            GEOFABRIK_LAYERS,
            0,
            0,
        ),
    ],
)
def test_pbf_reader(
    test_file_name: str,
    query: OsmTagsFilter,
    expected_result_length: int,
    expected_features_columns_length: int,
):
    """Test proper files loading in `PbfFileReader`."""
    features_gdf = PbfFileReader(tags_filter=query).convert_pbf_to_geodataframe(
        pbf_path=[Path(__file__).parent.parent / "test_files" / test_file_name],
        explode_tags=True,
        ignore_cache=True,
    )
    assert (
        len(features_gdf) == expected_result_length
    ), f"Mismatched result length ({len(features_gdf)}, {expected_result_length})"
    assert len(features_gdf.columns) == expected_features_columns_length + 1, (
        f"Mismatched columns length ({len(features_gdf.columns)},"
        f" {expected_features_columns_length + 1})"
    )


@pytest.mark.parametrize(  # type: ignore
    "filter_osm_id,osm_tags_filter,keep_all_tags,expected_tags_keys",
    [
        ("way/389888402", {"building": "apartments"}, False, ["building"]),
        (
            "way/389888402",
            {"building": "apartments"},
            True,
            [
                "addr:city",
                "addr:country",
                "addr:housenumber",
                "addr:postcode",
                "addr:street",
                "building",
                "building:levels",
            ],
        ),
        ("way/627022271", {"leisure": "garden"}, False, ["leisure"]),
        (
            "way/627022271",
            {"leisure": "garden"},
            True,
            [
                "addr:country",
                "leisure",
                "name",
            ],
        ),
    ],
)
def test_pbf_reader_proper_tags_reading(
    filter_osm_id: str,
    osm_tags_filter: Union[OsmTagsFilter, GroupedOsmTagsFilter],
    keep_all_tags: bool,
    expected_tags_keys: list[str],
):
    """Test proper tags reading with filtering on osm_id in `PbfFileReader`."""
    file_name = "monaco.osm.pbf"
    features_gdf = PbfFileReader(tags_filter=osm_tags_filter).convert_pbf_to_geodataframe(
        pbf_path=[Path(__file__).parent.parent / "test_files" / file_name],
        ignore_cache=True,
        filter_osm_ids=[filter_osm_id],
        explode_tags=False,
        keep_all_tags=keep_all_tags,
    )
    assert len(features_gdf) == 1
    returned_tags_keys = list(features_gdf.iloc[0].tags.keys())
    ut.assertListEqual(returned_tags_keys, expected_tags_keys)


@pytest.mark.parametrize(  # type: ignore
    "osm_tags_filter,expected_result_length,expected_top_10_ids,expected_no_columns,expected_top_10_columns",
    [
        (
            {"building": True},
            1283,
            [
                "relation/11384697",
                "relation/11484092",
                "relation/11484093",
                "relation/11484094",
                "relation/11485520",
                "relation/11538023",
                "relation/11546879",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
            ],
            2,
            ["building", "geometry"],
        ),
        (
            {"amenity": True, "leisure": True},
            1038,
            [
                "node/10020887517",
                "node/10021298117",
                "node/10021298717",
                "node/10025656390",
                "node/10025843517",
                "node/10025852089",
                "node/10025852090",
                "node/10028051243",
                "node/10068880335",
                "node/10127713363",
            ],
            3,
            ["amenity", "geometry", "leisure"],
        ),
        (
            {"amenity": "parking", "leisure": ["park", "garden"], "office": True},
            157,
            [
                "node/10025656391",
                "node/10025656392",
                "node/10039418191",
                "node/1079045434",
                "node/1079750865",
                "node/11094273773",
                "node/11151020213",
                "node/11252553796",
                "node/1661139827",
                "node/1662764987",
            ],
            4,
            ["amenity", "geometry", "leisure", "office"],
        ),
        (
            {"office": False},
            7896,
            [
                "node/10005045289",
                "node/10020887517",
                "node/10021298117",
                "node/10021298717",
                "node/10025656383",
                "node/10025656390",
                "node/10025656393",
                "node/10025656394",
                "node/10025656395",
                "node/10025843517",
            ],
            747,
            [
                "ISO3166-1:alpha2",
                "ISO3166-2",
                "abandoned:railway",
                "access",
                "access:backward",
                "access:covid19",
                "access:lanes",
                "addr:city",
                "addr:country",
                "addr:district",
            ],
        ),
        (
            {"building": True, "office": False},
            1276,
            [
                "relation/11384697",
                "relation/11484092",
                "relation/11484093",
                "relation/11484094",
                "relation/11485520",
                "relation/11538023",
                "relation/11546879",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
            ],
            2,
            ["building", "geometry"],
        ),
        (
            {"name:en": True},
            54,
            [
                "node/10039418191",
                "node/10671507005",
                "node/10674256605",
                "node/1661205490",
                "node/1790048269",
                "node/2838235236",
                "node/2838250945",
                "node/4187925032",
                "node/4416171690",
                "node/4416197079",
            ],
            2,
            ["geometry", "name:en"],
        ),
        (
            {"name:e*": True},
            57,
            [
                "node/10039418191",
                "node/10671507005",
                "node/10674256605",
                "node/1661205490",
                "node/1790048269",
                "node/25258130",
                "node/2838235236",
                "node/2838250945",
                "node/4187925032",
                "node/4416171690",
            ],
            9,
            [
                "geometry",
                "name:ee",
                "name:el",
                "name:en",
                "name:eo",
                "name:es",
                "name:et",
                "name:eu",
                "name:ext",
            ],
        ),
        (
            {"name:*": False},
            7806,
            [
                "node/10005045289",
                "node/10020887517",
                "node/10021298117",
                "node/10021298717",
                "node/10025656383",
                "node/10025656390",
                "node/10025656391",
                "node/10025656392",
                "node/10025656393",
                "node/10025656394",
            ],
            477,
            [
                "ISO3166-2",
                "abandoned:railway",
                "access",
                "access:backward",
                "access:covid19",
                "access:lanes",
                "addr:city",
                "addr:country",
                "addr:district",
                "addr:full",
            ],
        ),
        (
            {"*": True},
            7937,
            [
                "node/10005045289",
                "node/10020887517",
                "node/10021298117",
                "node/10021298717",
                "node/10025656383",
                "node/10025656390",
                "node/10025656391",
                "node/10025656392",
                "node/10025656393",
                "node/10025656394",
            ],
            758,
            [
                "ISO3166-1:alpha2",
                "ISO3166-2",
                "abandoned:railway",
                "access",
                "access:backward",
                "access:covid19",
                "access:lanes",
                "addr:city",
                "addr:country",
                "addr:district",
            ],
        ),
        ({"*": False}, 0, [], 1, ["geometry"]),
        (
            {"building": True, "addr:*": False},
            286,
            [
                "relation/11484092",
                "relation/11485520",
                "relation/11538023",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
                "relation/1484217",
                "relation/16248281",
                "relation/16248282",
                "relation/16248283",
            ],
            2,
            ["building", "geometry"],
        ),
        (
            {"building": True, "addr:*": True},
            1935,
            [
                "node/10025843517",
                "node/10025852088",
                "node/10025852090",
                "node/10028051243",
                "node/10039418191",
                "node/10060634234",
                "node/10060634235",
                "node/10060634237",
                "node/10060634238",
                "node/10060634239",
            ],
            10,
            [
                "addr:city",
                "addr:country",
                "addr:district",
                "addr:full",
                "addr:housename",
                "addr:housenumber",
                "addr:postcode",
                "addr:street",
                "building",
                "geometry",
            ],
        ),
        (
            {"building": True, "addr:*": True, "source:*": False},
            1933,
            [
                "node/10025843517",
                "node/10025852088",
                "node/10025852090",
                "node/10028051243",
                "node/10039418191",
                "node/10060634234",
                "node/10060634235",
                "node/10060634237",
                "node/10060634238",
                "node/10060634239",
            ],
            10,
            [
                "addr:city",
                "addr:country",
                "addr:district",
                "addr:full",
                "addr:housename",
                "addr:housenumber",
                "addr:postcode",
                "addr:street",
                "building",
                "geometry",
            ],
        ),
        (
            {"name:*": "Monaco"},
            2,
            ["node/1790048269", "node/6684051501"],
            46,
            [
                "geometry",
                "name:af",
                "name:als",
                "name:ang",
                "name:bar",
                "name:bi",
                "name:cy",
                "name:da",
                "name:de",
                "name:dsb",
            ],
        ),
        (
            {"name:*": ["Monaco", "France"]},
            2,
            ["node/1790048269", "node/6684051501"],
            46,
            [
                "geometry",
                "name:af",
                "name:als",
                "name:ang",
                "name:bar",
                "name:bi",
                "name:cy",
                "name:da",
                "name:de",
                "name:dsb",
            ],
        ),
        (
            {"highway": "primary", "maxspeed": False},
            63,
            [
                "way/1019174499",
                "way/1019360049",
                "way/1019360050",
                "way/1024393367",
                "way/1024393368",
                "way/1131276186",
                "way/1131276187",
                "way/1174006399",
                "way/1174006400",
                "way/1174006401",
            ],
            2,
            ["geometry", "highway"],
        ),
        (
            {"highway": "*ary"},
            400,
            [
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174499",
                "way/1019174508",
                "way/1019174509",
                "way/1019360049",
                "way/1019360050",
                "way/1019646822",
                "way/1024393367",
            ],
            2,
            ["geometry", "highway"],
        ),
        (
            {"highway": "*ary", "maxspeed": False},
            198,
            [
                "way/1019174499",
                "way/1019360049",
                "way/1019360050",
                "way/1024393367",
                "way/1024393368",
                "way/1058067231",
                "way/1058067232",
                "way/1099877047",
                "way/1099877048",
                "way/1131276186",
            ],
            2,
            ["geometry", "highway"],
        ),
        (
            {"*speed": "*0"},
            293,
            [
                "node/1079045434",
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174508",
                "way/1019174509",
                "way/1019646822",
                "way/1024523794",
                "way/1024523795",
                "way/1024523796",
            ],
            2,
            ["geometry", "maxspeed"],
        ),
        (
            {"*speed": "*0", "railway": False, "waterway": False},
            264,
            [
                "node/1079045434",
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174508",
                "way/1019174509",
                "way/1019646822",
                "way/1024523794",
                "way/1024523795",
                "way/1024523796",
            ],
            2,
            ["geometry", "maxspeed"],
        ),
        (
            {"*speed": ["*0", "90"]},
            293,
            [
                "node/1079045434",
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174508",
                "way/1019174509",
                "way/1019646822",
                "way/1024523794",
                "way/1024523795",
                "way/1024523796",
            ],
            2,
            ["geometry", "maxspeed"],
        ),
        (
            {"*speed": "*0", "highway": "primary"},
            356,
            [
                "node/1079045434",
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174499",
                "way/1019174508",
                "way/1019174509",
                "way/1019360049",
                "way/1019360050",
                "way/1019646822",
            ],
            3,
            ["geometry", "highway", "maxspeed"],
        ),
        (
            {"*speed": "*0", "highspeed": True},
            293,
            [
                "node/1079045434",
                "way/1001561436",
                "way/1001561437",
                "way/1001561438",
                "way/1019174508",
                "way/1019174509",
                "way/1019646822",
                "way/1024523794",
                "way/1024523795",
                "way/1024523796",
            ],
            2,
            ["geometry", "maxspeed"],
        ),
        (
            {"buildings": {"building": True}},
            1283,
            [
                "relation/11384697",
                "relation/11484092",
                "relation/11484093",
                "relation/11484094",
                "relation/11485520",
                "relation/11538023",
                "relation/11546879",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
            ],
            2,
            ["buildings", "geometry"],
        ),
        (
            {"buildings_all": {"building": True}, "buildings_star_all": {"building": "*"}},
            1283,
            [
                "relation/11384697",
                "relation/11484092",
                "relation/11484093",
                "relation/11484094",
                "relation/11485520",
                "relation/11538023",
                "relation/11546879",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
            ],
            3,
            ["buildings_all", "buildings_star_all", "geometry"],
        ),
        (
            {"buildings_all": {"building": True}, "buildings_office": {"building": "office"}},
            1283,
            [
                "relation/11384697",
                "relation/11484092",
                "relation/11484093",
                "relation/11484094",
                "relation/11485520",
                "relation/11538023",
                "relation/11546879",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
            ],
            3,
            ["buildings_all", "buildings_office", "geometry"],
        ),
        (
            {"english_name": {"name:en": True}, "all_names": {"name:*": True}},
            131,
            [
                "node/10039418191",
                "node/10671507005",
                "node/10674256605",
                "node/11339077004",
                "node/1661205490",
                "node/1704462398",
                "node/1780610146",
                "node/1784106772",
                "node/1790048269",
                "node/2088741979",
            ],
            3,
            ["all_names", "english_name", "geometry"],
        ),
        (
            {"english_name": {"name:en": True}, "e_starting_names": {"name:e*": True}},
            57,
            [
                "node/10039418191",
                "node/10671507005",
                "node/10674256605",
                "node/1661205490",
                "node/1790048269",
                "node/25258130",
                "node/2838235236",
                "node/2838250945",
                "node/4187925032",
                "node/4416171690",
            ],
            3,
            ["e_starting_names", "english_name", "geometry"],
        ),
        (
            {
                "buildings_and_features_with_addr": {"building": True, "addr:*": True},
                "buildings_and_features_with_names": {"building": True, "name:*": True},
            },
            2020,
            [
                "node/10025843517",
                "node/10025852088",
                "node/10025852090",
                "node/10028051243",
                "node/10039418191",
                "node/10060634234",
                "node/10060634235",
                "node/10060634237",
                "node/10060634238",
                "node/10060634239",
            ],
            3,
            ["buildings_and_features_with_addr", "buildings_and_features_with_names", "geometry"],
        ),
        (
            {
                "buildings_without_addr": {"building": True, "addr:*": False},
                "buildings_without_names": {"building": True, "name:*": False},
            },
            282,
            [
                "relation/11484092",
                "relation/11485520",
                "relation/11538023",
                "relation/1369192",
                "relation/1369193",
                "relation/1369195",
                "relation/1484217",
                "relation/16248281",
                "relation/16248282",
                "relation/16248283",
            ],
            3,
            ["buildings_without_addr", "buildings_without_names", "geometry"],
        ),
        (
            {
                "highways_and_ways_with_decimal_speeds": {"highway": True, "*speed": "*0"},
                "railways": {"railway": True},
            },
            3657,
            [
                "node/10068880332",
                "node/10688011472",
                "node/1074584632",
                "node/1074584641",
                "node/1074584644",
                "node/1074584650",
                "node/1074584658",
                "node/1074584679",
                "node/1074584680",
                "node/1074584737",
            ],
            3,
            ["geometry", "highways_and_ways_with_decimal_speeds", "railways"],
        ),
    ],
)
def test_correct_osm_tags_filters(
    osm_tags_filter: Union[OsmTagsFilter, GroupedOsmTagsFilter],
    expected_result_length: int,
    expected_top_10_ids: list[str],
    expected_no_columns: int,
    expected_top_10_columns: list[str],
):
    """Test proper tags reading with filtering in `PbfFileReader`."""
    file_name = "monaco.osm.pbf"
    features_gdf = PbfFileReader(tags_filter=osm_tags_filter).convert_pbf_to_geodataframe(
        pbf_path=[Path(__file__).parent.parent / "test_files" / file_name],
        ignore_cache=True,
        explode_tags=True,
    )
    assert (
        len(features_gdf) == expected_result_length
    ), f"Mismatched result length ({len(features_gdf)}, {expected_result_length})"
    ut.assertListEqual(list(features_gdf.sort_index().head(10).index), expected_top_10_ids)

    assert (
        len(features_gdf.columns) == expected_no_columns
    ), f"Mismatched columns length ({len(features_gdf.columns)}, {expected_no_columns})"
    ut.assertListEqual(list(features_gdf.columns.sort_values()[:10]), expected_top_10_columns)


@pytest.mark.parametrize(  # type: ignore
    "osm_tags_filter",
    [
        {"name:*": True, "name:en": False},
        {"name:*": ["Monaco", "France"], "name:en": False},
        {"*speed": "*0", "maxspeed": False, "highway": "primary"},
        ({"buildings": {"building": True}, "non-buildings": {"building": False}}),
        ({"buildings_office": {"building": ["office"]}, "non-buildings": {"building": False}}),
        ({"buildings_yes": {"building": "yes"}, "non-buildings": {"building": False}}),
        ({"buildings_all": {"building": "*"}, "non-buildings": {"building": False}}),
    ],
)
def test_incorrect_osm_tags_filters(
    osm_tags_filter: Union[OsmTagsFilter, GroupedOsmTagsFilter],
) -> None:
    """Test wrong tags reading with filtering in `PbfFileReader`."""
    with pytest.raises(ValueError):
        file_name = "monaco.osm.pbf"
        PbfFileReader(tags_filter=osm_tags_filter).convert_pbf_to_geodataframe(
            pbf_path=[Path(__file__).parent.parent / "test_files" / file_name],
            ignore_cache=True,
            explode_tags=False,
        )
