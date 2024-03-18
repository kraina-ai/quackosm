"""Tests for PbfFileReader OSM tags filtering."""

from pathlib import Path
from typing import Union
from unittest import TestCase

import pytest
from srai.loaders.osm_loaders.filters import GEOFABRIK_LAYERS, HEX2VEC_FILTER

from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.pbf_file_reader import PbfFileReader

ut = TestCase()


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
    features_gdf = PbfFileReader(tags_filter=query).get_features_gdf(
        file_paths=[Path(__file__).parent.parent / "test_files" / test_file_name],
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
    "osm_tags_filter,keep_all_tags,expected_result_length,expected_tags_keys",
    [
        ({"building": "apartments"}, False, ["building"]),
        (
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
        ({"leisure": "garden"}, False, ["leisure"]),
        (
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
def test_osm_tags_filters(
    osm_tags_filter: Union[OsmTagsFilter, GroupedOsmTagsFilter],
    keep_all_tags: bool,
    expected_result_length: int,
    expected_tags_keys: list[str],
):
    """Test proper tags reading with filtering in `PbfFileReader`."""
    file_name = "monaco.osm.pbf"
    features_gdf = PbfFileReader(tags_filter=osm_tags_filter).get_features_gdf(
        file_paths=[Path(__file__).parent.parent / "test_files" / file_name],
        ignore_cache=True,
        explode_tags=False,
        keep_all_tags=keep_all_tags,
    )
    assert (
        len(features_gdf) == expected_result_length
    ), f"Mismatched result length ({len(features_gdf)}, {expected_result_length})"
    returned_tags_keys = list(features_gdf.iloc[0].tags.keys())
    ut.assertListEqual(returned_tags_keys, expected_tags_keys)


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
    features_gdf = PbfFileReader(tags_filter=osm_tags_filter).get_features_gdf(
        file_paths=[Path(__file__).parent.parent / "test_files" / file_name],
        ignore_cache=True,
        filter_osm_ids=[filter_osm_id],
        explode_tags=False,
        keep_all_tags=keep_all_tags,
    )
    assert len(features_gdf) == 1
    returned_tags_keys = list(features_gdf.iloc[0].tags.keys())
    ut.assertListEqual(returned_tags_keys, expected_tags_keys)
