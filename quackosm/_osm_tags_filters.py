"""Module contains a dedicated type alias for OSM tags filter."""

from collections.abc import Iterable, Mapping
from typing import Union, cast, overload

from quackosm._typing import is_expected_type

OsmTagsFilter = Mapping[str, Union[list[str], str, bool]]

GroupedOsmTagsFilter = Mapping[str, OsmTagsFilter]


@overload
def merge_osm_tags_filter(osm_tags_filter: OsmTagsFilter) -> OsmTagsFilter: ...


@overload
def merge_osm_tags_filter(osm_tags_filter: GroupedOsmTagsFilter) -> OsmTagsFilter: ...


@overload
def merge_osm_tags_filter(osm_tags_filter: Iterable[OsmTagsFilter]) -> OsmTagsFilter: ...


@overload
def merge_osm_tags_filter(osm_tags_filter: Iterable[GroupedOsmTagsFilter]) -> OsmTagsFilter: ...


def merge_osm_tags_filter(
    osm_tags_filter: Union[
        OsmTagsFilter, GroupedOsmTagsFilter, Iterable[OsmTagsFilter], Iterable[GroupedOsmTagsFilter]
    ],
) -> OsmTagsFilter:
    """
    Merge OSM tags filter into `OsmTagsFilter` type.

    Optionally merges `GroupedOsmTagsFilter` into `OsmTagsFilter` to allow loaders to load all
    defined groups during single operation.

    Args:
        osm_tags_filter: OSM tags filter definition.

    Raises:
        AttributeError: When provided tags don't match both
            `OsmTagsFilter` or `GroupedOsmTagsFilter`.

    Returns:
        OsmTagsFilter: Merged filters.
    """
    if is_expected_type(osm_tags_filter, OsmTagsFilter):
        return cast("OsmTagsFilter", osm_tags_filter)
    elif is_expected_type(osm_tags_filter, GroupedOsmTagsFilter):
        return _merge_grouped_osm_tags_filter(cast("GroupedOsmTagsFilter", osm_tags_filter))
    elif is_expected_type(osm_tags_filter, Iterable):
        return _merge_multiple_osm_tags_filters(
            [
                merge_osm_tags_filter(
                    cast("Union[OsmTagsFilter, GroupedOsmTagsFilter]", sub_osm_tags_filter)
                )
                for sub_osm_tags_filter in osm_tags_filter
            ]
        )

    raise AttributeError(
        "Provided tags don't match required type definitions"
        " (OsmTagsFilter or GroupedOsmTagsFilter)."
    )


def merge_key_value_pairs_to_osm_tags_filter(
    key_value_pairs: list[tuple[str, Union[list[str], str, bool]]],
) -> OsmTagsFilter:
    merged_osm_tag_filter = _merge_multiple_osm_tags_filters(
        {osm_tag_filter_key: osm_tag_filter_value}
        for osm_tag_filter_key, osm_tag_filter_value in key_value_pairs
    )
    return merged_osm_tag_filter


def check_if_any_osm_tags_filter_value_is_positive(
    osm_tags_filter: Union[GroupedOsmTagsFilter, OsmTagsFilter],
) -> bool:
    if is_expected_type(osm_tags_filter, OsmTagsFilter):
        return any(
            osm_tag_filter_value != False  # noqa: E712
            for osm_tag_filter_value in cast("OsmTagsFilter", osm_tags_filter).values()
        )
    elif is_expected_type(osm_tags_filter, GroupedOsmTagsFilter):
        return any(
            check_if_any_osm_tags_filter_value_is_positive(osm_tags_filter_group)
            for osm_tags_filter_group in cast("GroupedOsmTagsFilter", osm_tags_filter).values()
        )

    raise AttributeError(
        "Provided tags don't match required type definitions"
        " (OsmTagsFilter or GroupedOsmTagsFilter)."
    )


def _merge_grouped_osm_tags_filter(grouped_filter: GroupedOsmTagsFilter) -> OsmTagsFilter:
    """
    Merge grouped osm tags filter into a base one.

    Function merges all filter categories into a single one for an OSM loader to use.

    Args:
        grouped_filter (GroupedOsmTagsFilter): Grouped filter to be merged into a single one.

    Returns:
        osm_tags_type: Merged filter.
    """
    if not is_expected_type(grouped_filter, GroupedOsmTagsFilter):
        raise ValueError(
            "Provided filter doesn't match required `GroupedOsmTagsFilter` definition."
        )

    return _merge_multiple_osm_tags_filters(grouped_filter.values())


def _merge_multiple_osm_tags_filters(osm_tags_filters: Iterable[OsmTagsFilter]) -> OsmTagsFilter:
    """
    Merge multiple osm tags filters into a single one.

    Function merges all OsmTagsFilters into a single one for an OSM loader to use.

    Args:
        osm_tags_filters (Iterable[OsmTagsFilter]): List of filters to be merged into a single one.

    Returns:
        osm_tags_type: Merged filter.
    """
    if not is_expected_type(osm_tags_filters, Iterable[OsmTagsFilter]):
        raise ValueError(
            "Provided filter doesn't match required `Iterable[OsmTagsFilter]` definition."
        )

    result: dict[str, Union[list[str], str, bool]] = {}
    for osm_tags_filter in osm_tags_filters:
        for osm_tag_key, osm_tag_value in osm_tags_filter.items():
            if osm_tag_key not in result:
                result[osm_tag_key] = []

            is_current_value_positive = (
                isinstance(result[osm_tag_key], (list, bool)) and result[osm_tag_key]
            )
            is_current_value_negative = result[osm_tag_key] == False  # noqa: E712

            # If current value is negative and filter already have positive value
            # If current value is positive and filter already have negative value
            if (is_current_value_positive and osm_tag_value == False) or (  # noqa: E712
                is_current_value_negative and osm_tag_value != False  # noqa: E712
            ):
                raise ValueError(
                    "Provided OSM tags filter values cannot be merged.\n"
                    f"There is a conflict between the following values with '{osm_tag_key}' key:"
                    f" {result[osm_tag_key]} and {osm_tag_value}."
                )

            # If filter is already a positive boolean, skip
            if isinstance(result[osm_tag_key], bool) and result[osm_tag_key]:
                continue

            current_values_list = cast("list[str]", result[osm_tag_key])

            # Check bool
            if osm_tag_value == True:  # noqa: E712
                result[osm_tag_key] = True
            elif osm_tag_value == False:  # noqa: E712
                result[osm_tag_key] = False
            # Check string
            elif isinstance(osm_tag_value, str) and osm_tag_value not in current_values_list:
                current_values_list.append(osm_tag_value)
            # Check list
            elif isinstance(osm_tag_value, list):
                new_values = [value for value in osm_tag_value if value not in current_values_list]
                current_values_list.extend(new_values)

    return cast("OsmTagsFilter", result)
