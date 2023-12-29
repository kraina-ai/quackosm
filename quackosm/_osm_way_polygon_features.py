from collections.abc import Iterable
from typing import Any, NamedTuple, cast

from quackosm._typing import is_expected_type


class OsmWayPolygonConfig(NamedTuple):
    """OSM Way polygon features config object."""

    all: Iterable[str]
    allowlist: dict[str, Iterable[str]]
    denylist: dict[str, Iterable[str]]


def parse_dict_to_config_object(raw_config: dict[str, Any]) -> OsmWayPolygonConfig:
    all_tags = raw_config.get("all", [])
    allowlist_tags = raw_config.get("allowlist", {})
    denylist_tags = raw_config.get("denylist", {})
    if not is_expected_type(all_tags, Iterable[str]):
        raise ValueError(f"Wrong type of key: all ({type(all_tags)})")

    if not is_expected_type(allowlist_tags, dict[str, Iterable[str]]):
        raise ValueError(f"Wrong type of key: all ({type(allowlist_tags)})")

    if not is_expected_type(denylist_tags, dict[str, Iterable[str]]):
        raise ValueError(f"Wrong type of key: denylist ({type(denylist_tags)})")

    return OsmWayPolygonConfig(
        all=cast(Iterable[str], all_tags),
        allowlist=cast(dict[str, Iterable[str]], allowlist_tags),
        denylist=cast(dict[str, Iterable[str]], denylist_tags),
    )
