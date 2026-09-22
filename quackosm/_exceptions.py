"""
Exceptions and warnings for QuackOSM.

The OSM-extracts-related exception and warning classes are re-exported from
``osmfinder.exceptions`` so that they are the exact same classes raised by the
underlying library.  This keeps backward compatibility for users who catch
``quackosm._exceptions.<SomeError>`` while ensuring that exceptions originating
from ``osmfinder`` are caught correctly.
"""

from osmfinder.exceptions import (
    GeometryNotCoveredError,
    GeometryNotCoveredWarning,
    MissingOsmCacheWarning,
    OldOsmCacheWarning,
    OsmExtractIndexOutdatedWarning,
    OsmExtractMultipleMatchesError,
    OsmExtractMultipleMatchesWarning,
    OsmExtractSearchError,
    OsmExtractsIndexesUnavailableError,
    OsmExtractSourceUnavailableWarning,
    OsmExtractsUnavailableError,
    OsmExtractUnavailableWarning,
    OsmExtractZeroMatchesError,
)

__all__ = [
    "EmptyResultWarning",
    "GeometryNotCoveredError",
    "GeometryNotCoveredWarning",
    "InvalidGeometryFilter",
    "MissingOsmCacheWarning",
    "MultiprocessingRuntimeError",
    "OldOsmCacheWarning",
    "OsmExtractIndexOutdatedWarning",
    "OsmExtractMultipleMatchesError",
    "OsmExtractMultipleMatchesWarning",
    "OsmExtractSearchError",
    "OsmExtractSourceUnavailableWarning",
    "OsmExtractUnavailableWarning",
    "OsmExtractZeroMatchesError",
    "OsmExtractsIndexesUnavailableError",
    "OsmExtractsUnavailableError",
    "QueryNotGeocodedError",
]


class EmptyResultWarning(Warning): ...


class InvalidGeometryFilter(Exception): ...


class MultiprocessingRuntimeError(RuntimeError): ...


class QueryNotGeocodedError(ValueError): ...
