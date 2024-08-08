class EmptyResultWarning(Warning): ...


class GeometryNotCoveredWarning(Warning): ...


class GeometryNotCoveredError(Exception): ...


class InvalidGeometryFilter(Exception): ...


class MultiprocessingRuntimeError(RuntimeError): ...



class OsmExtractIndexOutdatedWarning(Warning): ...


class OsmExtractSearchError(Exception):
    def __init__(self, message: str, matching_full_names: list[str]):
        super().__init__(message)
        self.matching_full_names = matching_full_names


class OsmExtractZeroMatchesError(OsmExtractSearchError): ...


class OsmExtractMultipleMatchesError(OsmExtractSearchError): ...

class QueryNotGeocodedError(ValueError): ...
