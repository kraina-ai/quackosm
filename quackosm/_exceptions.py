class EmptyResultWarning(Warning): ...


class GeometryNotCoveredWarning(Warning): ...


class GeometryNotCoveredError(Exception): ...


class InvalidGeometryFilter(Exception): ...


class MultiprocessingRuntimeError(RuntimeError): ...


class OsmExtractIndexOutdatedWarning(Warning): ...


class OsmExtractSearchError(Exception): ...


class OsmExtractZeroMatchesError(OsmExtractSearchError):
    def __init__(self, message: str, suggested_names: list[str]):
        super().__init__(message)
        self.suggested_names = suggested_names


class OsmExtractMultipleMatchesError(OsmExtractSearchError):
    def __init__(self, message: str, matching_full_names: list[str]):
        super().__init__(message)
        self.matching_full_names = matching_full_names
