"""OpenStreetMap extract class."""

from dataclasses import dataclass

from shapely.geometry.base import BaseGeometry


@dataclass
class OpenStreetMapExtract:
    """OSM Extract metadata object."""

    id: str
    url: str
    geometry: BaseGeometry
