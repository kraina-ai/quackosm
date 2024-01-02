"""Test big file pipeline."""
from srai.loaders.download import download_file

from quackosm.pbf_file_reader import PbfFileReader


def test_big_file() -> None:
    """Test if big file is working in a low memory environment."""
    download_file("https://download.geofabrik.de/europe/sweden-latest.osm.pbf", "sweden.osm.pbf")

    PbfFileReader().convert_pbf_to_gpq(pbf_path="sweden.osm.pbf", ignore_cache=True)
