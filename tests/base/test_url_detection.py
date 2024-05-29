"""Tests for proper URL detection."""

import pytest

from quackosm.pbf_file_reader import _is_url_path


@pytest.mark.parametrize(
    "path,is_url",
    [
        ("D:/a/quackosm/files/seychelles.osm.pbf", False),
        ("http://127.0.0.1:8080/seychelles.osm.pbf", True),
        ("ftp://127.0.0.1:8080/seychelles.osm.pbf", True),
        ("/dev/files/seychelles.osm.pbf", False),
        ("files/seychelles.osm.pbf", False),
        ("/files/seychelles.osm.pbf", False),
        ("./files/seychelles.osm.pbf", False),
        ("../files/seychelles.osm.pbf", False),
        ("http://download.geofabrik.de/africa/seychelles-latest.osm.pbf", True),
    ],
)  # type: ignore
def test_url_parsing(path: str, is_url: bool) -> None:
    """Test if URL detection works."""
    assert _is_url_path(path) == is_url
