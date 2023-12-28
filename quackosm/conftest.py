"""Fixtures for doctests."""

import urllib.request
from pathlib import Path

import pytest

LFS_DIRECTORY_URL = "https://github.com/kraina-ai/srai-test-files/raw/main/files/"


@pytest.fixture(autouse=True)
def add_pbf_files(doctest_namespace):  # type: ignore
    """Download PBF files used in doctests."""
    extracts = ["monaco", "kiribati", "maldives"]
    download_directory = Path(__file__).parent / "files"
    download_directory.mkdir(parents=True, exist_ok=True)
    for extract_name in extracts:
        pbf_file_download_url = LFS_DIRECTORY_URL + f"{extract_name}-latest.osm.pbf"
        pbf_file_path = download_directory / f"{extract_name}.osm.pbf"
        urllib.request.urlretrieve(pbf_file_download_url, pbf_file_path)
        doctest_namespace[f"{extract_name}_pbf_path"] = pbf_file_path
