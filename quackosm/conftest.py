"""Fixtures for doctests."""

import doctest
import shutil
import urllib.request
from doctest import OutputChecker
from pathlib import Path

import duckdb
import pytest

from quackosm.osm_extracts.geofabrik import _get_geofabrik_index

IGNORE_RESULT = doctest.register_optionflag("IGNORE_RESULT")


class CustomOutputChecker(OutputChecker):
    """Custom doctest OutputChecker for ignoring logs from functions."""

    def check_output(self: doctest.OutputChecker, want: str, got: str, optionflags: int) -> bool:
        """Skips output checking if IGNORE_RESULT flag is present."""
        if IGNORE_RESULT & optionflags:
            return True
        return OutputChecker.check_output(self, want, got, optionflags)


doctest.OutputChecker = CustomOutputChecker  # type: ignore

LFS_DIRECTORY_URL = "https://github.com/kraina-ai/srai-test-files/raw/main/files/"

EXTRACTS_NAMES = ["monaco", "kiribati", "maldives"]


@pytest.fixture(autouse=True)
def add_pbf_files(doctest_namespace):  # type: ignore
    """Download PBF files used in doctests."""
    download_directory = Path("files")
    download_directory.mkdir(parents=True, exist_ok=True)

    geofabrik_index = _get_geofabrik_index()
    for extract_name in EXTRACTS_NAMES:
        pbf_file_download_url = LFS_DIRECTORY_URL + f"{extract_name}-latest.osm.pbf"
        pbf_file_path = download_directory / f"{extract_name}.osm.pbf"
        geofabrik_download_path = geofabrik_index[geofabrik_index["id"] == extract_name].iloc[0][
            "file_name"
        ]
        geofabrik_pbf_file_path = download_directory / f"{geofabrik_download_path}.osm.pbf"
        urllib.request.urlretrieve(pbf_file_download_url, pbf_file_path)
        doctest_namespace[f"{extract_name}_pbf_path"] = pbf_file_path
        shutil.copy(pbf_file_path, geofabrik_pbf_file_path)


@pytest.fixture(autouse=True)
def install_spatial_extension():  # type: ignore
    """Install duckdb spatial extension."""
    duckdb.install_extension("spatial")
