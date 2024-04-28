"""Fixtures for doctests."""

import sys
import urllib.request
from pathlib import Path
from typing import Any, Union

import pytest
from pooch import retrieve

LFS_DIRECTORY_URL = "https://github.com/kraina-ai/srai-test-files/raw/main/files/"

EXTRACTS_NAMES = ["monaco", "kiribati", "maldives"]


def _retrieve_mock( # type: ignore
    url,
    known_hash,
    fname=None,
    path=None,
    processor=None,
    downloader=None,
    progressbar=False,
) -> Union[Path, str, Any]:
    print("mocked pooch function", url)
    for extract_name in EXTRACTS_NAMES:
        if url == f"https://download.geofabrik.de/europe/{extract_name}-latest.osm.pbf":
            return Path(__file__).parent / "files" / f"{extract_name}.osm.pbf"

    return retrieve(
        url=url,
        known_hash=known_hash,
        fname=fname,
        path=path,
        processor=processor,
        downloader=downloader,
        progressbar=progressbar,
    )


@pytest.fixture(autouse=True)
def add_pbf_files(doctest_namespace, mocker):  # type: ignore
    """Download PBF files used in doctests."""
    download_directory = Path(__file__).parent / "files"
    download_directory.mkdir(parents=True, exist_ok=True)
    for extract_name in EXTRACTS_NAMES:
        pbf_file_download_url = LFS_DIRECTORY_URL + f"{extract_name}-latest.osm.pbf"
        pbf_file_path = download_directory / f"{extract_name}.osm.pbf"
        urllib.request.urlretrieve(pbf_file_download_url, pbf_file_path)
        doctest_namespace[f"{extract_name}_pbf_path"] = pbf_file_path

    mocker.patch("pooch.retrieve", new=_retrieve_mock)


@pytest.fixture  # type: ignore
def optional_packages() -> list[str]:
    """Get a list with optional packages."""
    return [
        "rich",
    ]


@pytest.fixture(autouse=True)  # type: ignore
def cleanup_imports():
    """Clean imports."""
    yield
    sys.modules.pop("quackosm", None)


class PackageDiscarder:
    """Mock class for discarding list of packages."""

    def __init__(self) -> None:
        """Init mock class."""
        self.pkgnames: list[str] = []

    def find_spec(self, fullname, path, target=None) -> None:  # type: ignore
        """Throws ImportError if matching module."""
        if fullname in self.pkgnames:
            raise ImportError()


@pytest.fixture(autouse=True)  # type: ignore
def no_optional_dependencies(monkeypatch, optional_packages):
    """Mock environment without optional dependencies."""
    d = PackageDiscarder()

    for package in optional_packages:
        sys.modules.pop(package, None)
        d.pkgnames.append(package)
    sys.meta_path.insert(0, d)
    yield
    sys.meta_path.remove(d)
