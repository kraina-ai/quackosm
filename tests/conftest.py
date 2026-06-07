"""Global conftest for QuackOSM tests."""

import os
import shutil
from pathlib import Path

import pytest
from pytest import Item


# Copy cache before loading tests
def copy_geocode_cache() -> None:
    """Load cached geocoding results."""
    existing_cache_directory = Path(__file__).parent / "test_files" / "geocoding_cache"
    geocoding_cache_directory = Path("cache")
    geocoding_cache_directory.mkdir(exist_ok=True)
    for file_path in existing_cache_directory.glob("*.json"):
        destination_path = geocoding_cache_directory / file_path.name
        shutil.copy(file_path, destination_path)
        print(f"Copied {file_path} to {destination_path}")


def pytest_runtest_setup(item: Item) -> None:
    """Setup python encoding before `pytest_runtest_call(item)`."""
    os.environ["PYTHONIOENCODING"] = "utf-8"


@pytest.fixture(autouse=True, scope="session")
def remove_monaco_db_file():  # type: ignore
    """Remove old DuckDB file with tests results."""
    file_to_find = Path("files/monaco_nofilter_noclip_compact_sorted.duckdb")
    print(file_to_find, file_to_find.exists())
    if file_to_find.exists():
        file_to_find.unlink()
    print(file_to_find, file_to_find.exists())


copy_geocode_cache()
