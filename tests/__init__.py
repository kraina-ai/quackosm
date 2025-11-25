"""Tests for QuackOSM module."""

import shutil
from pathlib import Path


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


copy_geocode_cache()
