"""Tests for Parquet multiprocessing wrapper."""

import tempfile
from pathlib import Path
from random import random
from time import sleep
from typing import Any

import duckdb
import pytest

from quackosm._parquet_multiprocessing import map_parquet_dataset


def test_exception_wrapping() -> None:
    """Test if multiprocessing exception raising works.."""
    pbf_file = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"

    with tempfile.TemporaryDirectory(dir=Path(__file__).parent.resolve()) as tmp_dir_name:
        duckdb.install_extension("spatial")
        duckdb.load_extension("spatial")
        nodes_destination = Path(tmp_dir_name) / "nodes_valid_with_tags"
        nodes_destination.mkdir(exist_ok=True, parents=True)
        duckdb.sql(
            f"""
            COPY (
                SELECT
                    id, lon, lat
                FROM ST_ReadOSM('{pbf_file}')
                WHERE kind = 'node'
                AND lat IS NOT NULL AND lon IS NOT NULL
            ) TO '{nodes_destination}' (
                FORMAT 'parquet',
                PER_THREAD_OUTPUT true,
                ROW_GROUP_SIZE 25000
            )
            """
        )

        def raise_error(pa: Any) -> Any:
            sleep(random())
            raise KeyError("XD")

        with pytest.raises(RuntimeError):
            map_parquet_dataset(
                dataset_path=nodes_destination,
                destination_path=Path(tmp_dir_name) / "test",
                function=raise_error,
            )
