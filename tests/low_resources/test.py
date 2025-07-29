"""Run full country file in a low memory environment."""

import multiprocessing
import os
from contextlib import suppress
from pathlib import Path

import duckdb
import psutil
from pooch import get_logger as get_pooch_logger
from pooch import retrieve
from psutil._common import bytes2human

import quackosm as qosm


def test_country_file() -> None:
    """Test if big file is working in a low memory environment."""
    extract_name = "portugal"
    display_resources()
    files_dir = Path("files")
    file_name = files_dir / f"{extract_name}.osm.pbf"
    logger = get_pooch_logger()
    logger.setLevel("WARNING")
    retrieve(
        f"https://download.geofabrik.de/europe/{extract_name}-latest.osm.pbf",
        fname=f"{extract_name}.osm.pbf",
        path=files_dir,
        progressbar=False,
        known_hash=None,
    )

    qosm.convert_pbf_to_parquet(
        pbf_path=file_name, ignore_cache=True, verbosity_mode="verbose", sort_result=True
    )


def display_resources() -> None:
    """Show available resources."""
    print(
        "CPU count (duckdb):",
        duckdb.sql("SELECT current_setting('threads') AS threads").fetchone()[0],
    )
    print("CPU count (multiprocessing):", multiprocessing.cpu_count())
    print("CPU count (psutil):", psutil.cpu_count())
    print("CPU count (psutil physical):", psutil.cpu_count(logical=False))

    print(
        "Memory (duckdb):",
        duckdb.sql("SELECT current_setting('memory_limit') AS memlimit").fetchone()[0],
    )
    print("Memory (psutil):", bytes2human(psutil.virtual_memory().total))
    print("Memory (psutil, all):", psutil.virtual_memory())

    with suppress(Exception):
        print("CPU affinity (os):", os.sched_getaffinity(0))
        print("CPU affinity (psutil):", psutil.Process().cpu_affinity())
        print("Memory limit (cgroup):", bytes2human(int(open("/sys/fs/cgroup/memory.max").read())))


if __name__ == "__main__":
    test_country_file()
