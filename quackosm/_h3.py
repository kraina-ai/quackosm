import platform
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from h3ronpy.arrow.vector import ContainmentMode, wkb_to_cells
from pooch import Decompress, retrieve
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry

START_H3_RESOLUTION = 4
MAX_H3_RESOLUTION = 11
MAX_H3_CELLS = 10_000_000


def _transform_geometry_filter_to_h3(
    geometry: BaseGeometry, working_directory: Path, h3_resolution: Optional[int] = None
) -> tuple[Path, int]:
    """Fill geometry filter with H3 polygons and save them in a dedicated parquet file."""
    result_file_path = working_directory / "geometry_filter_h3_indexes.parquet"
    if isinstance(geometry, BaseMultipartGeometry):
        wkb = [sub_geometry.wkb for sub_geometry in geometry.geoms]
    else:
        wkb = [geometry.wkb]

    search_for_matching_resolution = False
    if h3_resolution is None:
        h3_resolution = START_H3_RESOLUTION
        search_for_matching_resolution = True

    finished_searching = False
    while not finished_searching:
        h3_indexes = wkb_to_cells(
            wkb,
            resolution=h3_resolution,
            containment_mode=ContainmentMode.Covers,
            flatten=True,
        ).unique()

        if not search_for_matching_resolution:
            finished_searching = True
        else:
            number_of_cells = len(h3_indexes)
            # Multiplying by 7, because thats the number of children in a parent cell.
            if (number_of_cells * 7) > MAX_H3_CELLS or h3_resolution == MAX_H3_RESOLUTION:
                finished_searching = True
            else:
                h3_resolution += 1

    pq.write_table(pa.table(dict(h3=h3_indexes)), result_file_path)
    return result_file_path, h3_resolution


# Based on https://github.com/fusedio/udfs/blob/main/public/DuckDB_H3_Example/utils.py
# Will be changed after H3 extension becomes available in the official repository
def _load_h3_duckdb_extension(con: duckdb.DuckDBPyConnection) -> None:
    """Load H3 DuckDB extension for current system."""
    system = platform.system()
    arch = platform.machine()
    arch = "amd64" if arch == "x86_64" else arch

    if system == "Windows":
        detected_os = "windows_amd64"
    elif system == "Darwin":
        detected_os = f"osx_{arch}"
    else:
        detected_os = f"linux_{arch}"
        if detected_os == "linux_amd64":
            detected_os = "linux_amd64_gcc4"

    url = f"https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v{duckdb.__version__}/{detected_os}/h3ext.duckdb_extension.gz"
    # Note this is not the correct file name, it will be fixed later in this function
    # This workaround of downloading in Python is needed because DuckDB cannot load extensions
    # from https (ssl, secure) URLs.
    ungzip_path = retrieve(
        url=url,
        processor=Decompress(name="h3ext.duckdb_extension"),
        known_hash=None,
        path="cache/h3_ext",
    )

    con.sql(f"INSTALL '{ungzip_path}';")
    con.sql("LOAD h3ext;")
