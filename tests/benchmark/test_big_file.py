"""Test big file pipeline."""

import shutil
from pathlib import Path

from parametrization import Parametrization as P
from pooch import get_logger as get_pooch_logger
from pooch import retrieve
from shapely import Polygon, box

from quackosm import PbfFileReader
from quackosm._osm_tags_filters import OsmTagsFilter


@P.parameters("extract_name", "geometry_filter", "tags_filter")  # type: ignore
@P.case(
    "Spain",
    "spain",
    box(-10.096436, 35.777019, 3.680420, 44.040591),
    {"name:*": False},
)  # type: ignore
def test_big_file(extract_name: str, geometry_filter: Polygon, tags_filter: OsmTagsFilter) -> None:
    """Test if big file is working in a low memory environment."""
    files_dir = Path("files")
    shutil.rmtree(files_dir)
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

    reader = PbfFileReader(
        working_directory=files_dir,
        verbosity_mode="verbose",
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
    )
    # Reset rows_per_group value to test automatic downscaling
    max_value = max(PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG.keys())
    reader.internal_rows_per_group = PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[max_value]
    reader.convert_pbf_to_parquet(pbf_path=file_name, ignore_cache=True)
