"""Test big file pipeline."""

import shutil
from pathlib import Path

from parametrization import Parametrization as P
from srai.loaders.download import download_file

from quackosm import PbfFileReader, geocode_to_geometry
from quackosm._osm_tags_filters import OsmTagsFilter


@P.parameters("extract_name", "geocode_filter", "tags_filter")  # type: ignore
@P.case(
    "Spain",
    "spain",
    ["Madrid", "Barcelona", "Valencia", "Seville"],
    {"building": True, "amenity": True},
)  # type: ignore
def test_big_file(extract_name: str, geocode_filter: list[str], tags_filter: OsmTagsFilter) -> None:
    """Test if big file is working in a low memory environment."""
    files_dir = Path("files")
    shutil.rmtree(files_dir)
    file_name = files_dir / f"{extract_name}.osm.pbf"
    download_file(
        f"https://download.geofabrik.de/europe/{extract_name}-latest.osm.pbf", str(file_name)
    )

    reader = PbfFileReader(
        working_directory=files_dir,
        verbosity_mode="verbose",
        tags_filter=tags_filter,
        geometry_filter=geocode_to_geometry(geocode_filter),
    )
    # Reset rows_per_group value to test automatic downscaling
    reader.rows_per_group = PbfFileReader.ROWS_PER_GROUP_MEMORY_CONFIG[24]
    reader.convert_pbf_to_parquet(pbf_path=file_name, ignore_cache=True)
