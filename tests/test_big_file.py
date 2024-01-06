"""Test big file pipeline."""
from pathlib import Path

from parametrization import Parametrization as P
from srai.loaders.download import download_file

from quackosm.pbf_file_reader import PbfFileReader


@P.parameters("extract_name")  # type: ignore
@P.case("Sweden", "sweden")  # type: ignore
@P.case("Czech Republic", "czech-republic")  # type: ignore
@P.case("Norway", "norway")  # type: ignore
@P.case("Poland", "poland")  # type: ignore
def test_big_file(extract_name: str) -> None:
    """Test if big file is working in a low memory environment."""
    file_name = f"{extract_name}.osm.pbf"
    download_file(f"https://download.geofabrik.de/europe/{extract_name}-latest.osm.pbf", file_name)

    gpq_file_path = PbfFileReader().convert_pbf_to_gpq(pbf_path=file_name, ignore_cache=True)
    Path(file_name).unlink()
    gpq_file_path.unlink()
