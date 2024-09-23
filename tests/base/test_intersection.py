"""Tests for PbfFileReader nodes intersection filtering."""

import tempfile
from pathlib import Path

import duckdb
import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.parquet as pq

from quackosm import geocode_to_geometry
from quackosm._intersection import intersect_nodes_with_geometry


def test_nodes_intersection() -> None:
    """Test if multiprocessing implementation works the same as local."""
    pbf_file = Path(__file__).parent.parent / "test_files" / "monaco.osm.pbf"
    geom_filter = geocode_to_geometry("Monaco-Ville, Monaco")

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
        nodes_points = pq.ParquetDataset(nodes_destination).read()
        points_array = ga.to_geopandas(
            ga.point().from_geobuffers(
                None,
                x=nodes_points["lon"].to_numpy(),
                y=nodes_points["lat"].to_numpy(),
            )
        )

        intersecting_points_mask = geom_filter.intersects(points_array)

        intersecting_ids_array = (
            nodes_points["id"].combine_chunks().filter(pa.array(intersecting_points_mask))
        )

        intersect_nodes_with_geometry(tmp_dir_path=Path(tmp_dir_name), geometry_filter=geom_filter)

        intersecting_points = pq.ParquetDataset(
            Path(tmp_dir_name) / "nodes_intersecting_ids"
        ).read()

        assert set(intersecting_points["id"]) == set(intersecting_ids_array)
