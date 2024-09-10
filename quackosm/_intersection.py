from functools import partial
from pathlib import Path
from typing import Optional

import geoarrow.pyarrow as ga
import pyarrow as pa
from shapely import STRtree
from shapely.geometry.base import BaseGeometry

from quackosm._parquet_multiprocessing import map_parquet_dataset
from quackosm._rich_progress import TaskProgressBar  # type: ignore[attr-defined]


def _intersect_nodes(
    table: pa.Table,
    geometry_filter: BaseGeometry,
) -> pa.Table:  # pragma: no cover
    points_array = ga.to_geopandas(
        ga.point().from_geobuffers(
            None,
            x=table["lon"].to_numpy(),
            y=table["lat"].to_numpy(),
        )
    )

    tree = STRtree(points_array)

    intersecting_ids_array = table["id"].take(tree.query(geometry_filter, predicate="intersects"))

    return pa.table({"id": intersecting_ids_array})


def intersect_nodes_with_geometry(
    tmp_dir_path: Path,
    geometry_filter: BaseGeometry,
    progress_bar: Optional[TaskProgressBar] = None,
) -> None:
    """
    Intersects nodes points with geometry filter using spatial index with multiprocessing.

    Args:
        tmp_dir_path (Path): Path of the working directory.
        geometry_filter (BaseGeometry): Geometry used for filtering.
        progress_bar (Optional[TaskProgressBar]): Progress bar to show task status.
            Defaults to `None`
    """
    dataset_path = tmp_dir_path / "nodes_valid_with_tags"
    destination_path = tmp_dir_path / "nodes_intersecting_ids"

    map_parquet_dataset(
        dataset_path=dataset_path,
        destination_path=destination_path,
        progress_bar=progress_bar,
        function=partial(_intersect_nodes, geometry_filter=geometry_filter),
        columns=["id", "lat", "lon"],
    )
