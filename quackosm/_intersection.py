import multiprocessing
from pathlib import Path
from queue import Queue
from time import sleep
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from geoarrow.rust.core import PointArray
from shapely import STRtree
from shapely.geometry.base import BaseGeometry

from quackosm._rich_progress import TaskProgressBar, log_message  # type: ignore[attr-defined]


def _intersection_worker(
    queue: Queue[tuple[str, int]], save_path: Path, geometry_filter: BaseGeometry
) -> None:  # pragma: no cover
    current_pid = multiprocessing.current_process().pid

    filepath = save_path / f"{current_pid}.parquet"
    writer = None
    while not queue.empty():
        try:
            file_name = None
            file_name, row_group_index = queue.get(block=True, timeout=1)

            pq_file = pq.ParquetFile(file_name)
            row_group_table = pq_file.read_row_group(row_group_index, ["id", "lat", "lon"])
            if len(row_group_table) == 0:
                continue

            points_array = PointArray.from_xy(
                x=row_group_table["lon"].combine_chunks(), y=row_group_table["lat"].combine_chunks()
            )

            tree = STRtree(points_array.to_shapely())

            intersecting_ids_array = row_group_table["id"].take(
                tree.query(geometry_filter, predicate="intersects")
            )

            table = pa.table({"id": intersecting_ids_array})

            if not writer:
                writer = pq.ParquetWriter(filepath, table.schema)

            writer.write_table(table)
        except Exception as ex:
            log_message(ex)
            if file_name is not None:
                queue.put((file_name, row_group_index))

    if writer:
        writer.close()


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
    queue: Queue[tuple[str, int]] = multiprocessing.Manager().Queue()

    dataset = pq.ParquetDataset(tmp_dir_path / "nodes_valid_with_tags")

    for pq_file in dataset.files:
        for row_group in range(pq.ParquetFile(pq_file).num_row_groups):
            queue.put((pq_file, row_group))

    total = queue.qsize()

    nodes_intersecting_path = tmp_dir_path / "nodes_intersecting_ids"
    nodes_intersecting_path.mkdir(parents=True, exist_ok=True)

    processes = [
        multiprocessing.Process(
            target=_intersection_worker,
            args=(queue, nodes_intersecting_path, geometry_filter),
        )
        for _ in range(multiprocessing.cpu_count())
    ]

    # Run processes
    for p in processes:
        p.start()

    if progress_bar:  # pragma: no cover
        progress_bar.create_manual_bar(total=total)
    while any(process.is_alive() for process in processes):
        if progress_bar:  # pragma: no cover
            progress_bar.update_manual_bar(current_progress=total - queue.qsize())
        sleep(1)

    if progress_bar:  # pragma: no cover
        progress_bar.update_manual_bar(current_progress=total)
