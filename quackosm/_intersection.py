import multiprocessing
from pathlib import Path
from queue import Queue
from time import sleep

import dask

# TODO: update dask and dask-geopandas after https://github.com/geopandas/dask-geopandas/issues/284
dask.config.set({"dataframe.query-planning-warning": False})

import dask_geopandas  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from shapely import Point, STRtree  # noqa: E402
from shapely.geometry.base import BaseGeometry  # noqa: E402

from quackosm._rich_progress import TaskProgressBar  # type: ignore[attr-defined] # noqa: E402

rows_per_partition = 100_000


def intersect_nodes_with_geometry_dask(tmp_dir_path: Path, geometry_filter: BaseGeometry) -> None:
    """
    Intersects nodes points with geometry filter using spatial index with dask geopandas.

    Args:
        tmp_dir_path (Path): Path of the working directory.
        geometry_filter (BaseGeometry): Geometry used for filtering.
    """
    nodes_intersecting_path = tmp_dir_path / "nodes_intersecting_ids"
    nodes_intersecting_path.mkdir(parents=True, exist_ok=True)

    pq_ds = pq.ParquetDataset(tmp_dir_path / "nodes_valid_with_tags")
    total_rows = sum(frag.count_rows() for frag in pq_ds.fragments)

    ddf = dask.dataframe.read_parquet(
        tmp_dir_path / "nodes_valid_with_tags",
        columns=["id", "lon", "lat"],
    ).repartition(npartitions=(1 + total_rows // rows_per_partition))
    ddf = dask_geopandas.from_dask_dataframe(
        ddf, geometry=dask_geopandas.points_from_xy(ddf, "lon", "lat")
    )
    ddf = ddf[ddf.within(geometry_filter)]
    ddf[["id"]].to_parquet(nodes_intersecting_path)


def _intersection_worker(
    queue: Queue[tuple[str, int]], save_path: Path, geometry_filter: BaseGeometry
) -> None:
    current_pid = multiprocessing.current_process().pid

    filepath = save_path / f"{current_pid}.parquet"
    writer = None
    while not queue.empty():
        try:
            file_name, row_group_index = queue.get(block=True, timeout=1)

            pq_file = pq.ParquetFile(file_name)
            row_group_table = pq_file.read_row_group(row_group_index, ["id", "lat", "lon"])
            if len(row_group_table) == 0:
                continue

            tree = STRtree(
                [
                    Point(lon.as_py(), lat.as_py())
                    for lon, lat in zip(row_group_table["lon"], row_group_table["lat"])
                ]
            )

            intersecting_ids_array = row_group_table["id"].take(
                tree.query(geometry_filter, predicate="intersects")
            )

            table = pa.table({"id": intersecting_ids_array})

            if not writer:
                writer = pq.ParquetWriter(filepath, table.schema)

            writer.write_table(table)
        except Exception as ex:
            print(ex)

    if writer:
        writer.close()


def intersect_nodes_with_geometry(
    tmp_dir_path: Path, geometry_filter: BaseGeometry, progress_bar: TaskProgressBar
) -> None:
    """
    Intersects nodes points with geometry filter using spatial index with multiprocessing.

    Args:
        tmp_dir_path (Path): Path of the working directory.
        geometry_filter (BaseGeometry): Geometry used for filtering.
        progress_bar (TaskProgressBar): Progress bar to show task status.
    """
    manager = multiprocessing.Manager()
    queue: Queue[tuple[str, int]] = manager.Queue()

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

    progress_bar.create_manual_bar(total=total)
    while any(process.is_alive() for process in processes):
        progress_bar.update_manual_bar(current_progress=total - queue.qsize())
        sleep(1)

    progress_bar.update_manual_bar(current_progress=total)
