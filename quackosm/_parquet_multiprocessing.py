import multiprocessing
import traceback
from pathlib import Path
from queue import Empty, Queue
from time import sleep
from typing import Callable, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from quackosm._rich_progress import TaskProgressBar  # type: ignore[attr-defined]


def _job(
    queue: Queue[tuple[str, int]],
    save_path: Path,
    function: Callable[[pa.Table], pa.Table],
    columns: Optional[list[str]] = None,
) -> None:  # pragma: no cover
    current_pid = multiprocessing.current_process().pid

    filepath = save_path / f"{current_pid}.parquet"
    writer = None
    while not queue.empty():
        try:
            file_name, row_group_index = None, None
            file_name, row_group_index = queue.get_nowait()

            pq_file = pq.ParquetFile(file_name)
            row_group_table = pq_file.read_row_group(row_group_index, columns=columns)
            if len(row_group_table) == 0:
                continue

            result_table = function(row_group_table)

            if not writer:
                writer = pq.ParquetWriter(filepath, result_table.schema)

            writer.write_table(result_table)
        except Empty:
            pass
        except Exception as ex:
            if file_name is not None and row_group_index is not None:
                queue.put((file_name, row_group_index))

            msg = (
                f"Error in worker (PID: {current_pid},"
                f" Parquet: {file_name}, Row group: {row_group_index})"
            )
            raise RuntimeError(msg) from ex

    if writer:
        writer.close()


class WorkerProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception: Optional[tuple[Exception, str]] = None

    def run(self) -> None: # pragma: no cover
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb: str = traceback.format_exc()
            self._cconn.send((e, tb))

    @property
    def exception(self) -> Optional[tuple[Exception, str]]:
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def map_parquet_dataset(
    dataset_path: Path,
    destination_path: Path,
    function: Callable[[pa.Table], pa.Table],
    columns: Optional[list[str]] = None,
    progress_bar: Optional[TaskProgressBar] = None,
) -> None:
    """
    Apply a function over parquet dataset in a multiprocessing environment.

    Will save results in multiple files in a destination path.

    Args:
        dataset_path (Path): Path of the parquet dataset.
        destination_path (Path): Path of the destination.
        function (Callable[[pa.Table], pa.Table]): Function to apply over a row group table.
            Will save resulting table in a new parquet file.
        columns (Optional[list[str]]): List of columns to read. Defaults to `None`.
        progress_bar (Optional[TaskProgressBar]): Progress bar to show task status.
            Defaults to `None`.
    """
    queue: Queue[tuple[str, int]] = multiprocessing.Manager().Queue()

    dataset = pq.ParquetDataset(dataset_path)

    for pq_file in dataset.files:
        for row_group in range(pq.ParquetFile(pq_file).num_row_groups):
            queue.put((pq_file, row_group))

    total = queue.qsize()

    destination_path.mkdir(parents=True, exist_ok=True)

    try:
        processes = [
            WorkerProcess(
                target=_job,
                args=(queue, destination_path, function, columns),
            )  # type: ignore[no-untyped-call]
            for _ in range(multiprocessing.cpu_count())
        ]

        # Run processes
        for p in processes:
            p.start()

        if progress_bar:  # pragma: no cover
            progress_bar.create_manual_bar(total=total)
        while any(process.is_alive() for process in processes):
            if any(p.exception for p in processes): # pragma: no cover
                break

            if progress_bar:  # pragma: no cover
                progress_bar.update_manual_bar(current_progress=total - queue.qsize())
            sleep(1)

        if progress_bar:  # pragma: no cover
            progress_bar.update_manual_bar(current_progress=total)
    finally:  # pragma: no cover
        # In case of exception
        exceptions = []
        for p in processes:
            if p.is_alive():
                p.terminate()

            if p.exception:
                exceptions.append(p.exception)

        if exceptions:
            # use ExceptionGroup in Python3.11
            _raise_multiple(exceptions)


def _raise_multiple(exceptions: list[tuple[Exception, str]]) -> None:
    if not exceptions:
        return
    try:
        error, traceback = exceptions.pop()
        msg = f"{error}\n\nOriginal {traceback}"
        raise type(error)(msg)
    finally:
        _raise_multiple(exceptions)
