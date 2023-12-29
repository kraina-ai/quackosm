# type: ignore
"""Wrapper over Rich progress bar."""

from collections.abc import Iterable

__all__ = ["TaskProgressSpinner", "TaskProgressBar"]


class TaskProgressSpinner:
    def __init__(self, step_name: str, step_number: str):
        self.step_name = step_name
        self.step_number = step_number
        self.progress = None

    def __enter__(self):
        try:  # pragma: no cover
            from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

            self.progress = Progress(
                SpinnerColumn(),
                TextColumn(f"[{self.step_number: >4}/18]"),
                TextColumn("[progress.description]{task.description}"),
                TextColumn("•"),
                TimeElapsedColumn(),
                transient=False,
            )

            self.progress.__enter__()
            self.progress.add_task(description=self.step_name, total=None)

        except ImportError:
            self.progress = None

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.progress:
            self.progress.__exit__(exc_type, exc_value, exc_tb)

        self.progress = None


class TaskProgressBar:
    def __init__(self, step_name: str, step_number: str):
        self.step_name = step_name
        self.step_number = step_number
        self.progress = None

    def __enter__(self):
        try:  # pragma: no cover
            from rich.progress import (
                BarColumn,
                MofNCompleteColumn,
                Progress,
                SpinnerColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )

            self.progress = Progress(
                SpinnerColumn(),
                TextColumn(f"[{self.step_number: >4}/18]"),
                TextColumn(
                    "[progress.description]{task.description}"
                    " [progress.percentage]{task.percentage:>3.0f}%"
                ),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                transient=False,
            )

            self.progress.__enter__()

        except ImportError:
            self.progress = None

        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.progress:
            self.progress.__exit__(exc_type, exc_value, exc_tb)

        self.progress = None

    def track(self, iterable: Iterable):
        if self.progress is not None:
            for i in self.progress.track(iterable, description=self.step_name):
                yield i
        else:
            for i in iterable:
                yield i
