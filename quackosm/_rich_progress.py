# type: ignore
"""Wrapper over Rich progress bar."""

import os
from collections.abc import Iterable
from contextlib import suppress
from datetime import timedelta

__all__ = ["TaskProgressSpinner", "TaskProgressBar"]

TOTAL_STEPS = 32


def log_message(message: str) -> None:
    try:  # pragma: no cover
        from rich import print as rprint

        rprint(message)
    except ImportError:
        print(message)


def show_total_elapsed_time(elapsed_seconds: float) -> None:
    with suppress(ImportError):  # pragma: no cover
        from rich import print as rprint

        elapsed_time_formatted = str(timedelta(seconds=int(elapsed_seconds)))
        rprint(f"Finished operation in [progress.elapsed]{elapsed_time_formatted}")


class TaskProgressSpinner:
    def __init__(
        self, step_name: str, step_number: str, silent_mode: bool, skip_step_number: bool = False
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.skip_step_number = skip_step_number
        self.progress = None
        self.force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"

    def __enter__(self):
        try:  # pragma: no cover
            if self.silent_mode:
                self.progress = None
            else:
                from rich.progress import (
                    Console,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                )

                columns = [
                    SpinnerColumn(),
                    TextColumn(f"[{self.step_number: >4}/{TOTAL_STEPS}]"),
                    TextColumn("[progress.description]{task.description}"),
                    TextColumn("•"),
                    TimeElapsedColumn(),
                ]

                if self.skip_step_number:
                    columns.pop(1)

                self.progress = Progress(
                    *columns,
                    refresh_per_second=1,
                    transient=False,
                    console=Console(
                        force_interactive=False if self.force_terminal else None,
                        force_jupyter=False if self.force_terminal else None,
                        force_terminal=True if self.force_terminal else None,
                    ),
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
    def __init__(
        self, step_name: str, step_number: str, silent_mode: bool, skip_step_number: bool = False
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.skip_step_number = skip_step_number
        self.progress = None
        self.force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"

    def __enter__(self):
        try:  # pragma: no cover
            if self.silent_mode:
                self.progress = None
            else:
                from rich.progress import (
                    BarColumn,
                    Console,
                    MofNCompleteColumn,
                    Progress,
                    ProgressColumn,
                    SpinnerColumn,
                    Task,
                    Text,
                    TextColumn,
                    TimeElapsedColumn,
                    TimeRemainingColumn,
                )

                class SpeedColumn(ProgressColumn):
                    def render(self, task: "Task") -> Text:
                        if task.speed is None:
                            return Text("")
                        elif task.speed >= 1:
                            return Text(f"{task.speed:.2f} it/s")
                        else:
                            return Text(f"{1/task.speed:.2f} s/it")  # noqa: FURB126

                columns = [
                    SpinnerColumn(),
                    TextColumn(f"[{self.step_number: >4}/{TOTAL_STEPS}]"),
                    TextColumn(
                        "[progress.description]{task.description}"
                        " [progress.percentage]{task.percentage:>3.0f}%"
                    ),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("•"),
                    TimeElapsedColumn(),
                    TextColumn("<"),
                    TimeRemainingColumn(),
                    TextColumn("•"),
                    SpeedColumn(),
                ]

                if self.skip_step_number:
                    columns.pop(1)

                self.progress = Progress(
                    *columns,
                    refresh_per_second=1,
                    transient=False,
                    speed_estimate_period=1800,
                    console=Console(
                        force_interactive=False if self.force_terminal else None,
                        force_jupyter=False if self.force_terminal else None,
                        force_terminal=True if self.force_terminal else None,
                    ),
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
            for i in self.progress.track(list(iterable), description=self.step_name):
                yield i
        else:
            for i in iterable:
                yield i
