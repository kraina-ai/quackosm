# type: ignore
"""Wrapper over Rich progress bar."""

import os
from collections.abc import Iterable
from contextlib import suppress
from datetime import timedelta
from typing import Literal, Optional

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
        self,
        step_name: str,
        step_number: str,
        silent_mode: bool,
        transient_mode: bool,
        skip_step_number: bool = False,
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.transient_mode = transient_mode
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
                    TextColumn(self.step_number),
                    TextColumn("[progress.description]{task.description}"),
                    TextColumn("•"),
                    TimeElapsedColumn(),
                ]

                if self.skip_step_number:
                    columns.pop(1)

                self.progress = Progress(
                    *columns,
                    refresh_per_second=1,
                    transient=self.transient_mode,
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
        self,
        step_name: str,
        step_number: str,
        silent_mode: bool,
        transient_mode: bool,
        skip_step_number: bool = False,
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.transient_mode = transient_mode
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
                    TextColumn(self.step_number),
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
                    transient=self.transient_mode,
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


class TaskProgressTracker:
    def __init__(
        self,
        verbosity_mode: Literal["silent", "transient", "verbose"] = "verbose",
        total_major_steps: int = 1,
        current_major_step: int = 1,
    ):
        self.verbosity_mode = verbosity_mode
        self.major_step_number: int = 0
        self.minor_step_number: Optional[int] = None
        self.total_major_steps = total_major_steps
        self.current_major_step = current_major_step

        if total_major_steps > 1:
            number_width = len(str(total_major_steps))
            self.major_steps_prefix = f"[{current_major_step: >{number_width}}/{total_major_steps}]"
        else:
            self.major_steps_prefix = ""

    def is_new(self):
        return self.major_step_number == 0 and self.minor_step_number is None

    def get_spinner(
        self,
        step_name: str,
        next_step: Literal["major", "minor"] = "major",
        with_minor_step: bool = False,
    ) -> TaskProgressSpinner:
        self._parse_steps(next_step=next_step, with_minor_step=with_minor_step)
        return TaskProgressSpinner(
            step_name=step_name,
            step_number=self.current_step_number,
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
        )

    def get_bar(
        self,
        step_name: str,
        next_step: Literal["major", "minor"] = "major",
        with_minor_step: bool = False,
    ) -> TaskProgressBar:
        self._parse_steps(next_step=next_step, with_minor_step=with_minor_step)
        return TaskProgressBar(
            step_name=step_name,
            step_number=self.current_step_number,
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
        )

    def get_basic_bar(self, step_name: str) -> TaskProgressBar:
        return TaskProgressBar(
            step_name=step_name,
            step_number="",
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            skip_step_number=False,
        )

    def get_basic_spinner(self, step_name: str) -> TaskProgressSpinner:
        return TaskProgressSpinner(
            step_name=step_name,
            step_number="",
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            skip_step_number=False,
        )

    def _parse_steps(
        self, next_step: Literal["major", "minor"] = "major", with_minor_step: bool = False
    ):
        if next_step == "major":
            self.major_step_number += 1
            self.minor_step_number = 1 if with_minor_step else None
        elif next_step == "minor":
            if not self.minor_step_number:
                self.minor_step_number = 0
            self.minor_step_number += 1

        step_number = (
            str(self.major_step_number)
            if not self.minor_step_number
            else f"{self.major_step_number}.{self.minor_step_number}"
        )

        self.current_step_number = f"{self.major_steps_prefix}[{step_number: >4}/{TOTAL_STEPS}]"
