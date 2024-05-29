# type: ignore
"""Wrapper over Rich progress bar."""

import json
import os
import time
from collections.abc import Iterable
from datetime import timedelta
from types import TracebackType
from typing import Any, Literal, Optional, Union

from rich import print as rprint
from rich.progress import (
    BarColumn,
    Console,
    Live,
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

__all__ = ["TaskProgressSpinner", "TaskProgressBar"]

TOTAL_STEPS = 32


def log_message(message: str) -> None:
    rprint(message)


def show_total_elapsed_time(elapsed_seconds: float) -> None:
    elapsed_time_formatted = str(timedelta(seconds=int(elapsed_seconds)))
    rprint(f"Finished operation in [progress.elapsed]{elapsed_time_formatted}")


class SpeedColumn(ProgressColumn):
    def render(self, task: "Task") -> Text:
        if task.speed is None:
            return Text("")
        elif task.speed >= 1:
            return Text(f"{task.speed:.2f} it/s")
        else:
            return Text(f"{1/task.speed:.2f} s/it")  # noqa: FURB126


class TransientProgress(Progress):
    def __init__(self, *columns: Union[str, ProgressColumn], live_obj: Live, **kwargs) -> None:
        super().__init__(*columns, **kwargs)
        self.live = live_obj
        self.live._get_renderable = self.get_renderable

    def start(self) -> None:
        if not self.live.transient:
            super().start()

    def stop(self) -> None:
        if not self.live.transient:
            super().stop()

    def __enter__(self) -> Progress:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.stop()


class TaskProgressSpinner:
    def __init__(
        self,
        step_name: str,
        step_number: str,
        silent_mode: bool,
        transient_mode: bool,
        progress_cls: Any,
        live_obj: Any,
        skip_step_number: bool = False,
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.transient_mode = transient_mode
        self.skip_step_number = skip_step_number
        self.progress = None
        self.progress_cls = progress_cls
        self.live_obj = live_obj

    def __enter__(self):
        if self.silent_mode:
            self.progress = None
        else:
            columns = [
                SpinnerColumn(),
                TextColumn(self.step_number),
                TextColumn("[progress.description]{task.description}"),
                TextColumn("•"),
                TimeElapsedColumn(),
            ]

            if self.skip_step_number:
                columns.pop(1)

            self.progress = self.progress_cls(
                *columns,
                live_obj=self.live_obj,
                transient=self.transient_mode,
            )

            self.progress.__enter__()
            self.progress.add_task(description=self.step_name, total=None)

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
        progress_cls: Any,
        live_obj: Any,
        skip_step_number: bool = False,
    ):
        self.step_name = step_name
        self.step_number = step_number
        self.silent_mode = silent_mode
        self.transient_mode = transient_mode
        self.skip_step_number = skip_step_number
        self.progress = None
        self.progress_cls = progress_cls
        self.live_obj = live_obj

    def _create_progress(self):
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

        self.progress = self.progress_cls(
            *columns,
            live_obj=self.live_obj,
            transient=self.transient_mode,
            speed_estimate_period=1800,
        )

    def __enter__(self):
        if self.silent_mode:
            self.progress = None
        else:
            self._create_progress()
            self.progress.__enter__()

        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.progress:
            self.progress.__exit__(exc_type, exc_value, exc_tb)

        self.progress = None

    def create_manual_bar(self, total: int):
        if self.progress:
            self.progress.add_task(description=self.step_name, total=total)

    def update_manual_bar(self, current_progress: int):
        if self.progress:
            self.progress.update(task_id=self.progress.task_ids[0], completed=current_progress)

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
        debug: bool = False,
    ):
        self.verbosity_mode = verbosity_mode
        self.major_step_number: int = 0
        self.minor_step_number: Optional[int] = None
        self.total_major_steps = total_major_steps
        self.current_major_step = current_major_step
        self.transient_progress_cls = None
        self.live = None
        self.console = None
        self.debug = debug
        self.steps_times = {}
        self.start_time = time.time()

        if total_major_steps > 1:
            number_width = len(str(total_major_steps))
            self.major_steps_prefix = f"[{current_major_step: >{number_width}}/{total_major_steps}]"
        else:
            self.major_steps_prefix = ""

        if not self.verbosity_mode == "silent":
            self.force_terminal = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"

            self.console = Console(
                force_interactive=False if self.force_terminal else None,
                force_jupyter=False if self.force_terminal else None,
                force_terminal=True if self.force_terminal else None,
            )
            self.transient_progress_cls = TransientProgress

    def reset_steps(self, current_major_step):
        self.major_step_number: int = 0
        self.minor_step_number: Optional[int] = None

        self.current_major_step = current_major_step
        if self.total_major_steps > 1:
            number_width = len(str(self.total_major_steps))
            self.major_steps_prefix = (
                f"[{self.current_major_step: >{number_width}}/{self.total_major_steps}]"
            )
        else:
            self.major_steps_prefix = ""

    def is_new(self):
        return self.major_step_number == 0 and self.minor_step_number is None

    def stop(self):
        if self.live:
            self.live.stop()
        if self.console and not self.console.is_interactive:
            self.console.print()

        if not self.verbosity_mode == "silent" and not self.is_new():
            end_time = time.time()
            elapsed_seconds = end_time - self.start_time
            show_total_elapsed_time(elapsed_seconds)

        if self.debug:
            rprint(f"Steps times: {json.dumps(self.steps_times)}")

    def _check_live_obj(self):
        if self.verbosity_mode == "silent":
            return
        if not self.live or not self.live._started:
            self.live = Live(
                console=self.console,
                auto_refresh=True,
                refresh_per_second=1,
                transient=self.verbosity_mode == "transient",
                redirect_stdout=True,
                redirect_stderr=True,
            )
            self.live.start(refresh=True)

    def get_spinner(
        self,
        step_name: str,
        next_step: Literal["major", "minor"] = "major",
        with_minor_step: bool = False,
    ) -> TaskProgressSpinner:
        self._parse_steps(next_step=next_step, with_minor_step=with_minor_step)
        self._check_live_obj()
        return TaskProgressSpinner(
            step_name=step_name,
            step_number=self.current_step_number,
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            progress_cls=self.transient_progress_cls,
            live_obj=self.live,
        )

    def get_bar(
        self,
        step_name: str,
        next_step: Literal["major", "minor"] = "major",
        with_minor_step: bool = False,
    ) -> TaskProgressBar:
        self._parse_steps(next_step=next_step, with_minor_step=with_minor_step)
        self._check_live_obj()
        return TaskProgressBar(
            step_name=step_name,
            step_number=self.current_step_number,
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            progress_cls=self.transient_progress_cls,
            live_obj=self.live,
        )

    def get_basic_bar(self, step_name: str) -> TaskProgressBar:
        self._check_live_obj()
        return TaskProgressBar(
            step_name=step_name,
            step_number="",
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            skip_step_number=False,
            progress_cls=self.transient_progress_cls,
            live_obj=self.live,
        )

    def get_basic_spinner(self, step_name: str) -> TaskProgressSpinner:
        self._check_live_obj()
        return TaskProgressSpinner(
            step_name=step_name,
            step_number="",
            silent_mode=self.verbosity_mode == "silent",
            transient_mode=self.verbosity_mode == "transient",
            skip_step_number=False,
            progress_cls=self.transient_progress_cls,
            live_obj=self.live,
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

        if self.debug:
            self.steps_times[step_number] = round(time.time(), 2)

        self.current_step_number = f"{self.major_steps_prefix}[{step_number: >4}/{TOTAL_STEPS}]"
