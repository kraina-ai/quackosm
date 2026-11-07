"""Step numbering over the shared progress display."""

import json
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Literal, TypeAlias

from rich import print as rprint
from rq_geo_toolkit.rich_utils import (
    VERBOSITY_MODE,
    ProgressTask,
    StepCounter,
    progress_display,
)

__all__ = [
    "VERBOSITY_MODE",
    "TaskProgressBar",
    "TaskProgressTracker",
    "log_message",
]

TOTAL_STEPS = 16
TaskProgressBar: TypeAlias = ProgressTask


def log_message(message: str) -> None:
    with progress_display() as display:
        # prefixed with the operation label when operations share the display
        if display.is_silent:
            rprint(message)
        else:
            display.print(message)


class TaskProgressTracker:
    def __init__(
        self,
        verbosity_mode: VERBOSITY_MODE = "verbose",
        total_file_steps: int = 1,
        current_file_step: int = 1,
        debug: bool = False,
    ):
        self.verbosity_mode = verbosity_mode
        self.total_files_steps = total_file_steps
        self.debug = debug
        self.steps_times: dict[str, float] = {}
        with progress_display(verbosity_mode) as display:
            # the counter of the operation, declared with `with_progress_display(total_steps)`
            self.steps = display.steps or StepCounter(TOTAL_STEPS)
        self.reset_steps(current_file_step)

    @property
    def major_step_number(self) -> int:
        return self.steps.major_step_number  # type: ignore[no-any-return]

    @major_step_number.setter
    def major_step_number(self, value: int) -> None:
        self.steps.major_step_number = value

    @property
    def major_step_label(self) -> str:
        width = len(str(self.steps.total_steps))
        return f"[{self.major_step_number: >{width}}/{self.steps.total_steps}]"

    def reset_steps(self, current_file_step: int) -> None:
        prefix = ""
        if self.total_files_steps > 1:
            number_width = len(str(self.total_files_steps))
            prefix = f"[{current_file_step: >{number_width}}/{self.total_files_steps}]"
        self.steps.reset(prefix)

    def stop(self) -> None:
        if self.debug:
            self.steps_times["finish"] = round(time.time(), 2)
            rprint(f"Steps times: {json.dumps(self.steps_times)}")

    def start_step_group(self, step_name: str, total_minor_steps: int) -> None:
        with progress_display(self.verbosity_mode) as display:
            display.step_group(step_name, total_minor_steps)

    def skip_step(self, step_name: str) -> None:
        with progress_display(self.verbosity_mode) as display:
            display.skip_step(step_name)

    @contextmanager
    def get_spinner(
        self, step_name: str, next_step: Literal["major", "minor"] = "major"
    ) -> Generator[None, None, None]:
        with (
            progress_display(self.verbosity_mode) as display,
            display.spinner(step_name, step=next_step),
        ):
            self._record_step_time()
            yield

    @contextmanager
    def get_bar(
        self, step_name: str, next_step: Literal["major", "minor"] = "major"
    ) -> Generator[TaskProgressBar, None, None]:
        with (
            progress_display(self.verbosity_mode) as display,
            display.bar(step_name, step=next_step) as bar,
        ):
            self._record_step_time()
            yield bar

    @contextmanager
    def get_basic_bar(self, step_name: str) -> Generator[TaskProgressBar, None, None]:
        with progress_display(self.verbosity_mode) as display, display.bar(step_name) as bar:
            yield bar

    @contextmanager
    def get_basic_spinner(self, step_name: str) -> Generator[None, None, None]:
        with progress_display(self.verbosity_mode) as display, display.spinner(step_name):
            yield

    def _record_step_time(self) -> None:
        if self.debug:
            self.steps_times[self.steps.step_number] = round(time.time(), 2)
