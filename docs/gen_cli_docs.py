"""Capture the CLI help page and save it to the docs."""

from pathlib import Path
from typing import cast

import mkdocs_gen_files
import typer
from rich.console import Console
from typer.rich_utils import (
    COLOR_SYSTEM,
    FORCE_TERMINAL,
    STYLE_NEGATIVE_OPTION,
    STYLE_NEGATIVE_SWITCH,
    STYLE_OPTION,
    STYLE_SWITCH,
    STYLE_USAGE,
    Theme,
    highlighter,
    rich_format_help,
)

# typer >= 0.27 renamed STYLE_METAVAR -> STYLE_TYPES and
# STYLE_METAVAR_SEPARATOR -> STYLE_TYPES_SEPARATOR (PR #1863).
# Support both old and new typer versions.
try:
    from typer.rich_utils import STYLE_TYPES as STYLE_METAVAR
    from typer.rich_utils import STYLE_TYPES_SEPARATOR as STYLE_METAVAR_SEPARATOR

    _THEME_METAVAR_KEY = "types"
    _THEME_METAVAR_SEPARATOR_KEY = "types_sep"
except ImportError:  # typer < 0.27
    from typer.rich_utils import STYLE_METAVAR, STYLE_METAVAR_SEPARATOR

    _THEME_METAVAR_KEY = "metavar"
    _THEME_METAVAR_SEPARATOR_KEY = "metavar_sep"

from quackosm.cli import app

API_DIRECTORY_PATH = Path("api")

GLOBAL_CONSOLE = None


def _get_rich_console_new(stderr: bool = False) -> Console:
    global GLOBAL_CONSOLE  # noqa: PLW0603
    GLOBAL_CONSOLE = Console(
        theme=Theme(
            {
                "option": STYLE_OPTION,
                "switch": STYLE_SWITCH,
                "negative_option": STYLE_NEGATIVE_OPTION,
                "negative_switch": STYLE_NEGATIVE_SWITCH,
                _THEME_METAVAR_KEY: STYLE_METAVAR,
                _THEME_METAVAR_SEPARATOR_KEY: STYLE_METAVAR_SEPARATOR,
                "usage": STYLE_USAGE,
            },
        ),
        record=True,
        highlighter=highlighter,
        color_system=COLOR_SYSTEM,
        force_terminal=FORCE_TERMINAL,
        width=240,
        stderr=stderr,
    )
    return GLOBAL_CONSOLE


typer.rich_utils._get_rich_console = _get_rich_console_new

typer_obj = app

click_obj = typer.main.get_command(typer_obj)
ctx = typer.Context(command=click_obj, info_name="QuackOSM")
rich_format_help(obj=click_obj, ctx=ctx, markup_mode="rich")
html_text: str = cast("Console", GLOBAL_CONSOLE).export_html(
    inline_styles=True,
    code_format='<div class="highlight"><pre><code>{code}</code></pre></div>',
)
html_text = html_text.replace(
    "font-weight: bold",
    (
        "font-weight: normal;"
        " text-shadow: calc(-0.06ex) 0 0 currentColor, calc(0.06ex) 0 0 currentColor;"
    ),
)

with mkdocs_gen_files.open(API_DIRECTORY_PATH / "CLI.md", "a") as fd:
    print(html_text, file=fd)
