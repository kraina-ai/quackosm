"""Main CLI module."""

from quackosm import __app_name__, cli


def main() -> None:
    """Run provided CLI."""
    cli.app(prog_name=__app_name__)


if __name__ == "__main__":
    main()
