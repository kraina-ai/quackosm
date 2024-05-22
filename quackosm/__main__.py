"""Main CLI module."""


def main() -> None:
    """Run provided CLI."""
    try:
        from quackosm import __app_name__, cli
    except ImportError as exc:
        error_msg = (
            "Missing optional dependencies required for the CLI."
            " Please install required packages using `pip install quackosm[cli]`."
        )
        raise ImportError(error_msg) from exc

    cli.app(prog_name=__app_name__)  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    main()
