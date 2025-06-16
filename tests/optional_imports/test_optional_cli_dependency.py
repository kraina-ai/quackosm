"""Optional dependencies tests."""

import sys
from collections.abc import Generator
from typing import Any

import pytest

from quackosm.__main__ import main


@pytest.fixture  # type: ignore[misc]
def optional_packages() -> list[str]:
    """Get a list with optional packages."""
    return [
        "typer",
        "click",
        "geohash",
    ]


@pytest.fixture(autouse=True)  # type: ignore[misc]
def cleanup_imports() -> Generator[Any, Any, Any]:
    """Clean imports."""
    yield
    sys.modules.pop("quackosm", None)


class PackageDiscarder:
    """Mock class for discarding list of packages."""

    def __init__(self) -> None:
        """Init mock class."""
        self.pkgnames: list[str] = []

    def find_spec(self, fullname, path, target=None) -> None:  # type: ignore
        """Throws ImportError if matching module."""
        if fullname in self.pkgnames:
            raise ImportError()


@pytest.fixture  # type: ignore[misc]
def no_optional_dependencies(monkeypatch: Any, optional_packages: Any) -> Generator[Any, Any, Any]:
    """Mock environment without optional dependencies."""
    d = PackageDiscarder()

    for package in optional_packages:
        sys.modules.pop(package, None)
        d.pkgnames.append(package)
    sys.meta_path.insert(0, d)
    yield
    sys.meta_path.remove(d)


@pytest.mark.usefixtures("no_optional_dependencies")  # type: ignore
def test_main_missing_imports() -> None:
    """Test if missing import error is raised."""
    with pytest.raises(ImportError):
        main()
