"""Global conftest for QuackOSM tests."""
import os

from pytest import Item


def pytest_runtest_setup(item: Item) -> None:
    """Setup python encoding before `pytest_runtest_call(item)`."""
    os.environ["PYTHONIOENCODING"] = "utf-8"
