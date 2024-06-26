import pytest


def pytest_collection_modifyitems(config, items):
    if not items:
        pytest.exit("No tests found.", returncode=0)
