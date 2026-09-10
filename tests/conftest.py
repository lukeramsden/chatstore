import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "real_source" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="real-source tests are local-only"))
