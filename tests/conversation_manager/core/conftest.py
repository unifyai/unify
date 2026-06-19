"""
Pytest configuration for conversation_manager/core tests.

This conftest enables blacklist checks for tests in this directory that
require BlackListManager and unknown contact creation functionality.
"""

import os

import pytest


def pytest_configure(config):
    """Enable blacklist checks for tests in this directory."""
    # Set before any tests run so SETTINGS picks it up
    os.environ["DROID_CONVERSATION_BLACKLIST_CHECKS_ENABLED"] = "true"


@pytest.fixture(autouse=True)
def ensure_blacklist_checks_enabled():
    """Ensure blacklist checks are enabled for all tests in this directory.

    This fixture runs for every test and ensures the environment variable
    is set, even if pytest_configure ran in a different order.
    """
    original = os.environ.get("DROID_CONVERSATION_BLACKLIST_CHECKS_ENABLED")
    os.environ["DROID_CONVERSATION_BLACKLIST_CHECKS_ENABLED"] = "true"
    yield
    # Restore original value
    if original is None:
        os.environ.pop("DROID_CONVERSATION_BLACKLIST_CHECKS_ENABLED", None)
    else:
        os.environ["DROID_CONVERSATION_BLACKLIST_CHECKS_ENABLED"] = original
