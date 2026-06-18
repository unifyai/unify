from __future__ import annotations

import os


def pytest_configure(config) -> None:
    """Configure manager implementations for handle integration tests."""
    os.environ["DROID_CONTACT_IMPL"] = "real"
    os.environ["DROID_TRANSCRIPT_IMPL"] = "real"
