"""
tests/settings.py
==================

Test environment settings using pydantic-settings.

TestingSettings inherits all production settings from unify.settings.ProductionSettings
and adds test-only configuration. This mirrors the structure of unify/settings.py.

IMPORTANT: SETTINGS is a lazy proxy to avoid import-order issues. When test
subdirectories set environment variables in pytest_configure(), those run AFTER
the root conftest.py imports this module. The lazy proxy defers instantiation
until first actual use, allowing env vars to be set first.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING

# Set UNILLM_CACHE_DIR to repo root so cache location is consistent regardless of cwd
_REPO_ROOT = Path(__file__).parent.parent.resolve()
os.environ.setdefault("UNILLM_CACHE_DIR", str(_REPO_ROOT))

from pydantic.fields import computed_field

from unify.settings import ProductionSettings

if TYPE_CHECKING:
    from typing import Any


class TestingSettings(ProductionSettings):
    """Test environment settings - inherits all production settings.

    Production settings (UNIFY_MODEL, etc.) are inherited from
    ProductionSettings. This class adds test-only settings.
    """

    # Override composed manager settings with test defaults

    # ─────────────────────────────────────────────────────────────────────────
    # Test Infrastructure Settings
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_INCREMENTING_TIMESTAMPS: bool = (
        False  # Auto-increment timestamps for NEW markers
    )
    EVENTBUS_PUBLISHING_ENABLED: bool = False  # Disabled by default in tests
    UNIFY_DELETE_CONTEXT_ON_EXIT: bool = False
    UNIFY_OVERWRITE_PROJECT: bool = False
    # Each pytest process owns its own store, so deleting the project at the
    # session boundary only matters when a store is reused across runs via
    # UNIFY_STORE_PATH. Both are opt-in.
    UNIFY_TESTS_DELETE_PROJ_ON_START: bool = False
    UNIFY_TESTS_DELETE_PROJ_ON_EXIT: bool = False
    UNIFY_CACHE_STATS: bool = False
    UNIFY_TEST_TAGS: str = ""  # Comma-separated list of tags for duration logging
    UNIFY_TEST_PROJECT_NAME: str = "UnityTests"

    # ─────────────────────────────────────────────────────────────────────────
    # File Lock Settings (for parallel test coordination)
    # ─────────────────────────────────────────────────────────────────────────
    UNIFY_FILE_LOCK_TIMEOUT: float = 3600.0  # 1 hour - handles slow tests under load

    @computed_field
    @property
    def test_project_name(self) -> str:
        """Return the project name every test session activates."""
        return self.UNIFY_TEST_PROJECT_NAME


class _SettingsProxy:
    """Lazy proxy that defers TestingSettings instantiation until first access.

    This solves import-order issues where test subdirectories set environment
    variables in pytest_configure(), which runs AFTER root conftest.py imports
    this module. By deferring instantiation, env vars can be set first.

    The proxy forwards all attribute access to the underlying TestingSettings
    instance, creating it on first use.
    """

    _instance: TestingSettings | None = None

    def _get_instance(self) -> TestingSettings:
        if self._instance is None:
            self._instance = TestingSettings()
        return self._instance

    def __getattr__(self, name: str) -> "Any":
        return getattr(self._get_instance(), name)

    def model_dump(self, **kwargs) -> dict:
        """Forward model_dump() to the underlying instance."""
        return self._get_instance().model_dump(**kwargs)


# Lazy singleton - instantiated on first use, not at import time
SETTINGS: TestingSettings = _SettingsProxy()  # type: ignore[assignment]
