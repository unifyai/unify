"""Helpers for deployment-scoped diagnostic logging."""

from __future__ import annotations

from contextlib import contextmanager
import logging
from time import perf_counter
from typing import Iterator


def staging_diagnostics_enabled() -> bool:
    from unity.settings import SETTINGS

    return SETTINGS.DEPLOY_ENV == "staging"


def log_staging_diagnostic(
    logger: logging.Logger,
    message: str,
    *args,
    level: int = logging.INFO,
) -> None:
    if staging_diagnostics_enabled():
        logger.log(level, message, *args)


@contextmanager
def integration_sync_timing(
    logger: logging.Logger,
    phase: str,
    *details: str,
) -> Iterator[None]:
    if not staging_diagnostics_enabled():
        yield
        return

    suffix = f" {' '.join(details)}" if details else ""
    start = perf_counter()
    logger.info("[IntegrationSyncTiming] %s started%s", phase, suffix)
    try:
        yield
    finally:
        logger.info(
            "[IntegrationSyncTiming] %s completed in %.2fs%s",
            phase,
            perf_counter() - start,
            suffix,
        )
