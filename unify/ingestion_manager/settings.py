"""Settings for IngestionManager.

The thresholds below are what ``mode="auto"`` decides on. They are settings rather
than constants because the right boundary depends on the deployment: a pod with
generous memory can absorb a larger inline run than a constrained one, and moving
the boundary should not require a code change.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    """Environment-driven configuration, prefixed ``UNITY_INGESTION_``."""

    model_config = SettingsConfigDict(
        env_prefix="UNITY_INGESTION_",
        extra="ignore",
    )

    IMPL: str = "real"

    # Base URL of the hosted pipeline control plane that runs dispatched work.
    # Empty means this deployment cannot dispatch: submitting such a run then fails
    # immediately and says so, rather than queueing something nothing will collect.
    PIPELINE_URL: str = ""

    # Row count at or below which a rows/table source runs in process. Chosen so a
    # typical API page or connected-app pull stays inline -- dispatching those would
    # add minutes of queue latency to work that takes seconds.
    MAX_INLINE_ROWS: int = 50_000

    # File count at or below which a file source runs in process.
    MAX_INLINE_FILES: int = 10

    # Total bytes at or below which a file source runs in process. Guards the case
    # the file count misses: three very large spreadsheets are dispatch work even
    # though three files sound small.
    MAX_INLINE_BYTES: int = 64 * 1024 * 1024

    # How many worker threads run inline ingestions concurrently. Inline work is
    # I/O-bound against the backend, so a small pool is enough and keeps a burst of
    # submissions from starving the rest of the process.
    INLINE_WORKERS: int = 2

    # Rows kept per status read when reconstructing stage progress.
    MAX_EVENTS_PER_READ: int = 500
