"""Settings for IngestionManager."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    """Environment-driven configuration, prefixed ``UNIFY_INGESTION_``."""

    model_config = SettingsConfigDict(
        env_prefix="UNIFY_INGESTION_",
        extra="ignore",
    )

    IMPL: str = "real"

    # Threads draining the run queue. Small on purpose: a deep pool would let a
    # burst of submissions contend with the assistant it shares a process with.
    INLINE_WORKERS: int = 2

    # Rows per page when reading runs or events back. This is a page size and
    # never a total: reads past it continue by offset rather than truncating,
    # which would silently under-report a long run's history.
    EVENTS_PAGE_SIZE: int = 1_000

    # How long a worker holds a unit of work before its lease may be taken over,
    # and the grace period past expiry before a peer steals it. The grace exists
    # so a merely-slow heartbeat does not lose work to a racing peer.
    LEASE_TTL_SECONDS: int = 900
    LEASE_STEAL_AFTER_SECONDS: int = 30

    # Resume attempts allowed when a run finishes with its durable checkpoint
    # short of the row count the source declared. Bounded so a shortfall that
    # cannot be resolved surfaces as a failure instead of retrying forever.
    INCOMPLETE_MAX_RETRIES: int = 5
    INCOMPLETE_RETRY_SECONDS: int = 15
