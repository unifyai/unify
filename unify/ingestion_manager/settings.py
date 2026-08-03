"""Settings for IngestionManager.

Deliberately small. An earlier shape carried thresholds for file count and for
how long an in-process run was allowed to take, and both were unsound: what a
file costs to parse is unknowable before parsing it, so any number derived from
count or bytes misroutes work in both directions -- a 40 KB PDF can be hundreds
of dense pages and a 12 MB spreadsheet one sheet of images.

What remains is a single row ceiling, and it is legitimate for a reason the file
thresholds were not: a row count is *measured*, exactly and cheaply, before
anything runs. Rows in hand are counted directly; rows in a context are counted
by one server-side aggregate. Deciding on a measurement is sound; deciding on a
prediction is not.
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

    # Explicit override for the pipeline control plane's base URL. Normally
    # unset: the control plane is mounted on the communication service this
    # deployment already talks to, so `resolved_pipeline_url` falls back to that
    # rather than requiring a second variable to be set in lockstep. Requiring
    # one was a real hazard -- a hosted pod missing it reads as "no fleet" and
    # parses files in the assistant's own process, which is the one thing the
    # tier rule exists to prevent, and it fails silently by doing the work.
    PIPELINE_URL: str = ""

    def resolved_pipeline_url(self) -> str:
        """Base URL of the control plane, or empty when there is genuinely none.

        Empty means no fleet is reachable and every run executes in process --
        safe rather than merely tolerable, because both tiers write the same
        artifacts and checkpoints, so a fleet configured later adopts whatever
        an interrupted local run left behind.
        """
        if self.PIPELINE_URL:
            return self.PIPELINE_URL
        # Imported lazily: the root settings module imports this one.
        from unify.settings import SETTINGS

        return (SETTINGS.conversation.COMMS_URL or "").rstrip("/")

    # Row count at or below which a rows or table source runs in process.
    #
    # This is not a memory limit -- it is a latency boundary. Below it, queue
    # round-trip and worker cold start dominate the work itself, and the common
    # case is a plan that fetches a page from an API and builds a canvas over it
    # in the next step. Above it, ingestion is sustained I/O that belongs on
    # workers that scale horizontally and do not compete with the assistant for
    # its own process.
    MAX_INLINE_ROWS: int = 10_000

    # Threads draining the in-process queue. Small on purpose: in-process work
    # exists for latency, not throughput, and a deep pool would let a burst of
    # submissions contend with the assistant it shares a process with.
    INLINE_WORKERS: int = 2

    # Rows per page when reading runs or events back. The backend caps a single
    # read at 1000, so this is a page size and never a total: reads past it
    # continue by offset rather than truncating, which would silently under-report
    # a long run's history.
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
