"""Run observability written to the ``Ingestion/*`` contexts, from any tier.

The run row and its events are the record a person or the actor reads;
checkpoints and leases stay on the artifact store because they are correctness
primitives that need fenced compare-and-swap, which a log row does not offer.
This module is the bridge for code that runs *outside* the manager's process --
worker pods -- so a dispatched run's progress lands in the same two contexts an
in-process run already writes, and ``get_status`` reads one history whichever
tier executed.

Drift between the stores is avoided by construction rather than by
reconciliation jobs: events are emitted at the same commit points that write
checkpoints, and the run row's terminal state is written exactly once by
whoever finishes the work. Progress numbers derive from the checkpoint counts,
so the two can disagree only in staleness, never in direction.

Writes go through unisdk under whatever identity is installed --- the worker
resolves the owning assistant's key per message and hydrates the session before
touching this, so a journal row is written *as the assistant*, into contexts
the assistant already owns. Rows are small on purpose (decision: never a bulk
payload in a log row); anything heavy stays in object storage and is referenced
by key.

A journal is deliberately best-effort on the worker side: ingestion must not
fail because observability was unreachable, so write errors are logged and
swallowed here and nowhere else.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Payload block a dispatch carries so workers can journal against the run the
# manager recorded. Absent on payloads that predate it, in which case the
# journal is inert and the GCS event trail remains the only record.
OBSERVABILITY_KEY = "observability"

# Floor between progress events per stage. Stage transitions and terminal
# states always write; only the chunk-by-chunk counter is throttled, because a
# 500k-row table would otherwise write a thousand rows that all say "working".
PROGRESS_INTERVAL_SECONDS = 15.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunJournal:
    """Writes one run's events and terminal state to its ``Ingestion/*`` rows.

    One instance per (run, worker attempt). Thread-safe for the progress
    throttle; unisdk calls are already safe to make concurrently.
    """

    def __init__(
        self,
        *,
        run_key: str,
        runs_context: str,
        events_context: str,
    ) -> None:
        self.run_key = run_key
        self._runs_context = runs_context
        self._events_context = events_context
        self._lock = threading.Lock()
        self._last_progress: Dict[str, float] = {}

    # -- events ---------------------------------------------------------------

    def event(
        self,
        *,
        stage: Optional[str] = None,
        level: str = "info",
        message: str = "",
        state: Optional[str] = None,
        done: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        """Append one event row. Never raises; a run outlives its telemetry."""
        entry: Dict[str, Any] = {
            "run_key": self.run_key,
            "at": _now(),
            "level": level,
            "message": message,
        }
        if stage is not None:
            entry["stage"] = stage
        if state is not None:
            entry["state"] = state
        if done is not None:
            entry["done"] = int(done)
        if total is not None:
            entry["total"] = int(total)

        try:
            import unisdk

            unisdk.create_logs(context=self._events_context, entries=[entry])
        except Exception as error:  # noqa: BLE001 -- observability must not kill work
            logger.warning(
                "Run %s could not journal an event to %s: %s",
                self.run_key,
                self._events_context,
                error,
            )

    def progress(
        self,
        *,
        stage: str,
        done: int,
        total: Optional[int] = None,
        message: str = "",
    ) -> None:
        """Append a progress event, at most once per stage per interval.

        The checkpoint remains the exact record; these rows exist so a reader
        polling the run sees movement without replaying object storage.
        """
        now = time.monotonic()
        with self._lock:
            last = self._last_progress.get(stage, 0.0)
            if now - last < PROGRESS_INTERVAL_SECONDS:
                return
            self._last_progress[stage] = now
        self.event(
            stage=stage,
            state="running",
            done=done,
            total=total,
            message=message or f"Committed {done} row(s).",
        )

    # -- the run row ----------------------------------------------------------

    def update_run(self, updates: Dict[str, Any]) -> None:
        """Merge *updates* into the run's row. Never raises.

        Addressed by ``run_key`` rather than a stored row id so a journal can
        be constructed from a message payload alone. Terminal updates should
        come from exactly one place per run -- the completion gate of whoever
        finished the work -- which is what keeps this write conflict-free
        without needing the row store to arbitrate.
        """
        try:
            import unisdk

            rows = unisdk.get_logs(
                context=self._runs_context,
                filter=f"run_key == '{self.run_key}'",
                limit=1,
            )
            if not rows:
                logger.warning(
                    "Run %s has no row in %s to update",
                    self.run_key,
                    self._runs_context,
                )
                return
            unisdk.update_logs(logs=rows[0], entries=updates)
        except Exception as error:  # noqa: BLE001 -- observability must not kill work
            logger.warning(
                "Run %s could not update its row in %s: %s",
                self.run_key,
                self._runs_context,
                error,
            )


class NullJournal:
    """Journal for payloads that carry no observability block.

    Same surface, no writes. Lets worker code journal unconditionally instead
    of branching at every call site.
    """

    run_key = ""

    def event(self, **_kwargs: Any) -> None:
        return

    def progress(self, **_kwargs: Any) -> None:
        return

    def update_run(self, _updates: Dict[str, Any]) -> None:
        return


def journal_from_payload(payload: Dict[str, Any]) -> RunJournal | NullJournal:
    """The journal a work item's payload asks for, or an inert one.

    The dispatch submit stages the block under :data:`OBSERVABILITY_KEY` with
    the run key and the two context paths the manager recorded the run in.
    A payload without it (an operator CLI submit, a message from before the
    block existed) journals nowhere rather than guessing at context paths.
    """
    block = payload.get(OBSERVABILITY_KEY) or {}
    run_key = str(block.get("run_key") or "")
    runs_context = str(block.get("runs_context") or "")
    events_context = str(block.get("events_context") or "")
    if not (run_key and runs_context and events_context):
        return NullJournal()
    return RunJournal(
        run_key=run_key,
        runs_context=runs_context,
        events_context=events_context,
    )
