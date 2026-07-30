"""What an ingestion reports back: one run, its progress, and what to do next.

Every ingestion is a **run** with an id, whichever tier executed it. That is the
point of the design: an inline run and a dispatched cloud run are asked about the
same way, so a plan that submits work does not have to know where it went in order
to find out how it ended.

Runs and their events are ordinary rows in contexts this manager owns, read through
the same logging path as every other catalogue. There is deliberately no separate
ledger machinery, no cost accounting and no bespoke progress files: a run's history
is rows, queryable like anything else. The data and files themselves still go to
object storage, which is what lets a parse worker and an ingest worker hand off
remotely -- but observing the run does not.

Nothing here holds bulk data. Row payloads are staged as artifacts and referenced
by key, so a run row stays small however large the ingestion: a single row
carrying an embedded payload is both a write the backend can reject outright and a
read that costs more than the answer is worth.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from unify.common.authorship import AuthoredRow

# Lifecycle of a run. `paused` is distinct from `cancelled`: a paused run keeps its
# queued work and can be resumed, a cancelled one cannot.
RunState = Literal[
    "queued",
    "running",
    "paused",
    "succeeded",
    "failed",
    "cancelled",
]

# Terminal states, past which polling learns nothing further.
TERMINAL_STATES: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})

# What a retry should re-attempt.
#
#   dlq    -- only the items that were parked after exhausting their retries.
#             The common case, and the safe one: nothing that already succeeded
#             is touched.
#   stale  -- items claimed by a worker that then stopped reporting. Recovers a
#             crashed worker's share without re-running healthy work.
#   all    -- every item, including ones that succeeded. Only when the target was
#             emptied or the parse changed.
RetryScope = Literal["dlq", "stale", "all"]


class StageProgress(BaseModel):
    """How one stage of a run is doing.

    Stages differ by source -- a folder of spreadsheets parses before it ingests,
    a list of rows only ingests -- so this is a list rather than a fixed shape.
    """

    model_config = ConfigDict(extra="forbid")

    stage: str = Field(description="Stage name, e.g. 'parse', 'ingest', 'embed'.")
    state: RunState
    done: int = Field(default=0, description="Items finished.")
    total: Optional[int] = Field(
        default=None,
        description="Items expected, when knowable up front.",
    )
    error: Optional[str] = None


class LogEntry(BaseModel):
    """One recorded event from a run."""

    model_config = ConfigDict(extra="forbid")

    at: str = Field(description="ISO timestamp.")
    stage: Optional[str] = None
    level: Literal["info", "warning", "error"] = "info"
    message: str


class IngestionEventRow(AuthoredRow):
    """Row stored in the ``Ingestion/Events`` context.

    One append-only line per thing that happened. Stage progress is folded from
    these rather than kept as a separate counter, so there is one source of truth: a
    counter maintained alongside events drifts the moment a worker dies between
    incrementing it and recording why.
    """

    model_config = ConfigDict(extra="forbid")

    run_key: str
    at: str
    stage: Optional[str] = None
    level: Literal["info", "warning", "error"] = "info"
    message: str = ""
    # Progress carried on the event so a reader can reconstruct where a stage got
    # to without replaying the work.
    state: Optional[str] = None
    done: Optional[int] = None
    total: Optional[int] = None


class IngestionRunRecord(AuthoredRow):
    """Row stored in the ``Ingestion/Runs`` context.

    ``run_id`` is deliberately absent: the backend auto-counts it, and including
    it here would let a caller try to set it.
    """

    model_config = ConfigDict(extra="forbid")

    # Stable handle the actor holds and polls with. Distinct from the auto-counted
    # row id so it can be minted before the row exists and survive a re-run.
    run_key: str

    state: RunState = "queued"
    # Which tier ran it. Recorded because "why did this take an hour" is usually
    # answered by it, and because a resume has to know where to look.
    executed_as: Optional[Literal["inline", "dispatched"]] = None

    source_kind: str = ""
    target_kind: str = ""

    # Artifact key of the staged request, so a retry or resume can rebuild the
    # work without the caller reconstructing it. A *key*, never the request
    # itself: a rows source can be arbitrarily large, and embedding it here would
    # put bulk data in a log row.
    request_key: str = ""
    # Exact size once measured -- the count the tier decision was made on, and
    # what the completion check holds the durable checkpoint against.
    declared_rows: Optional[int] = None

    # Concrete context paths this run wrote. The reason a canvas can be built over
    # ingested files without anyone hardcoding the storage layout: the run says
    # where its output went.
    contexts: List[str] = Field(default_factory=list)
    rows_written: int = 0
    files_processed: int = 0

    stages_json: Optional[str] = None
    error: Optional[str] = None
    # Items parked after exhausting retries -- the depth `retry(only="dlq")` clears.
    parked: int = 0

    # Set when the dispatched tier ran it, for correlating with cloud-side records.
    dispatch_id: Optional[str] = None

    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class IngestionRun(BaseModel):
    """Handle returned by ``submit``, before the work has necessarily finished.

    ``submit`` does not block, so this is what a plan carries forward. Poll it with
    ``get_status``, or hand it to ``wait`` when the plan genuinely cannot continue
    without the data.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    state: RunState
    executed_as: Optional[Literal["inline", "dispatched"]] = None
    # Populated once known; empty while a run is still queued.
    contexts: List[str] = Field(default_factory=list)


class RunStatus(BaseModel):
    """Everything worth knowing about a run, and what to do about it.

    ``next_step`` exists because a status a caller has to interpret will eventually
    be interpreted wrongly. It states the one action that makes sense now -- keep
    waiting, retry the parked items, fix the target, or nothing -- so recovery does
    not depend on the reader reconstructing the rule from the other fields.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    state: RunState
    executed_as: Optional[Literal["inline", "dispatched"]] = None

    stages: List[StageProgress] = Field(default_factory=list)
    contexts: List[str] = Field(default_factory=list)
    rows_written: int = 0
    files_processed: int = 0
    parked: int = 0
    error: Optional[str] = None

    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    next_step: str = Field(
        description=(
            "The single action that makes sense now, in plain words -- including "
            "'nothing, this finished' when that is the answer."
        ),
    )

    @property
    def is_terminal(self) -> bool:
        """Whether polling again could tell you anything new."""
        return self.state in TERMINAL_STATES


class RetryResult(BaseModel):
    """Outcome of asking for a retry."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    scope: str
    # Zero is a meaningful answer, not a failure: it means there was nothing in
    # that scope to retry.
    requeued: int = 0
    state: RunState = "queued"
    detail: Optional[str] = None


class IngestionSummary(BaseModel):
    """One run as it appears in a listing."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    state: RunState
    source_kind: str = ""
    target_kind: str = ""
    contexts: List[str] = Field(default_factory=list)
    rows_written: int = 0
    parked: int = 0
    created_at: Optional[str] = None
    finished_at: Optional[str] = None


def stage_list(stages: Optional[Dict[str, Any]]) -> List[StageProgress]:
    """Coerce a stored stage map into ordered progress entries."""
    if not stages:
        return []
    return [StageProgress.model_validate(entry) for entry in stages.values()]
