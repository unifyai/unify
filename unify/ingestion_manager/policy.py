"""Decisions that are pure functions of a request or a run.

Kept out of the manager so they can be reasoned about and tested without a
backend, and so the two that matter most -- where a request runs, and what a
caller should do next -- are stated once each rather than inferred from control
flow at several call sites.
"""

from __future__ import annotations

from typing import List, Literal, Optional

from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types.request import IngestionRequest
from unify.ingestion_manager.types.run import StageProgress

Tier = Literal["inline", "dispatched"]


def choose_tier(
    request: IngestionRequest,
    settings: Optional[IngestionSettings] = None,
    *,
    row_count: Optional[int] = None,
    has_fleet: Optional[bool] = None,
) -> Tier:
    """Decide where a request runs. Deterministic, and never a caller's choice.

    Two rules, and the reason they differ is the whole design:

    **Files always dispatch.** Parsing loads the file and its model into the
    process that does it, and a thread does not change that -- threads share one
    address space and one memory limit, so a parse that overruns takes down the
    assistant sharing the process, not just the ingestion. There is also no
    number that predicts the risk: bytes and file count say nothing about page
    count or density. So the answer cannot be a threshold; it has to be a
    boundary, and the boundary is the process.

    **Rows and tables dispatch above a measured ceiling.** Here a count is
    available exactly and cheaply before anything runs, so the decision rests on
    a measurement rather than a guess. Under the ceiling, queue round-trip and
    cold start cost more than the work; over it, ingestion is sustained I/O that
    scales out on the fleet and would otherwise compete with the assistant for
    its own process.

    ``row_count`` is the exact count: ``len(rows)`` for rows in hand, or one
    server-side aggregate for a stored table. Passing ``None`` for a table source
    means it has not been counted yet, and an uncounted table dispatches -- an
    unknown size is not evidence of a small one.

    Both tiers write the same artifacts and the same checkpoints, so choosing
    dispatch for work that would have been quick costs latency, and nothing else:
    the run is still resumable, still recoverable, and still asked about the same
    way. That asymmetry is why the ceiling can sit low without risk.
    """
    config = settings or IngestionSettings()
    fleet = bool(config.PIPELINE_URL) if has_fleet is None else has_fleet

    # No fleet reachable: in process is the only tier there is. Safe rather than
    # merely tolerable -- the artifacts and checkpoints land in the layout a
    # fleet reads, so one configured later can adopt whatever was left behind.
    if not fleet:
        return "inline"

    source = request.source

    if source.kind in {"files", "folder"}:
        return "dispatched"

    counted = len(source.rows) if source.kind == "rows" else row_count
    if counted is None:
        return "dispatched"
    return "inline" if counted <= config.MAX_INLINE_ROWS else "dispatched"


def next_step(
    *,
    state: str,
    parked: int,
    error: Optional[str],
    executed_as: Optional[str],
    contexts: List[str],
) -> str:
    """State the one action that makes sense for a run in this condition.

    Exists because a status a caller has to interpret will eventually be
    interpreted wrongly, and the cost of that is either a retry that duplicates
    data or a failure nobody notices. Every branch names a concrete next action,
    including that there is nothing to do.
    """
    if state == "queued":
        where = "on the worker fleet" if executed_as == "dispatched" else "in process"
        return f"Nothing yet -- this is queued to run {where}. Poll get_status again."

    if state == "running":
        return (
            "Still running. Poll get_status again, or call wait() if the plan "
            "cannot continue without the data."
        )

    if state == "paused":
        return (
            "Paused with work outstanding. Call resume() to continue from the "
            "last checkpoint, or cancel() to abandon it."
        )

    if state == "cancelled":
        return (
            "Cancelled, so the remaining work was abandoned. Anything already "
            "committed is still there. Submit again to redo the rest."
        )

    if state == "failed":
        if parked > 0:
            return (
                f"Failed with {parked} item(s) parked after exhausting their "
                'retries. Call retry(only="dlq") to re-attempt just those -- work '
                "that already committed is left alone."
            )
        detail = f" The reported cause: {error}" if error else ""
        return (
            "Failed before anything could be parked, so a retry would fail the "
            f"same way. Read get_logs() to find the cause, fix it, then submit "
            f"again.{detail}"
        )

    if state == "succeeded":
        if parked > 0:
            # Worth flagging loudly: a run that finished with parked items looks
            # successful in a listing, and the missing rows are easy to miss.
            return (
                f"Finished, but {parked} item(s) were parked and are not in the "
                'result. Call retry(only="dlq") to bring them in.'
            )
        if contexts:
            return f"Nothing -- this finished. The data is in {', '.join(contexts)}."
        return "Nothing -- this finished."

    return "Unrecognised state; read get_logs() to see what happened."


def stages_from_events(events: List[dict]) -> List[StageProgress]:
    """Fold recorded events into current per-stage progress.

    Progress is derived rather than stored so there is one source of truth. A
    separately maintained counter drifts from the events the moment a worker dies
    between incrementing it and recording why.
    """
    ordered: dict[str, StageProgress] = {}
    for event in events:
        stage = event.get("stage")
        if not stage:
            continue
        current = ordered.get(stage)
        if current is None:
            current = StageProgress(stage=stage, state="running")
            ordered[stage] = current
        state = event.get("state")
        if state:
            current.state = state
        done = event.get("done")
        if isinstance(done, int):
            current.done = done
        total = event.get("total")
        if isinstance(total, int):
            current.total = total
        if event.get("level") == "error" and event.get("message"):
            current.error = event["message"]
    return list(ordered.values())
