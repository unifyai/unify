"""Decisions that are pure functions of a request or a run.

Kept out of the manager so they can be reasoned about and tested without a backend,
and so the two that matter most -- which tier runs a request, and what a caller
should do next -- are stated in one place each rather than inferred from control flow
at several call sites.
"""

from __future__ import annotations

import os
from typing import List, Optional

from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types.request import IngestionRequest
from unify.ingestion_manager.types.run import StageProgress

Tier = str  # "inline" | "dispatched"


def _total_bytes(paths: List[str]) -> Optional[int]:
    """Total size of the paths that can be measured.

    Returns ``None`` when nothing could be stat'd, which is the honest answer for
    paths that are not on this filesystem yet -- the file count then decides alone
    rather than a fabricated zero making everything look small.
    """
    measured = 0
    seen = False
    for path in paths:
        try:
            measured += os.path.getsize(path)
            seen = True
        except OSError:
            continue
    return measured if seen else None


def choose_tier(
    request: IngestionRequest,
    settings: Optional[IngestionSettings] = None,
) -> Tier:
    """Decide whether a request runs in process or is dispatched.

    Decided entirely from measurable shape -- how many rows, how many files, how
    many bytes, whether the source is open-ended. Nothing here reads a description,
    a filename or anything else a person wrote, so the choice cannot be steered by
    phrasing.

    An explicit ``mode`` wins outright: the caller sometimes knows something the
    shape does not show, such as a small file that takes minutes to parse.
    """
    if request.mode in {"inline", "dispatched"}:
        return request.mode

    config = settings or IngestionSettings()
    source = request.source

    if source.kind == "rows":
        return "inline" if len(source.rows) <= config.MAX_INLINE_ROWS else "dispatched"

    if source.kind == "files":
        if len(source.paths) > config.MAX_INLINE_FILES:
            return "dispatched"
        measured = _total_bytes(source.paths)
        if measured is not None and measured > config.MAX_INLINE_BYTES:
            return "dispatched"
        return "inline"

    if source.kind == "folder":
        # A folder is a statement that the set is open-ended. Its size cannot be
        # known without walking it, and a plan should not be held open to find out,
        # so this is dispatch work by intent rather than by measurement.
        return "dispatched"

    # A stored table is read server-side in bounded pages, so the local cost is
    # small regardless of how many rows match.
    return "inline"


def next_step(
    *,
    state: str,
    parked: int,
    error: Optional[str],
    executed_as: Optional[str],
    contexts: List[str],
) -> str:
    """State the one action that makes sense for a run in this condition.

    Exists because a status a caller has to interpret will eventually be interpreted
    wrongly, and the cost of that is either a retry that duplicates data or a
    failure nobody notices. Every branch names a concrete next action, including
    that there is nothing to do.
    """
    if state == "queued":
        where = "remotely" if executed_as == "dispatched" else "in process"
        return f"Nothing yet -- this is queued to run {where}. Poll get_status again."

    if state == "running":
        return "Still running. Poll get_status again, or call wait() if the plan cannot continue without it."

    if state == "paused":
        return "Paused with work outstanding. Call resume() to continue it, or cancel() to abandon it."

    if state == "cancelled":
        return (
            "Cancelled, so the remaining work was abandoned. Anything already stored "
            "is still there. Submit again to redo the rest."
        )

    if state == "failed":
        if parked > 0:
            return (
                f"Failed with {parked} item(s) parked after exhausting their retries. "
                'Call retry(only="dlq") to re-attempt just those -- work that '
                "succeeded is left alone."
            )
        detail = f" The reported cause: {error}" if error else ""
        return (
            "Failed before anything could be parked, so a retry would fail the same "
            f"way. Read get_logs() to find the cause, fix it, then submit again.{detail}"
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
