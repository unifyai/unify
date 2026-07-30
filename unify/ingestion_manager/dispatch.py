"""Client for the pipeline control plane at ``/infra/pipeline/*``.

The assistant does not talk to the queue or the bucket directly. It posts a run
to the hosted control plane, which holds the cloud credentials and does the
upload and publish on its behalf.

That indirection is the point rather than an accident of layering. Handing every
assistant pod the credentials to publish to the parse topic and write to the
artifact bucket would make each one able to enqueue work for any tenant, and the
blast radius of a compromised pod would be the whole fleet. Posting a run to an
endpoint that authenticates the pod and derives the routing itself keeps that
authority in one service.

It also keeps the wire contract small: a run is a staged request key plus the
paths to process. Everything else the fleet needs is already in the artifact the
manager staged, which is the same artifact an in-process run reads -- so the two
tiers describe work identically and neither has a representation the other cannot
resume from.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from unify.ingestion_manager.types.request import IngestionRequest
from unify.ingestion_manager.types.run import RetryScope

logger = logging.getLogger(__name__)

# Long enough for the control plane to upload sources and publish, short enough
# that a wedged plane surfaces as an error rather than as a hung submit. The
# manager records the run before dispatching, so a timeout leaves something with
# an id rather than lost work.
_TIMEOUT_S = 120.0


def _post(base_url: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(
        f"{base_url.rstrip('/')}/infra/pipeline/{path.lstrip('/')}",
        json=payload,
        headers=_auth_headers(),
        timeout=_TIMEOUT_S,
    )
    response.raise_for_status()
    return response.json() if response.content else {}


def _get(base_url: str, path: str) -> Dict[str, Any]:
    response = requests.get(
        f"{base_url.rstrip('/')}/infra/pipeline/{path.lstrip('/')}",
        headers=_auth_headers(),
        timeout=_TIMEOUT_S,
    )
    response.raise_for_status()
    return response.json() if response.content else {}


def _auth_headers() -> Dict[str, str]:
    """Authenticate as this assistant, using its own key.

    Its own key rather than an admin one deliberately: the control plane scopes
    the dispatch to whoever asked, so a pod cannot enqueue or inspect work
    belonging to another tenant even if it tries.
    """
    from unify.session_details import SESSION_DETAILS

    key = getattr(SESSION_DETAILS, "api_key", "") or ""
    if not key:
        raise RuntimeError(
            "No assistant key available to authenticate a pipeline dispatch. "
            "The control plane scopes work to the caller, so an unauthenticated "
            "dispatch cannot be routed.",
        )
    return {"Authorization": f"Bearer {key}"}


def dispatch_run(
    *,
    base_url: str,
    run_key: str,
    request: IngestionRequest,
    request_key: str,
    paths: List[str],
) -> str:
    """Hand a run to the fleet and return the dispatch id that tracks it.

    ``run_key`` is passed through as the job identity rather than letting the
    control plane mint one, so the checkpoints and leases the fleet writes land
    under the same job the manager already recorded. That shared identity is what
    makes a dispatched run resumable in process and an in-process run adoptable
    by the fleet.

    One message per file: the fleet parallelises by message, so a single message
    naming many files would parse them all on one pod and lose the scaling the
    dispatch was chosen for.
    """
    payload = {
        "run_key": run_key,
        "request_key": request_key,
        "paths": list(paths),
        "source_kind": request.source.kind,
        "target_kind": request.target.kind,
        "destination": request.destination,
    }
    body = _post(base_url, "submit", payload)
    dispatch_id = str(body.get("dispatch_id") or run_key)
    logger.info(
        "Dispatched run %s to the fleet as %s (%d file(s))",
        run_key,
        dispatch_id,
        len(paths),
    )
    return dispatch_id


def fetch_status(*, base_url: str, dispatch_id: str) -> Dict[str, Any]:
    """Read the fleet's view of a dispatch.

    Advisory only. The run's own state and its checkpoints are the record; this
    answers "is anything still working on it", which a checkpoint cannot -- a
    stalled run and a slow one look identical from progress alone.
    """
    return _get(base_url, f"status/{dispatch_id}")


def request_retry(
    *,
    base_url: str,
    dispatch_id: str,
    scope: RetryScope,
) -> Dict[str, Any]:
    """Ask the fleet to re-attempt part of a dispatch.

    Serialised by the control plane rather than here. Two recovery paths
    publishing at once -- a retry and a stale-recovery on the same table -- put
    two live attempts against one lease, and the loser's writes freeze the
    checkpoint: the run then under-ingests without reporting anything. One owner
    of the transition is what removes that race, instead of a flag warning
    operators not to cause it.
    """
    return _post(base_url, "retry", {"dispatch_id": dispatch_id, "scope": scope})


def request_cancel(*, base_url: str, dispatch_id: str) -> Dict[str, Any]:
    """Abandon a dispatch's remaining work, keeping what already committed."""
    return _post(base_url, "cancel", {"dispatch_id": dispatch_id})


def request_pause(*, base_url: str, dispatch_id: str) -> Dict[str, Any]:
    """Stop a dispatch but keep its outstanding work.

    In-flight messages are parked rather than dropped, so the queue drains and
    the fleet can scale down while nothing is lost. Resume replays them.
    """
    return _post(base_url, "pause", {"dispatch_id": dispatch_id})


def request_resume(*, base_url: str, dispatch_id: str) -> Dict[str, Any]:
    """Replay a paused dispatch's parked work, in its original order."""
    return _post(base_url, "resume", {"dispatch_id": dispatch_id})


def probe(*, base_url: Optional[str]) -> bool:
    """Report whether a control plane is configured and answering.

    Used to decide whether the fleet is reachable at all. A deployment without
    one runs everything in process, which is safe because both tiers write the
    same layout -- a fleet configured later adopts whatever was left behind.
    """
    if not base_url:
        return False
    try:
        _get(base_url, "health")
        return True
    except Exception:
        logger.warning("Pipeline control plane at %s is not answering", base_url)
        return False
