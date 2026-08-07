"""Carrying out the workflow install-state changes a reading surface recorded.

Someone clicks Install in Console; Console records the intent as a row and
Orchestra publishes; this performs it. The work itself lives in
``WorkflowManager`` — planting content is a fan-out over the custom-sync
engine, which only the assistant has — so this module is just the bridge from
the event to it.

Two properties shape the handler:

* **The dispatch is an optimisation, not the mechanism.** The row is durable
  and the boot sweep drains the same queue, so losing this event costs
  latency and nothing else. That is why the handler needs no retry of its own.
* **A redelivery must not install twice.** The guard is the atomic claim inside
  ``execute_requests``, not anything here, because the boot sweep and a
  redelivered event race exactly the same way.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from unify.conversation_manager.events import WorkflowRequestRequested

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager

LOGGER = logging.getLogger(__name__)


async def handle_workflow_request_requested(
    event: WorkflowRequestRequested,
    cm: "ConversationManager",
) -> bool:
    """Drain the recorded install-state requests. Returns False: no LLM turn.

    Deliberately drains the queue rather than executing only the request the
    event names. There is one entrypoint either way — the claim makes a
    concurrent drain safe — and draining means a request whose own dispatch was
    lost is picked up by the next one instead of waiting for a reboot.

    No turn is taken afterwards because a reconcile is deterministic work whose
    outcome is written to the request row that Console is already reading;
    waking the slow brain would add a conversational turn nobody asked for.
    """
    from unify.manager_registry import ManagerRegistry

    manager = ManagerRegistry.get_workflow_manager()
    if manager is None:
        # No shelf in this deployment, so nothing could have been requested
        # against one. Recorded rather than raised: the request row stays for
        # a deployment that does have the feature.
        LOGGER.warning(
            "Workflow request %s arrived but this deployment has no workflow "
            "catalogue; leaving it recorded",
            event.request_id,
        )
        return False

    destination = None if event.destination == "personal" else event.destination

    try:
        # `execute_requests` is synchronous and blocks on the whole fan-out
        # across surfaces, so it goes to a worker thread rather than stalling
        # the event loop that is also serving the live conversation.
        report = await asyncio.to_thread(
            manager.execute_requests,
            destination=destination,
        )
    except Exception:
        # Deliberately broad. Per-request failures are already recorded on
        # their own rows, so reaching here means the pass itself could not run
        # — an unresolvable context, a backend that is down. The rows stay
        # pending and the boot sweep retries them, which is the whole reason
        # the row is the mechanism and this is not.
        LOGGER.exception(
            "Workflow request pass triggered by %s could not run; the rows "
            "stay pending for the next sweep",
            event.request_id,
        )
        return False

    settled = report.get("settled") or {}
    if settled:
        LOGGER.info("Workflow requests settled: %s", settled)
    else:
        # Normal, not a fault: the boot sweep or a prior delivery already took
        # this request, and the claim let exactly one of them have it.
        LOGGER.info(
            "Workflow request %s was already settled by another pass",
            event.request_id,
        )
    return False
