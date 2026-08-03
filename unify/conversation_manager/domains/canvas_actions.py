"""Running the canvas actions viewers trigger.

A viewer presses a control on a canvas; Orchestra validates, records and publishes;
this executes the recorded run. The work itself lives in ``CanvasManager`` — the
three lanes are the function catalogue, the task scheduler and the actor, all of
which the assistant owns — so this module is only the bridge from the event to it.

Two properties shape the handler:

* **A failure here must not be silent.** The viewer is watching a control that says
  "working", so a run that cannot start has to be recorded as failed rather than
  left pending forever.
* **A redelivery must not run the work twice.** Delivery is at-least-once, and the
  guard lives in ``run_invocation`` rather than here, because a retry from any
  source needs the same protection.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from unify.conversation_manager.domains import comms_utils
from unify.conversation_manager.events import CanvasInvocationRequested

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager

LOGGER = logging.getLogger(__name__)


async def handle_canvas_invocation_requested(
    event: CanvasInvocationRequested,
    cm: "ConversationManager",
) -> bool:
    """Execute one recorded canvas action run.

    Returns whether the assistant should take a turn afterwards. It should not:
    the run is deterministic work with its outcome already written back to the
    invocation row and streamed to the canvas, so waking the slow brain would add
    a conversational turn nobody asked for. The ``assistant`` lane does involve the
    actor, but that happens inside the run rather than as a reaction to it.
    """
    from unify.manager_registry import ManagerRegistry

    canvas = ManagerRegistry.get_canvas_manager()

    try:
        # `run_invocation` is synchronous and blocks on a function execution or a
        # task trigger, so it goes to a worker thread rather than stalling the
        # event loop that is also serving the live conversation.
        record = await asyncio.to_thread(
            canvas.run_invocation,
            event.invocation_id,
            token=event.canvas_token,
        )
    except Exception:
        # Deliberately broad: whatever went wrong, the viewer is watching a control
        # that says "working" and the row must not stay pending. `run_invocation`
        # records its own failures, so reaching here means it could not even start
        # — a missing canvas, an unresolvable manager — and the log is the only
        # place that detail survives.
        LOGGER.exception(
            "Canvas invocation %s on %s could not be executed",
            event.invocation_id,
            event.canvas_token,
        )
        # The control still has to stop saying "working". A run that could not start
        # is a failure the viewer needs to see, not one to leave to the log.
        comms_utils.publish_canvas_invocation(
            token=event.canvas_token,
            invocation_id=event.invocation_id,
            action_name="",
            status="failed",
            error="This action could not be started.",
        )
        return False

    comms_utils.publish_canvas_invocation(
        token=event.canvas_token,
        invocation_id=event.invocation_id,
        action_name=record.action_name,
        status=record.status,
        error=record.error,
    )

    if record.status == "failed":
        LOGGER.warning(
            "Canvas action %r on %s failed: %s",
            record.action_name,
            event.canvas_token,
            record.error,
        )
    else:
        LOGGER.info(
            "Canvas action %r on %s finished with status %s",
            record.action_name,
            event.canvas_token,
            record.status,
        )

    return False
