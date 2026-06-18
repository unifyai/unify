"""Selection seam for the scheduled-activation materialiser.

The hosted Droid stack relies on Communication + Cloud Tasks to materialise
``Tasks/Activations`` rows into real timers and to deliver the wake event at
fire time. A local install has none of that infrastructure available and
needs an in-process equivalent.

To keep the two paths interchangeable, anything that wakes the conversation
manager for a scheduled / triggered task implements
:class:`ActivationMaterializer`. The factory :func:`build_materializer`
returns the right implementation for the current deployment based on
``SETTINGS.task.LOCAL_SCHEDULER_ENABLED``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from droid.conversation_manager.conversation_manager import ConversationManager


@runtime_checkable
class ActivationMaterializer(Protocol):
    """Produces ``TaskDue`` wake events when scheduled activations come due.

    The contract is intentionally minimal: a materialiser is a long-running
    background service that the conversation manager starts during init and
    stops on shutdown. Implementations decide whether they own a timer wheel
    in-process, watch an external scheduler, or do nothing at all.
    """

    async def start(self) -> None:
        """Start the materialiser. Idempotent — safe to call twice."""

    async def stop(self) -> None:
        """Stop the materialiser and release any background tasks. Idempotent."""


class NoopMaterializer:
    """Materialiser used in hosted deployments where Communication owns timing.

    Hosted Droid sessions receive ``task_due`` wake events through
    Communication's Pub/Sub + ``CommsManager`` ingress path. The in-process
    scheduler must not also fire timers — that would cause duplicate
    executions. ``NoopMaterializer`` provides the same lifecycle surface as
    :class:`LocalActivationScheduler` so the conversation manager start-up
    code can be uniform.
    """

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def build_materializer(
    cm: "ConversationManager",
) -> ActivationMaterializer:
    """Return the right materialiser for the current deployment.

    Local installs (``SETTINGS.task.LOCAL_SCHEDULER_ENABLED == True``) get
    a :class:`LocalActivationScheduler` bound to the conversation manager's
    event broker. Hosted deployments get a :class:`NoopMaterializer`.
    """

    from droid.settings import SETTINGS

    if SETTINGS.task.LOCAL_SCHEDULER_ENABLED:
        # Local import to avoid the circular dependency:
        # local_scheduler.__init__ → scheduler → machine_state → droid init,
        # while task_scheduler.settings is imported very early.
        from .scheduler import LocalActivationScheduler

        return LocalActivationScheduler(
            event_broker=cm.event_broker,
            poll_interval_seconds=SETTINGS.task.LOCAL_SCHEDULER_POLL_INTERVAL_SECONDS,
        )
    return NoopMaterializer()
