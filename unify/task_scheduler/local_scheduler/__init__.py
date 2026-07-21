"""In-process activation scheduler for local Unity installs.

Replaces Communication's Cloud Tasks + Kubernetes path when the user runs
Unity locally (bundled Orchestra in Docker, no Communication gateway). The
in-process scheduler watches the same Orchestra-projected ``Tasks/Executions``
rows the hosted path watches, but fires due tasks directly on the event
broker instead of via a chain of HTTP hops.

Public surface:

- :class:`ActivationMaterializer` — the Protocol every scheduler implements.
- :class:`LocalActivationScheduler` — the in-process, asyncio-timer
  implementation used by local installs.
- :class:`NoopMaterializer` — a do-nothing implementation used in hosted
  mode where Communication owns materialisation.
- :func:`build_materializer` — selects the right implementation based on
  ``SETTINGS.task.LOCAL_SCHEDULER_ENABLED``.
"""

from .materializer import (
    ActivationMaterializer,
    NoopMaterializer,
    build_materializer,
)
from .offline_dispatcher import LocalOfflineDispatcher
from .scheduler import LocalActivationScheduler

__all__ = [
    "ActivationMaterializer",
    "LocalActivationScheduler",
    "LocalOfflineDispatcher",
    "NoopMaterializer",
    "build_materializer",
]
