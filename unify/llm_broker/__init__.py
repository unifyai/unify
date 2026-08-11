"""Pod-local LLM broker: the provider key's container, not the runtime's.

Runs as a sidecar beside the assistant runtime. The provider credentials are
mounted here and nowhere else in the pod, so the container that executes user
code holds none. Containers in a pod share a network namespace but not a PID
namespace, so in-process sandbox code can reach this broker over loopback and
have it make a call, but cannot read the key it makes the call with -- it has
no access to this process's environment or memory. That distinction is the
whole point: the incident this exists to prevent was a raw key being lifted
out of ``os.environ`` and spent off-platform, not calls being made on-platform.

Bytes go pod -> loopback -> provider and never traverse Orchestra. Orchestra
is consulted only for metadata: may this call proceed, and what did it cost.
Proxying the bytes instead would hold one of Orchestra's request slots for the
length of a generation, putting LLM volume in contention with billing, logs
and search, and adding a network hop to every voice turn.

What the broker does *not* do is decide what anything costs. It reports the
provider's own usage object verbatim and lets Orchestra price it, so the side
holding the ledger stays the side that sets prices.
"""

from unify.llm_broker.app import build_app

__all__ = ["build_app"]
