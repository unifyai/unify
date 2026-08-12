"""Locating the pod-local broker sidecar from the runtime's environment.

The sidecar holds the provider credentials; callers that used to read a key from
the environment instead route through it over loopback. They all need the same
answer -- where is it, and is there one at all -- so it lives here rather than
being re-derived at each call site.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse


def broker_origin() -> str | None:
    """``scheme://host:port`` of the broker sidecar, or ``None`` when none is set.

    Derived from ``UNILLM_LLM_GATEWAY_URL`` -- the LLM leg's base, which every
    pod running the sidecar sets -- so a second variable cannot drift from it.
    ``None`` in self-host / local dev, where callers keep their own env-key
    behaviour. Also ``None`` without ``UNIFY_KEY``, since that is the nonce a
    caller presents to the sidecar and there is nothing to authenticate without
    it.
    """
    gateway = os.environ.get("UNILLM_LLM_GATEWAY_URL", "").strip()
    if not gateway or not os.environ.get("UNIFY_KEY", "").strip():
        return None
    parsed = urlparse(gateway)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"
