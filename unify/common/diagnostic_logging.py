"""Helpers for verbose diagnostic logging, gated on the unify debug level."""

from __future__ import annotations

import logging


def staging_diagnostics_enabled() -> bool:
    """Whether verbose diagnostics should be emitted (unify logger at DEBUG)."""
    return logging.getLogger("unify").isEnabledFor(logging.DEBUG)
