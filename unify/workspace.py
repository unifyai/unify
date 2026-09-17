"""The assistant's workspace on the local filesystem."""

from __future__ import annotations

from pathlib import Path


def get_local_root() -> str:
    """Return the resolved workspace directory.

    Uses ``SETTINGS.UNIFY_LOCAL_ROOT`` when set, otherwise the ``workspace``
    directory under the store home (``UNIFY_HOME``, default ``~/.unify``).
    Every path the assistant works with resolves through this function.
    """
    from unify.db.engine import store_home
    from unify.settings import SETTINGS

    explicit = SETTINGS.UNIFY_LOCAL_ROOT.strip()
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    return str(store_home() / "workspace")
