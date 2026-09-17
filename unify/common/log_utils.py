"""
Wrappers around db.log/create_logs with:
1. _user injection (user ID, matches user_context path component)
2. _user_id injection (user ID from SESSION_DETAILS)
3. _assistant injection (assistant ID, matches assistant_context path component)
4. _assistant_id injection (assistant's agent_id from SESSION_DETAILS.assistant.agent_id)
5. Automatic addition to aggregation contexts by reference (copy=False)

Usage
-----
Replace direct db.log/create_logs calls with these wrappers:

    from unify.common.log_utils import log, create_logs

    # Instead of: db.log(context=ctx, **entries)
    log(context=ctx, **entries)

    # Instead of: db.create_logs(context=ctx, entries=entries_list)
    create_logs(context=ctx, entries=entries_list)

The wrappers automatically inject _user, _user_id, _assistant and
_assistant_id as private fields.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from unify import db
from unify.session_details import SESSION_DETAILS

logger = logging.getLogger(__name__)


def _get_user_context() -> Optional[str]:
    """Retrieve user's context path component (user ID) from SESSION_DETAILS.

    Injected as _user into every log entry. Matches the user_id path segment
    in context paths like {user_id}/{assistant_id}/Contacts.
    Needed by the store's deletion cascade.
    """
    return SESSION_DETAILS.user_context or None


def _get_user_id() -> Optional[str]:
    """Retrieve user_id from SESSION_DETAILS."""
    return SESSION_DETAILS.user.id or None


def _get_assistant_context() -> Optional[str]:
    """Retrieve assistant's context path component (assistant ID) from SESSION_DETAILS.

    Injected as _assistant into every log entry. Matches the assistant_id path
    segment in context paths like {user_id}/{assistant_id}/Contacts.
    Needed by the store's deletion cascade.
    """
    return SESSION_DETAILS.assistant_context or None


def _get_assistant_id() -> Optional[str]:
    """Retrieve assistant's agent_id from SESSION_DETAILS as a string."""
    aid = SESSION_DETAILS.assistant.agent_id
    return str(aid) if aid is not None else None


def _inject_private_fields(entries: Dict[str, Any]) -> Dict[str, Any]:
    """Inject _user, _user_id, _assistant and _assistant_id into entries."""
    result = dict(entries)

    user_ctx = _get_user_context()
    if user_ctx is not None:
        result["_user"] = user_ctx

    user_id = _get_user_id()
    if user_id is not None:
        result["_user_id"] = user_id

    assistant_ctx = _get_assistant_context()
    if assistant_ctx is not None:
        result["_assistant"] = assistant_ctx

    assistant_id = _get_assistant_id()
    if assistant_id is not None:
        result["_assistant_id"] = assistant_id

    return result


class MissingRowIdentityError(RuntimeError):
    """A created row came back without its auto-counted identity column."""


def assigned_row_id(log: db.Log, column: str, *, context: str) -> int:
    """Return the identity value the backend assigned to a freshly created row.

    A context configured with unique-key auto-counting stamps *column* on
    every insert and echoes it back on the created log. A missing echo means
    the context is live without that configuration — it was created
    implicitly by a row write reaching the backend before provisioning — so
    the row can never be addressed by id. The orphan row is deleted so junk
    does not accumulate, and the corruption is raised to the caller: no API
    can retrofit the configuration, so the context must be deleted and
    re-provisioned before further writes.
    """
    value = log.entries.get(column)
    if value is not None:
        return int(value)
    db.delete_logs(logs=log.id, context=context)
    raise MissingRowIdentityError(
        f"Context {context!r} accepted a row without assigning {column!r}: "
        "the context is live without its unique-key/auto-counting "
        "configuration, so rows written to it cannot be addressed by id. "
        "The orphan row was deleted. Delete and re-provision the context "
        "before writing again.",
    )


# EventBus metadata columns stored alongside an event's spread payload fields.
_EVENT_META_KEYS = frozenset(
    {"row_id", "event_id", "calling_id", "event_timestamp", "payload_cls", "type"},
)


def payload_from_log_entries(entries: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct an event payload from a spread event log row.

    Events are stored in their per-type context with payload fields spread into
    top-level columns. This drops the EventBus metadata columns and the injected
    private fields (all underscore-prefixed, see :func:`_inject_private_fields`)
    so the result matches the payload dict that was originally published.
    """
    return {
        key: value
        for key, value in entries.items()
        if key not in _EVENT_META_KEYS and not key.startswith("_")
    }


def log(
    context: str,
    *,
    new: bool = True,
    mutable: bool = False,
    project: Optional[str] = None,
    **entries: Any,
) -> db.Log:
    """
    Wrapper around db.log with private field injection.

    Parameters
    ----------
    context : str
        The context to log to (e.g., "42/7/Contacts")
    new : bool, default True
        Whether to create a new log entry
    mutable : bool, default False
        Whether the log entry is mutable
    **entries
        Field values to log

    Returns
    -------
    db.Log
        The created log object
    """
    entries = _inject_private_fields(entries)
    return db.log(
        project=project,
        context=context,
        new=new,
        mutable=mutable,
        **entries,
    )


def create_logs(
    context: str,
    *,
    entries: List[Dict[str, Any]],
    project: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Wrapper around db.create_logs with private field injection.

    Parameters
    ----------
    context : str
        The context to log to (e.g., "42/7/Tasks")
    entries : List[Dict[str, Any]]
        List of entry dicts to create
    **kwargs
        Additional arguments passed to db.create_logs (e.g. ``on_duplicate``)

    Returns
    -------
    List[db.Log]
        The created rows, with ids and auto-counted keys filled in.
    """
    entries = [_inject_private_fields(dict(entry)) for entry in entries]
    return db.create_logs(
        project=project,
        context=context,
        entries=entries,
        **kwargs,
    )
