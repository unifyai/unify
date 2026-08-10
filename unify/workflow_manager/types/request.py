"""Pydantic model for the ``Workflows/Requests`` context.

A request is one recorded intent to **change install state** — install,
uninstall, upgrade, or save settings — written by a surface that cannot
reconcile on its own. Console is the caller: planting content needs the
custom-sync engine, which is the assistant's, and a hosted assistant is
an on-demand job that is usually asleep when someone clicks Install. So
the click writes a durable row and the assistant executes it on its next
wake; the wake dispatch is best-effort, and a sweep catches whatever a
missed dispatch left pending.

**This is not a workflow runtime.** The boundary invariant (a workflow has
no executions, no steering, no run history) is about the workflow *doing
its job* — that work belongs to the tasks it plants, which own their
schedules, executions and handles. A request row is setup changing: its
``status`` describes one reconcile pass, there is no schedule, no retry
policy, no occurrence rows, and nothing here is steerable. Reading these
rows tells you what the user asked the installation to become, never what
the workflow did.

Rows are ordinary tenant data in the assistant's own contexts, like the
installations beside them.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from unify.common.authorship import AuthoredRow

ACTIONS = ("install", "uninstall", "update", "save_params")
"""Every mutation a reading surface may ask for.

``update`` is ``install`` re-run against the current bundle (the reconcile
entrypoint), and ``save_params`` writes settings without replanting.
Anything not in this tuple is refused when the request is claimed, so a
typo fails one row loudly instead of doing something approximate.
"""

TERMINAL_STATUSES = ("succeeded", "failed")


class WorkflowRequest(AuthoredRow):
    """One requested change to a workflow's install state."""

    SHORTHAND_MAP: ClassVar[dict[str, str]] = {
        "request_id": "rid",
        "slug": "s",
        "action": "a",
        "params": "p",
        "destination": "dst",
        "status": "st",
        "claim_key": "ck",
        "claimed_at": "cat",
        "settled_at": "sat",
        "error": "e",
        "outcome": "o",
    }

    request_id: str = Field(
        description=(
            "Caller-minted idempotency key and the row's identity. The "
            "writer generates it, so a retried write converges on one row "
            "rather than queueing the same install twice."
        ),
    )
    slug: str = Field(description="Bundle the request acts on.")
    action: str = Field(
        description=(
            "One of 'install', 'uninstall', 'update', 'save_params'. "
            "'update' is an install re-run against the current bundle."
        ),
    )
    params: str = Field(
        default="{}",
        description=(
            "JSON object of settings for the actions that take them. An "
            "empty object on install means 'keep whatever is recorded', "
            "matching install_workflow's own contract."
        ),
    )
    destination: str = Field(
        default="personal",
        description="Root the request targets, personal or team:<id>.",
    )
    status: str = Field(
        default="pending",
        description=(
            "'pending' until claimed, 'running' while the reconcile is in "
            "flight, then 'succeeded' or 'failed'. Describes the reconcile "
            "pass, never the workflow's own work."
        ),
    )
    claim_key: str = Field(
        default="",
        description=(
            "Identity of the executor holding this row. Written atomically "
            "so two concurrent executors cannot both run one request; the "
            "loser sees the winner's key and moves on."
        ),
    )
    claimed_at: str = Field(
        default="",
        description=(
            "ISO-8601 UTC time the claim was taken. A claim older than the "
            "reclaim window is treated as abandoned, so an executor that "
            "died mid-pass does not strand the request forever."
        ),
    )
    settled_at: str = Field(
        default="",
        description="ISO-8601 UTC time the request reached a terminal status.",
    )
    error: str = Field(
        default="{}",
        description=(
            "JSON object of what failed, keyed by surface where the failure "
            "was per-surface — the same shape the installation's 'partial' "
            "status reports — or {'error': reason} for a whole-request "
            "failure."
        ),
    )
    outcome: str = Field(
        default="{}",
        description=(
            "JSON summary of what the reconcile produced, so a reading "
            "surface can render the result without a second call: which "
            "surfaces were planted, whether jobs armed or were held, and "
            "the connect_required envelope when requirements are unmet."
        ),
    )

    @classmethod
    def shorthand_map(cls) -> dict[str, str]:
        return dict(cls.SHORTHAND_MAP)

    @classmethod
    def shorthand_inverse_map(cls) -> dict[str, str]:
        return {v: k for k, v in cls.SHORTHAND_MAP.items()}
