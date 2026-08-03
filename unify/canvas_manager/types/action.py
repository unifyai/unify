"""Canvas actions: the write path from a rendered canvas back into unify.

An action is a named, pre-declared piece of work a viewer can trigger from the
canvas — running a stored function, triggering a task, or handing a request to
the assistant. It is what turns a canvas from a report into an application.

The security model is that the frame names an action and supplies arguments,
and nothing else. The dispatch target lives on the stored record and never
crosses the boundary, and arguments are re-validated server-side against the
declared schema, because client-side validation in an untrusted frame is a
usability feature rather than a control.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from unify.common.authorship import AuthoredRow

# How the result is surfaced once the work completes.
ResultMode = Literal["fire_and_forget", "show_result", "stream"]

# Which lane executes the work.
#   function  — a stored FunctionManager function, via the offline runner
#   task      — an existing durable task, via the task trigger route
#   assistant — hand the request to the actor, for when no stored function fits
ActionKind = Literal["function", "task", "assistant"]

ACTION_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"


class CanvasAction(BaseModel):
    """One declared action on a canvas.

    Exactly one dispatch target must be given: ``function_id``,
    ``function_name``, ``implementation``, ``task_id`` or ``request``.

    Parameters
    ----------
    name : str
        Stable identifier the canvas invokes. Lowercase snake case.
    label : str
        Human-readable text for the control. Shown to the viewer, so it must
        describe what the action actually does.
    kind : "function" | "task" | "assistant"
        Which execution lane handles it.
    input_schema : dict | None
        JSON Schema for viewer-supplied arguments. Every array and string in it
        must carry ``maxItems`` / ``maxLength``: those bounds are the blast
        radius of the action, and they are enforced server-side on every
        invocation.
    confirm : str | None
        Text shown in the confirmation dialog. Required when ``destructive``.
        Rendered by console *outside* the frame, alongside the actual arguments
        being submitted, so a canvas can neither suppress nor fake it.
    destructive : bool
        Whether this sends, deletes, spends or otherwise cannot be undone.
    max_invocations_per_hour : int
        Per-canvas, per-user rate limit.
    """

    name: str = Field(pattern=ACTION_NAME_PATTERN)
    label: str
    icon: Optional[str] = None
    kind: ActionKind = "function"

    function_id: Optional[int] = None
    function_name: Optional[str] = None
    implementation: Optional[str] = None
    task_id: Optional[int] = None
    request: Optional[str] = None

    input_schema: Optional[dict] = None
    confirm: Optional[str] = None
    destructive: bool = False
    result_mode: ResultMode = "show_result"
    max_invocations_per_hour: int = Field(default=20, ge=1, le=1000)


class CanvasActionRow(AuthoredRow):
    """Row stored in the ``Canvas/Actions`` context."""

    model_config = ConfigDict(extra="forbid")

    canvas_token: str
    action_name: str
    label: str
    icon: Optional[str] = None
    kind: str = "function"

    # Resolved at author time to exactly one concrete target.
    function_id: Optional[int] = None
    task_id: Optional[int] = None
    request: Optional[str] = None

    input_schema_json: Optional[str] = None
    confirm: Optional[str] = None
    destructive: bool = False
    result_mode: str = "show_result"
    max_invocations_per_hour: int = 20

    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class CanvasInvocationRow(AuthoredRow):
    """Row stored in the ``Canvas/Invocations`` context.

    The durable carrier for one action run. It exists because neither available
    dispatch lane can take arguments: the task trigger route accepts no payload,
    and the offline runner's environment carries an entrypoint but no argument
    channel. Persisting the arguments solves that, and in doing so also provides
    idempotency, progress reporting, result delivery and an audit trail.
    """

    model_config = ConfigDict(extra="forbid")

    canvas_token: str
    action_name: str
    args_json: Optional[str] = None

    status: str = "pending"
    progress_json: Optional[str] = None
    result_json: Optional[str] = None
    error: Optional[str] = None

    # Idempotency key. A double-click, a retry and a reconnect all collapse onto
    # the same run rather than sending the same email twice.
    run_key: str = ""
    requested_by_user_id: Optional[str] = None

    # Execution claim. Delivery of the dispatch event is at-least-once, so two
    # deliveries can race to execute one run; the claim is what makes exactly
    # one of them proceed. `claim_key` is the winner's nonce, `claimed_at` is
    # when it took the run -- and what lets a claim whose holder died be told
    # apart from one that is merely slow.
    claim_key: str = ""
    claimed_at: Optional[str] = None

    created_at: Optional[str] = None
    finished_at: Optional[str] = None


class CanvasInvocationRecord(CanvasInvocationRow):
    """A stored invocation, as read back."""

    invocation_id: Optional[int] = None
