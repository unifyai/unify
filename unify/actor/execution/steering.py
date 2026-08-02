"""Steering for code while it is still running.

A code block dispatched through ``execute_code`` or ``execute_function`` used
to be opaque once scheduled: the async tool loop raced interjections against
pending tool tasks and gave the model a turn, but the only lever that turn
offered over a running block was ``stop_*``, which cancels it outright. A
correction arriving four sends into a five-send loop could not reach the loop
— it could only kill it, losing the send that was already correct along with
the one that was not.

This module supplies the missing half: a channel into the running program, and
checkpoints where the program looks at it.

Checkpoints
-----------
The AST transformer in :func:`instrument_for_steering` inserts a checkpoint
between top-level statements and at the top of every loop body, at any nesting
depth. Loop bodies matter more than top-level statements — repeated work is
where a correction has something left to change, and a block that sends to a
list of vendors spends its whole life inside one statement.

Reaching a checkpoint with an empty channel costs a queue peek and an
immediate return. The instrumentation itself costs ~25 microseconds per
instrumented statement, which a block pays once per execution against LLM
latency measured in seconds.

What a checkpoint does not do
-----------------------------
It never classifies the interjection. Deciding whether "actually, stop" means
abandon-the-block is the model's judgement, made on a real turn with the
transcript in front of it, not a string comparison run inside the sandbox. So
a checkpoint that finds a message suspends and hands the decision up:

* resuming is any message arriving on the same channel — the block continues
  and the text is readable from ``steering.messages``
* abandoning is ``stop_<tool>_<call_id>``, an existing tool that cancels the
  task and unwinds the block through ordinary cancellation

Neither path parses intent, and the two are different tools rather than two
readings of one string.

The blocking-call boundary
--------------------------
A checkpoint can only run when the sandbox coroutine yields. Measured against
the real sandbox: ``await``-bearing code yields reliably, while
``time.sleep``, synchronous HTTP, and tight CPU loops hold the event loop for
their full duration. During such a call the interjection cannot even arrive —
the tool loop that would receive it runs on the same event loop — so this is a
property of the runtime rather than a limit these checkpoints introduce.
Instrumentation is placed to bracket those calls, which is the most that can
be observed without moving execution off the loop thread.
"""

from __future__ import annotations

import ast
import asyncio
from typing import Any, Dict, List, Optional

#: Names the instrumented program calls. Chosen to be unlikely to collide with
#: anything a model writes, and stripped from tracebacks shown to the model.
CHECKPOINT_FN = "__steering_checkpoint__"
CHECKPOINT_SYNC_FN = "__steering_checkpoint_sync__"
CHANNEL_GLOBAL = "__steering_channel__"

#: How long a suspended checkpoint waits for the model to decide before giving
#: up and resuming. A suspend that never returns would strand the block and,
#: with it, the loop; the model reliably takes a turn because the notification
#: sets ``llm_turn_required``, but a turn that answers in prose rather than
#: calling a tool would otherwise hang here forever.
DEFAULT_SUSPEND_TIMEOUT_S = 180.0


class SteeringChannel:
    """The sandbox side of one tool call's interjection queue.

    Instances live in sandbox globals under :data:`CHANNEL_GLOBAL` for the
    duration of a single ``execute_code`` / ``execute_function`` call, and are
    reachable from generated code as ``steering``.
    """

    def __init__(
        self,
        *,
        interject_q: Optional[asyncio.Queue],
        notification_q: Optional[asyncio.Queue] = None,
        suspend_timeout: float = DEFAULT_SUSPEND_TIMEOUT_S,
    ) -> None:
        self._interject_q = interject_q
        self._notification_q = notification_q
        self._suspend_timeout = suspend_timeout
        self._messages: List[str] = []
        self._line: Optional[int] = None
        self._checkpoints = 0
        self._suspensions = 0
        self._source_lines: List[str] = []

    # ── what generated code may read ──────────────────────────────────────
    @property
    def messages(self) -> List[str]:
        """Interjections delivered so far, oldest first.

        Code that wants to adapt rather than be re-planned can read this; code
        that ignores it behaves exactly as it did before instrumentation.
        """
        return list(self._messages)

    @property
    def active(self) -> bool:
        """Whether a steering channel is attached to this execution."""
        return self._interject_q is not None

    # ── what the tool reads back after execution ──────────────────────────
    def progress(self) -> Dict[str, Any]:
        """How far the block got, for the model's next turn.

        Reported whether the block finished, was interrupted, or was
        cancelled, so a turn taken mid-block is never guessing about what has
        already happened.
        """
        report: Dict[str, Any] = {
            "checkpoints_passed": self._checkpoints,
            "suspensions": self._suspensions,
        }
        if self._line is not None:
            report["last_line_reached"] = self._line
            source = self._source_at(self._line)
            if source:
                report["last_statement"] = source
        if self._messages:
            report["interjections_received"] = list(self._messages)
        return report

    def bind_source(self, code: str) -> None:
        """Record the block's source so progress can quote the line reached."""
        self._source_lines = code.splitlines()

    def _source_at(self, line: int) -> str:
        if not 1 <= line <= len(self._source_lines):
            return ""
        # REPL semantics rewrite a trailing expression into a ``return`` before
        # this source is bound. Quoting it back with the ``return`` would show
        # the model a line it did not write.
        return self._source_lines[line - 1].strip().removeprefix("return ")

    # ── the checkpoint itself ─────────────────────────────────────────────
    def _drain(self) -> List[str]:
        """Take everything waiting. Synchronous and non-blocking by design.

        A checkpoint must be cheap enough to sit inside a loop body, so the
        empty case costs one ``QueueEmpty`` and no trip through the scheduler.
        """
        if self._interject_q is None:
            return []
        found: List[str] = []
        while True:
            try:
                item = self._interject_q.get_nowait()
            except asyncio.QueueEmpty:
                return found
            found.append(_as_text(item))

    async def checkpoint(self, line: Optional[int] = None) -> None:
        """Look at the channel, and suspend if anything is waiting."""
        self._checkpoints += 1
        if line is not None:
            self._line = line
        incoming = self._drain()
        if not incoming:
            return
        self._messages.extend(incoming)
        await self._suspend(incoming)

    def checkpoint_sync(self, line: Optional[int] = None) -> None:
        """Checkpoint for a synchronous scope, which cannot suspend.

        A ``def`` body inside the block has no way to await, so this records
        the interjection and lets execution continue to the next point that
        can suspend. The message is still visible to the model in
        :meth:`progress`, so the turn taken after the block is not blind to it.
        """
        self._checkpoints += 1
        if line is not None:
            self._line = line
        self._messages.extend(self._drain())

    async def _suspend(self, incoming: List[str]) -> None:
        """Hold the block and give the model a turn to decide about it.

        The notification sets ``llm_turn_required`` in the async tool loop, so
        the model speaks next with the interjection and the progress report in
        front of it. Resuming is any further message on the channel; abandoning
        is ``stop_*``, whose cancellation unwinds straight through this await.
        """
        self._suspensions += 1
        if self._notification_q is not None:
            payload = {
                "type": "steering_checkpoint",
                "message": (
                    "Execution is suspended at a checkpoint after an "
                    "interjection arrived. Decide whether the running code "
                    "should continue as written or be abandoned and replaced."
                ),
                "interjections": list(incoming),
                "progress": self.progress(),
            }
            try:
                self._notification_q.put_nowait(payload)
            except asyncio.QueueFull:
                pass

        if self._interject_q is None:
            return
        try:
            decision = await asyncio.wait_for(
                self._interject_q.get(),
                timeout=self._suspend_timeout,
            )
        except asyncio.TimeoutError:
            self._messages.append(
                "[no steering decision arrived; execution resumed as written]",
            )
            return
        self._messages.append(_as_text(decision))


def _as_text(item: Any) -> str:
    """Interjections arrive as bare strings from the loop, dicts from nesting."""
    if isinstance(item, dict):
        for key in ("content", "message", "text"):
            value = item.get(key)
            if value:
                return str(value)
        return str(item)
    return str(item)


# ---------------------------------------------------------------------------
# AST instrumentation
# ---------------------------------------------------------------------------
class _CheckpointInserter(ast.NodeTransformer):
    """Insert checkpoints between statements and at the top of loop bodies.

    Tracks whether the scope being rewritten can await, because a ``def`` body
    cannot and needs the synchronous variant.
    """

    def __init__(self) -> None:
        self._async_scope = True

    # A nested ``def`` cannot await; an ``async def`` can. Class bodies run at
    # definition time and are left alone.
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:  # noqa: N802
        previous, self._async_scope = self._async_scope, False
        try:
            self.generic_visit(node)
        finally:
            self._async_scope = previous
        return node

    def visit_AsyncFunctionDef(  # noqa: N802
        self,
        node: ast.AsyncFunctionDef,
    ) -> ast.AST:
        previous, self._async_scope = self._async_scope, True
        try:
            self.generic_visit(node)
        finally:
            self._async_scope = previous
        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:  # noqa: N802
        return node

    def visit_For(self, node: ast.For) -> ast.AST:  # noqa: N802
        self.generic_visit(node)
        node.body = self._prefixed(node.body)
        return node

    def visit_AsyncFor(self, node: ast.AsyncFor) -> ast.AST:  # noqa: N802
        self.generic_visit(node)
        node.body = self._prefixed(node.body)
        return node

    def visit_While(self, node: ast.While) -> ast.AST:  # noqa: N802
        self.generic_visit(node)
        node.body = self._prefixed(node.body)
        return node

    def _prefixed(self, body: List[ast.stmt]) -> List[ast.stmt]:
        """Put a checkpoint at the top of a loop body.

        Placed before the body rather than after it so a correction lands
        before the next iteration's side effect, not after it.
        """
        if not body:
            return body
        return [_checkpoint_stmt(body[0].lineno, self._async_scope), *body]


def _checkpoint_stmt(line: int, is_async: bool) -> ast.stmt:
    call = ast.Call(
        func=ast.Name(
            id=CHECKPOINT_FN if is_async else CHECKPOINT_SYNC_FN,
            ctx=ast.Load(),
        ),
        args=[ast.Constant(value=line)],
        keywords=[],
    )
    value: ast.expr = ast.Await(value=call) if is_async else call
    return ast.Expr(value=value)


def instrument_for_steering(tree: ast.Module) -> ast.Module:
    """Rewrite *tree* so it reports progress and observes the channel.

    Checkpoints go between top-level statements and at the top of every loop
    body. The module's final expression is deliberately left as the last
    statement: the sandbox turns it into the block's return value, and a
    checkpoint appended after it would not run anyway.
    """
    tree = _CheckpointInserter().visit(tree)

    instrumented: List[ast.stmt] = []
    for statement in tree.body:
        instrumented.append(_checkpoint_stmt(statement.lineno, True))
        instrumented.append(statement)
    tree.body = instrumented
    ast.fix_missing_locations(tree)
    return tree


# ---------------------------------------------------------------------------
# Binding into sandbox globals
# ---------------------------------------------------------------------------
_STEERING_MISSING = object()


def bind_sandbox_steering_channel(
    global_state: Dict[str, Any],
    channel: SteeringChannel,
) -> Dict[str, Any]:
    """Expose *channel* and its checkpoints to a sandbox for one tool call.

    Mirrors the clarification binding: values are installed on the live
    globals and the previous values returned as a token, so nested calls
    restore rather than clobber.
    """
    previous = {
        key: global_state.get(key, _STEERING_MISSING)
        for key in (CHANNEL_GLOBAL, CHECKPOINT_FN, CHECKPOINT_SYNC_FN, "steering")
    }
    global_state[CHANNEL_GLOBAL] = channel
    global_state[CHECKPOINT_FN] = channel.checkpoint
    global_state[CHECKPOINT_SYNC_FN] = channel.checkpoint_sync
    global_state["steering"] = channel
    return previous


def restore_sandbox_steering_channel(
    global_state: Dict[str, Any],
    token: Dict[str, Any],
) -> None:
    """Undo :func:`bind_sandbox_steering_channel`."""
    for key, value in token.items():
        if value is _STEERING_MISSING:
            global_state.pop(key, None)
        else:
            global_state[key] = value


def current_channel(global_state: Dict[str, Any]) -> Optional[SteeringChannel]:
    """The channel bound to *global_state*, if a tool call installed one."""
    channel = global_state.get(CHANNEL_GLOBAL)
    return channel if isinstance(channel, SteeringChannel) else None


# ---------------------------------------------------------------------------
# Dispatch-boundary checkpoints
# ---------------------------------------------------------------------------
class _SteeringManagerProxy:
    """Checkpoints one manager's methods on the way in."""

    __slots__ = ("_manager", "_channel")

    def __init__(self, manager: Any, channel: SteeringChannel) -> None:
        self._manager = manager
        self._channel = channel

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._manager, name)
        if not callable(attr):
            return attr

        if asyncio.iscoroutinefunction(attr):

            async def _checked(*args: Any, **kwargs: Any) -> Any:
                await self._channel.checkpoint()
                return await attr(*args, **kwargs)

        else:

            def _checked(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
                self._channel.checkpoint_sync()
                return attr(*args, **kwargs)

        return _checked


class SteeringDispatchProxy:
    """Checkpoint every primitive call, wherever it sits in the source.

    Statement and loop-body checkpoints cannot reach inside a single
    expression, so ``[await send(v) for v in vendors]`` and
    ``asyncio.gather(*calls)`` dispatch a whole batch between two checkpoints.
    Checking at the dispatch boundary covers those, because the checkpoint
    runs inside each call rather than around the statement containing them.

    Stored functions invoked as bare callables are not covered by this proxy;
    they are reached through statement and loop-body checkpoints only.
    """

    __slots__ = ("_target", "_channel")

    def __init__(self, target: Any, channel: SteeringChannel) -> None:
        self._target = target
        self._channel = channel

    def __getattr__(self, name: str) -> Any:
        return _SteeringManagerProxy(getattr(self._target, name), self._channel)
