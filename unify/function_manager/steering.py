"""Steering a block of code for as long as it is running, and no longer.

A correction that arrives while ``execute_code`` or ``execute_function`` is in
flight has to be able to reach the work, not just kill it. Killing loses the
sends that were already right along with the one that was not.

The mechanism has four parts, and they only exist for the duration of one
call:

**Checkpoints.** An AST pass injects ``_cp(label)`` and ``_int(func)`` calls at
function entry, at the top of every loop iteration, and around every awaited
call — including awaits nested inside expressions, so a comprehension or a
``gather`` is covered as well as a plain statement. ``_cp`` honours pause and
is cheap when nothing is pending; ``_int`` raises when a patch targets the
function it names.

**An idempotency cache.** Every dispatch through a tool namespace is memoised
under ``(call_stack, loop_context, branch_path, step, tool, args)``. Path and
loop context come from probes the same AST pass injects, so the key describes
*where in the execution* a call happened rather than just what it was.

**Retry in place.** ``ControlledInterruption`` unwinds to a retry loop around
the invocation, which swaps in the patched source and runs it again. The cache
turns the unchanged prefix into replays, so execution reaches the first
genuinely different statement having redone nothing.

**A bounded lifetime.** Cache, patches and instrumentation are created when the
call begins and discarded when it returns. Nothing survives into the next call,
which is what keeps the positional cache key sound: the surrounding code cannot
have changed underneath it, because the block is still the one that is running.

Code that runs in another process is steered at two grains. Every primitive
dispatch already blocks the child on the parent's JSON-RPC reply, so the reply
is a checkpoint (:func:`dispatch_with_steering`) — venv and shell both get
this without instrumentation. Between dispatches, venv children additionally
run parent-instrumented source whose probes read a pushed control directive
(:meth:`SteeringSession.relay_corrections`), so a loop that makes no primitive
call is still interruptible; on retry the parent re-sends patched source
either way.

What this deliberately does not do
----------------------------------
Replay records that a side effect happened; it cannot undo one. A patch
redirects the work that has not happened yet. A correction meaning "unsend
that" is not something this can satisfy, and a cache hit must never be allowed
to look like it did — which is why invalidation is explicit rather than
inferred.

A checkpoint also only runs when the block yields. Synchronous blocking work —
``time.sleep``, non-async HTTP, a tight compute loop — holds the event loop for
its full duration, and during that window the correction cannot even arrive,
since the loop that would receive it is the one being held.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import contextvars
import logging
import typing
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

#: Names the instrumented program calls. Injected into the execution globals
#: for the lifetime of one call and removed afterwards.
CP_FN = "_cp"
INT_FN = "_int"
AROUND_CP_FN = "_around_cp"
RUNTIME_GLOBAL = "runtime"
SESSION_GLOBAL = "__steering_session__"

#: Retry ceiling for one invocation. A patch that keeps failing must surface as
#: an error rather than spinning: each retry costs a full re-execution from the
#: cache, and a correction that cannot be satisfied is information the caller
#: needs.
MAX_RETRIES = 5


class ControlledInterruption(Exception):
    """Raised inside instrumented code to stop and retry the enclosing call.

    Carries the reason so the retry can report what redirected it.
    """


@dataclass
class Patch:
    """Replacement source for one function, and what it invalidates.

    ``invalidate`` names tool paths whose cached results must be discarded
    before the retry. Without it a correction that means "call that again
    differently" would be answered by the memoised result of the original
    call — the cache confirming exactly the thing the correction wanted
    changed.
    """

    function_name: str
    source: str
    reason: str = ""
    invalidate: Tuple[str, ...] = ()


@dataclass
class InterruptionRequest:
    """A decision about work that is currently running."""

    reason: str
    patches: List[Patch] = field(default_factory=list)

    def targets(self, function_name: str) -> bool:
        return any(p.function_name == function_name for p in self.patches)


# ---------------------------------------------------------------------------
# Execution position
# ---------------------------------------------------------------------------
class SteeringRuntime:
    """Where execution currently is, in terms the cache key can use.

    Three independent axes, kept separate on purpose: the call stack says which
    function, the loop context says which iteration of which loop, and the path
    context says which branch of which conditional. Collapsing them into one
    counter would make a call inside the second iteration of a loop in the else
    branch indistinguishable from an unrelated call that happened to be the same
    number of steps in.
    """

    def __init__(self) -> None:
        self.action_counter = 0
        self.path_context: List[str] = []
        self.call_stack: List[Tuple[int, str]] = []
        self._frame_counter = 0
        self._loop_stack: List[Tuple[str, int]] = []
        self._occurrences: Dict[Tuple[str, str], int] = {}
        self._pause = asyncio.Event()
        self._pause.set()

    def next_occurrence(self, signature: Tuple[str, str]) -> int:
        """How many times this exact call has been made during this attempt.

        Reset per attempt, so a replay counts from zero and lines up with what
        the previous attempt recorded.
        """
        seen = self._occurrences.get(signature, 0)
        self._occurrences[signature] = seen + 1
        return seen

    # ── call stack ────────────────────────────────────────────────────────
    def push_frame(self, func_name: str) -> Tuple[int, str]:
        self._frame_counter += 1
        token = (self._frame_counter, func_name)
        self.call_stack.append(token)
        return token

    def pop_frame(self, token: Tuple[int, str]) -> None:
        # A mismatched pop means the stack is already wrong; correcting it here
        # would hide that, and a wrong stack silently corrupts every subsequent
        # cache key.
        if self.call_stack and self.call_stack[-1] == token:
            self.call_stack.pop()
        elif self.call_stack:
            logger.warning(
                "steering: stale frame pop, expected %s found %s",
                token,
                self.call_stack[-1],
            )

    def stack_tuple(self) -> Tuple[str, ...]:
        return tuple(name for _, name in self.call_stack)

    # ── branch path ───────────────────────────────────────────────────────
    def push_path_context(self, context_id: str) -> None:
        self.path_context.append(context_id)

    def pop_path_context(self) -> None:
        if self.path_context:
            self.path_context.pop()

    # ── loops ─────────────────────────────────────────────────────────────
    def start_loop_context(self, loop_id: str) -> None:
        self._loop_stack.append((loop_id, -1))

    def increment_loop_iteration(self, loop_id: str) -> None:
        if not self._loop_stack:
            return
        current_id, iteration = self._loop_stack[-1]
        self._loop_stack[-1] = (current_id, iteration + 1)

    def end_loop_context(self, loop_id: str) -> None:
        if self._loop_stack:
            self._loop_stack.pop()

    def loop_tuple(self) -> Tuple[Tuple[str, int], ...]:
        return tuple(self._loop_stack)

    # ── pause ─────────────────────────────────────────────────────────────
    def pause(self) -> None:
        self._pause.clear()

    def resume(self) -> None:
        self._pause.set()

    @property
    def paused(self) -> bool:
        return not self._pause.is_set()

    async def wait_if_paused(self) -> None:
        await self._pause.wait()

    def reset_position(self) -> None:
        """Return to the start for a retry, keeping nothing but the pause state.

        The cache survives a retry; the position must not, or the replayed
        prefix would be looked up under keys from the run that was abandoned.
        """
        self.action_counter = 0
        self.path_context.clear()
        self.call_stack.clear()
        self._frame_counter = 0
        self._loop_stack.clear()
        self._occurrences.clear()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------
CacheKey = Tuple[Any, ...]


class IdempotencyCache:
    """Results of dispatches already made during this call.

    Not a performance cache. Every entry records that a side effect has
    *already happened*, which is what lets a retry skip it rather than repeat
    it. That also means a stale entry is not a slow path but a wrong one, so
    eviction is explicit.
    """

    def __init__(self) -> None:
        self._entries: Dict[CacheKey, Dict[str, Any]] = {}
        self.hits = 0
        self.misses = 0

    def __contains__(self, key: CacheKey) -> bool:
        return key in self._entries

    def get(self, key: CacheKey) -> Optional[Dict[str, Any]]:
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        self.hits += 1
        return entry

    def put(self, key: CacheKey, result: Any, *, tool: str) -> None:
        self._entries[key] = {"result": result, "tool": tool}

    def invalidate(self, tool_paths: typing.Iterable[str]) -> int:
        """Drop every entry whose tool matches one of *tool_paths*.

        Matching is by prefix so a correction can name a family
        (``primitives.comms``) or one call (``primitives.comms.send``).
        """
        wanted = tuple(tool_paths)
        if not wanted:
            return 0
        doomed = [
            key
            for key, entry in self._entries.items()
            if any(str(entry["tool"]).startswith(p) for p in wanted)
        ]
        for key in doomed:
            del self._entries[key]
        return len(doomed)

    def completed_calls(self) -> List[str]:
        return [str(entry["tool"]) for entry in self._entries.values()]

    def __len__(self) -> int:
        return len(self._entries)


# ---------------------------------------------------------------------------
# The per-call session
# ---------------------------------------------------------------------------
class SteeringSession:
    """Everything steerable about one in-flight call.

    Created when the call starts, discarded when it returns. Holding it any
    longer would let a cache key from a finished block collide with a live one.
    """

    def __init__(
        self,
        *,
        interject_q: Optional[asyncio.Queue] = None,
        notification_q: Optional[asyncio.Queue] = None,
        patch_author: Optional[Any] = None,
    ) -> None:
        self.runtime = SteeringRuntime()
        self.cache = IdempotencyCache()
        self.interruption: Optional[InterruptionRequest] = None
        self.retries = 0
        self.messages: List[str] = []
        self._interject_q = interject_q
        self._notification_q = notification_q
        self._patch_author = patch_author
        self._applied: List[Patch] = []
        #: Source of the attempt currently running. Kept current across
        #: retries, so a second correction is written against the code that is
        #: actually executing rather than the version it replaced.
        self.source: str = ""

    def bind_source(self, source: str) -> None:
        self.source = source

    # ── the injected callables ────────────────────────────────────────────
    async def cp(self, label: str = "") -> None:
        """Cooperative yield point: honour pause, then take any correction.

        Deliberately cheap when nothing is happening — one queue peek and a
        set event — because this runs at every loop iteration and around every
        dispatch.
        """
        self.runtime.action_counter += 1
        if self.runtime.paused:
            await self.runtime.wait_if_paused()
        await self._collect()

    async def interrupt_point(self, func_name: str) -> None:
        """Raise if a pending correction targets *func_name*.

        The request is deliberately left in place: the retry loop is what
        applies the patch, so clearing it here would unwind into a handler
        with nothing to act on and no way to tell that from a spurious raise.
        Consuming it is :func:`run_with_steering`'s job.
        """
        await self._collect()
        request = self.interruption
        if request is not None and request.targets(func_name):
            raise ControlledInterruption(request.reason or "steered")

    async def around(self, label: str, awaitable: Any) -> Any:
        """Bracket one awaited call with checkpoints.

        Used for awaits sitting inside a larger expression, where a statement
        level probe cannot reach — a comprehension over recipients dispatches
        the whole batch between two statements otherwise.
        """
        await self.cp(f"Before: {label}")
        try:
            return await awaitable
        finally:
            await self.cp(f"After: {label}")

    # ── correction intake ─────────────────────────────────────────────────
    async def _collect(self) -> None:
        """Drain waiting interjections and turn them into a patch."""
        if self._interject_q is None:
            return
        incoming: List[str] = []
        while True:
            try:
                incoming.append(_as_text(self._interject_q.get_nowait()))
            except asyncio.QueueEmpty:
                break
        if not incoming:
            return
        self.messages.extend(incoming)
        if self._patch_author is None:
            return
        request = await self._patch_author(
            interjections=incoming,
            session=self,
        )
        if request is not None:
            self.interruption = request

    def note_applied(self, patch: Patch) -> None:
        self._applied.append(patch)

    async def relay_corrections(
        self,
        source: str,
        send_control: typing.Callable[[Dict[str, Any]], typing.Awaitable[None]],
        *,
        poll_interval: float = 0.05,
    ) -> None:
        """Deliver corrections to code running where no checkpoint can look.

        Out-of-process, the parent's checkpoints fire only when the child
        dispatches; a child inside a loop that makes no primitive call never
        gives the parent a chance to collect interjections, let alone act on
        them. This watches the intake while an attempt runs and, once a
        correction targets the running block, pushes one interrupt directive
        over *send_control* so the child's next instrumented checkpoint
        raises. Runs until then, or until the attempt ends and cancels it.
        """
        while True:
            await self._collect()
            request = self.interruption
            if request is not None and _targets_running_block(request, source):
                await send_control(
                    {
                        "type": "control",
                        "action": "interrupt",
                        "reason": request.reason or "steered",
                        "functions": sorted(
                            {patch.function_name for patch in request.patches},
                        ),
                    },
                )
                return
            await asyncio.sleep(poll_interval)

    def notify(self, payload: Dict[str, Any]) -> None:
        if self._notification_q is None:
            return
        try:
            self._notification_q.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    # ── reporting ─────────────────────────────────────────────────────────
    def progress(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "steps": self.runtime.action_counter,
            "retries": self.retries,
            "replayed": self.cache.hits,
            "executed": self.cache.misses,
        }
        if self.messages:
            report["interjections_received"] = list(self.messages)
        if self._applied:
            report["patched"] = [
                {"function": p.function_name, "reason": p.reason} for p in self._applied
            ]
        return report


def _as_text(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("content", "message", "text"):
            value = item.get(key)
            if value:
                return str(value)
        return str(item)
    return str(item)


def make_cache_key(
    session: SteeringSession,
    tool: str,
    args: tuple,
    kwargs: dict,
) -> CacheKey:
    """Identify a dispatch by what it did, and how many times it has done it.

    The question the cache answers is "has this exact side effect already
    happened?", so the key is the call itself plus an occurrence index: the
    second ``send(a)`` of a run is a different effect from the first, but the
    first ``send(a)`` is the same effect wherever in the program it is written.

    Keying on execution position instead — call stack, loop iteration, branch
    path, step counter — is what the earlier design did, and it defeats the
    commonest correction there is. Narrowing a loop with ``if v.startswith(...)``
    puts every surviving call inside a new branch, changing its position and
    therefore its key, so a retry re-sends everything that had already gone
    out. Position is still tracked, and still reported, but it does not decide
    identity.
    """
    signature = (tool, _serialize_args(args, kwargs))
    occurrence = session.runtime.next_occurrence(signature)
    return (*signature, occurrence)


def _serialize_args(args: tuple, kwargs: dict) -> str:
    def _short(value: Any) -> str:
        text = repr(value)
        return text if len(text) <= 200 else text[:199] + "…"

    parts = [_short(a) for a in args]
    parts += [f"{k}={_short(v)}" for k, v in sorted(kwargs.items())]
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# AST instrumentation
# ---------------------------------------------------------------------------
class _Instrumenter(ast.NodeTransformer):
    """Insert probes so the runtime knows where execution is.

    Three kinds, and they answer different questions:

    * ``_cp`` / ``_int`` — can this be paused, and has a correction arrived
      that targets the function we are inside?
    * loop and path probes — which iteration, which branch? These feed the
      cache key, and without them a call in the second iteration of a loop is
      indistinguishable from the same call in the first.
    * ``_around_cp`` — awaits nested inside expressions, which no
      statement-level probe can reach.

    A synchronous ``def`` gets the loop and path probes but not ``_cp`` /
    ``_int``, which are awaits it cannot make. It still contributes correct
    position to the cache key; it just cannot suspend.
    """

    def __init__(self, tool_namespaces: typing.Set[str]) -> None:
        self._tool_namespaces = tool_namespaces
        self._defined_functions: typing.Set[str] = set()
        self._function_context: List[str] = []
        self._counters: Dict[str, Dict[str, int]] = {}
        self._in_async = True

    # ── helpers ───────────────────────────────────────────────────────────
    @property
    def _current_function(self) -> str:
        return self._function_context[-1] if self._function_context else "global"

    def _counter(self, kind: str) -> int:
        scope = self._counters.setdefault(self._current_function, {})
        scope[kind] = scope.get(kind, 0) + 1
        return scope[kind]

    @staticmethod
    def _call(func_id: str, args: List[ast.expr]) -> ast.Call:
        return ast.Call(
            func=ast.Name(id=func_id, ctx=ast.Load()),
            args=args,
            keywords=[],
        )

    def _awaited_stmt(self, call: ast.Call) -> ast.Expr:
        return ast.Expr(value=ast.Await(value=call))

    def _cp_node(self, label: str) -> ast.Expr:
        return self._awaited_stmt(self._call(CP_FN, [ast.Constant(value=label)]))

    def _int_node(self, func_name: str) -> ast.Expr:
        return self._awaited_stmt(self._call(INT_FN, [ast.Constant(value=func_name)]))

    @staticmethod
    def _runtime_call(method: str, args: List[ast.expr]) -> ast.Expr:
        return ast.Expr(
            value=ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id=RUNTIME_GLOBAL, ctx=ast.Load()),
                    attr=method,
                    ctx=ast.Load(),
                ),
                args=args,
                keywords=[],
            ),
        )

    def _label(self, call: ast.Call) -> str:
        parts: List[str] = []
        node: ast.expr = call.func
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts)) if parts else "call"

    def _is_tool_call(self, label: str, call: ast.Call) -> bool:
        """Whether this dispatch is worth caching and bracketing.

        Tool namespaces and functions the block itself defines. Arbitrary
        library calls are left alone: memoising them would claim a side effect
        happened that we cannot describe, and bracketing every call would bury
        the program in probes.
        """
        root = label.split(".", 1)[0]
        if root in self._tool_namespaces:
            return True
        return (
            isinstance(call.func, ast.Name) and call.func.id in self._defined_functions
        )

    # ── scopes ────────────────────────────────────────────────────────────
    def visit_Module(self, node: ast.Module) -> ast.Module:  # noqa: N802
        for stmt in node.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._defined_functions.add(stmt.name)
        self.generic_visit(node)
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:  # noqa: N802
        self._function_context.append(node.name)
        was_async, self._in_async = self._in_async, False
        try:
            self.generic_visit(node)
        finally:
            self._in_async = was_async
            self._function_context.pop()
        return node

    def visit_AsyncFunctionDef(  # noqa: N802
        self,
        node: ast.AsyncFunctionDef,
    ) -> ast.AsyncFunctionDef:
        self._function_context.append(node.name)
        was_async, self._in_async = self._in_async, True
        try:
            self.generic_visit(node)
            # After the docstring, so the function keeps it.
            offset = (
                1
                if node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
                else 0
            )
            node.body[offset:offset] = [
                self._cp_node(f"Enter function: {node.name}"),
                self._int_node(node.name),
            ]
        finally:
            self._in_async = was_async
            self._function_context.pop()
        return node

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.ClassDef:  # noqa: N802
        return node

    # ── branches ──────────────────────────────────────────────────────────
    def _with_path(self, block: List[ast.stmt], context_id: str) -> List[ast.stmt]:
        """Record which branch ran, and unrecord it however the block exits.

        The pop sits in ``finally`` because an exception leaving a branch must
        not leave the path context claiming we are still inside it — every
        cache key made afterwards would be wrong.
        """
        if not block:
            return block
        return [
            ast.Try(
                body=[
                    self._runtime_call(
                        "push_path_context",
                        [ast.Constant(value=context_id)],
                    ),
                    *block,
                ],
                handlers=[],
                orelse=[],
                finalbody=[self._runtime_call("pop_path_context", [])],
            ),
        ]

    def visit_If(self, node: ast.If) -> ast.If:  # noqa: N802
        if_id = f"if_{self._counter('if')}"
        self.generic_visit(node)
        node.body = self._with_path(node.body, f"{if_id}_true")
        if node.orelse:
            node.orelse = self._with_path(node.orelse, f"{if_id}_false")
        return node

    def visit_Try(self, node: ast.Try) -> ast.Try:  # noqa: N802
        try_id = f"try_{self._counter('try')}"
        self.generic_visit(node)
        node.body = self._with_path(node.body, f"{try_id}_body")
        for index, handler in enumerate(node.handlers):
            handler.body = self._with_path(handler.body, f"{try_id}_except_{index}")
        if node.orelse:
            node.orelse = self._with_path(node.orelse, f"{try_id}_else")
        if node.finalbody:
            node.finalbody = self._with_path(node.finalbody, f"{try_id}_finally")
        return node

    # ── loops ─────────────────────────────────────────────────────────────
    def _wrap_loop(self, node: Any) -> ast.Try:
        loop_id = f"{type(node).__name__.lower()}_{self._counter('loop')}"
        enclosing = self._current_function
        self.generic_visit(node)

        probes: List[ast.stmt] = [
            self._runtime_call(
                "increment_loop_iteration",
                [ast.Constant(value=loop_id)],
            ),
        ]
        if self._in_async:
            # Before the body, so a correction lands before the iteration's
            # side effect rather than after it.
            probes.append(self._cp_node(f"Iteration of {loop_id} in {enclosing}"))
            probes.append(self._int_node(enclosing))
        node.body = probes + node.body

        return ast.Try(
            body=[
                self._runtime_call("start_loop_context", [ast.Constant(value=loop_id)]),
                node,
            ],
            handlers=[],
            orelse=[],
            finalbody=[
                self._runtime_call("end_loop_context", [ast.Constant(value=loop_id)]),
            ],
        )

    def visit_For(self, node: ast.For) -> ast.Try:  # noqa: N802
        return self._wrap_loop(node)

    def visit_While(self, node: ast.While) -> ast.Try:  # noqa: N802
        return self._wrap_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> ast.Try:  # noqa: N802
        return self._wrap_loop(node)

    # ── dispatches ────────────────────────────────────────────────────────
    def visit_Await(self, node: ast.Await) -> ast.Await:  # noqa: N802
        """Bracket an awaited dispatch wherever it sits in an expression.

        This is what covers ``[await send(v) for v in vendors]`` and
        ``gather(*calls)``: the probe goes around the call itself, so it runs
        once per dispatch rather than once per enclosing statement.
        """
        self.generic_visit(node)
        if not isinstance(node.value, ast.Call):
            return node
        call = node.value
        label = self._label(call)
        if not self._is_tool_call(label, call):
            return node
        node.value = self._call(AROUND_CP_FN, [ast.Constant(value=label), call])
        return node


def instrument(tree: ast.Module, *, tool_namespaces: typing.Set[str]) -> ast.Module:
    """Rewrite *tree* so the runtime can see and steer it."""
    tree = _Instrumenter(tool_namespaces).visit(tree)
    ast.fix_missing_locations(tree)
    return tree


# ---------------------------------------------------------------------------
# Memoised dispatch
# ---------------------------------------------------------------------------
#: Attribute values handed back untouched rather than descended into. Anything
#: else non-callable is treated as a nested namespace, because the calls that
#: matter live one level down: ``primitives.integrations.slack.send_message``,
#: ``primitives.computer.web.new_session``.
_PASSTHROUGH_TYPES = (
    str,
    bytes,
    bytearray,
    int,
    float,
    bool,
    complex,
    type(None),
    list,
    tuple,
    dict,
    set,
    frozenset,
)


def _is_async_callable(fn: Any) -> bool:
    """Whether *fn* ultimately dispatches a coroutine.

    Context forwarding hands back a synchronous ``functools.wraps`` wrapper
    around an async method, and ``iscoroutinefunction`` reports False for it
    because it reads the wrapper's own code flags rather than following
    ``__wrapped__``.
    """
    seen: typing.Set[int] = set()
    while fn is not None and id(fn) not in seen:
        if asyncio.iscoroutinefunction(fn):
            return True
        seen.add(id(fn))
        fn = getattr(fn, "__wrapped__", None)
    return False


class _MemoisedNamespace:
    """Routes one namespace's calls through the cache.

    A hit means the call already happened during this block and its effect is
    already in the world, so the recorded result stands in for re-running it.
    That is only sound because the cache dies with the block: a key can never
    outlive the execution whose position it describes.
    """

    __slots__ = ("_target", "_session", "_path")

    def __init__(self, target: Any, session: SteeringSession, path: str) -> None:
        self._target = target
        self._session = session
        self._path = path

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)
        if not callable(attr):
            if isinstance(attr, _PASSTHROUGH_TYPES):
                return attr
            return _MemoisedNamespace(attr, self._session, f"{self._path}.{name}")

        tool = f"{self._path}.{name}"
        session = self._session

        if _is_async_callable(attr):

            async def _dispatch(*args: Any, **kwargs: Any) -> Any:
                key = make_cache_key(session, tool, args, kwargs)
                hit = session.cache.get(key)
                if hit is not None:
                    return hit["result"]
                result = attr(*args, **kwargs)
                if _inspect_isawaitable(result):
                    result = await result
                session.cache.put(key, result, tool=tool)
                return result

        else:

            def _dispatch(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
                key = make_cache_key(session, tool, args, kwargs)
                hit = session.cache.get(key)
                if hit is not None:
                    return hit["result"]
                result = attr(*args, **kwargs)
                session.cache.put(key, result, tool=tool)
                return result

        _copy_signature(attr, _dispatch)
        return _dispatch


def _inspect_isawaitable(value: Any) -> bool:
    import inspect

    return inspect.isawaitable(value)


def _copy_signature(source: Any, target: Any) -> None:
    import functools

    try:
        functools.wraps(source)(target)
    except (AttributeError, TypeError, ValueError):
        pass


class MemoisedDispatch:
    """Wraps a tool root (``primitives``) so every call below it is memoised."""

    __slots__ = ("_target", "_session")

    def __init__(self, target: Any, session: SteeringSession) -> None:
        # Never stack: a block that runs another block arrives with the outer
        # wrapper already installed, and two layers would cache each dispatch
        # under two different step counters.
        while isinstance(target, MemoisedDispatch):
            target = target._target
        self._target = target
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return _MemoisedNamespace(
            getattr(self._target, name),
            self._session,
            name,
        )


# ---------------------------------------------------------------------------
# Out-of-process dispatch
# ---------------------------------------------------------------------------
def _targets_running_block(request: InterruptionRequest, source: str) -> bool:
    """Whether a correction is aimed at the block running out-of-process.

    In-process, ``_int(name)`` fires only inside the function a patch names.
    The parent cannot see which function a child process is inside, so the
    nearest sound reading is "the patch names a function this block defines":
    firing early costs one replay-backed retry, whereas not firing would let
    the remaining dispatches run under a correction that asked them not to.

    Source that is not Python — a shell script — defines nothing a patch can
    name, so nothing fires; the patch author likewise refuses to author
    patches for it. Shell tops out at pause and replay.
    """
    if not request.patches:
        return False
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    return any(patch.function_name in defined for patch in request.patches)


async def dispatch_with_steering(
    session: Optional[SteeringSession],
    tool: str,
    kwargs: dict,
    dispatch: typing.Callable[[], typing.Awaitable[Any]],
) -> Any:
    """One out-of-process dispatch, steered from the parent side.

    A child process making a ``primitives.*`` call is blocked until the
    parent replies, which makes the reply the checkpoint: pause holds here,
    a pending correction interrupts here, and a dispatch the previous attempt
    already made replays from the cache instead of running again. The child
    needs no instrumentation for any of this — the suspension is a property
    of the RPC protocol itself.

    ``tool`` is the RPC path (``contacts.ask``), which is the same string the
    in-process :class:`MemoisedDispatch` records, so cache entries mean the
    same thing whichever side of the process boundary made them.

    Raises :class:`ControlledInterruption` when a pending correction targets
    the running block. The caller owns translating that into an
    ``rpc_interrupt`` reply so the child unwinds, and re-raising it into the
    retry loop; the request itself stays pending, because consuming it is
    :func:`run_with_steering`'s job.
    """
    if session is None:
        return await dispatch()
    await session.cp(f"RPC: {tool}")
    request = session.interruption
    if request is not None and _targets_running_block(request, session.source):
        raise ControlledInterruption(request.reason or "steered")
    key = make_cache_key(session, tool, (), kwargs)
    hit = session.cache.get(key)
    if hit is not None:
        return hit["result"]
    result = await dispatch()
    session.cache.put(key, result, tool=tool)
    return result


# ---------------------------------------------------------------------------
# Binding and retry
# ---------------------------------------------------------------------------
_MISSING = object()

#: Namespaces whose calls are memoised and bracketed. Anything outside these is
#: ordinary code: instrumenting every library call would bury the program in
#: probes, and memoising one would claim an effect happened that we cannot
#: describe well enough to skip safely.
DEFAULT_TOOL_NAMESPACES = frozenset({"primitives", "actor", "computer_primitives"})


def bind_session(
    global_state: Dict[str, Any],
    session: SteeringSession,
    *,
    tool_namespaces: typing.Iterable[str] = DEFAULT_TOOL_NAMESPACES,
) -> Dict[str, Any]:
    """Install the probes and memoised namespaces for one call.

    Returns a token for :func:`restore_session`. Values are saved and put back
    rather than deleted, so a nested call restores the outer block's session
    instead of clearing it.
    """
    names = [CP_FN, INT_FN, AROUND_CP_FN, RUNTIME_GLOBAL, SESSION_GLOBAL]
    previous: Dict[str, Any] = {n: global_state.get(n, _MISSING) for n in names}

    global_state[CP_FN] = session.cp
    global_state[INT_FN] = session.interrupt_point
    global_state[AROUND_CP_FN] = session.around
    global_state[RUNTIME_GLOBAL] = session.runtime
    global_state[SESSION_GLOBAL] = session

    for namespace in tool_namespaces:
        target = global_state.get(namespace)
        if target is None:
            continue
        previous[namespace] = target
        global_state[namespace] = MemoisedDispatch(target, session)

    return previous


def restore_session(global_state: Dict[str, Any], token: Dict[str, Any]) -> None:
    """Undo :func:`bind_session`."""
    for name, value in token.items():
        if value is _MISSING:
            global_state.pop(name, None)
        else:
            global_state[name] = value


#: The session for the call currently in flight.
#:
#: Binding by sandbox object does not work: only one execution path
#: (``state_mode="stateful"``, ``session_id=0``) runs in the sandbox that was
#: current when the tool started. Stateless — the default — builds a fresh one,
#: and pooled modes fetch another, so a session bound at tool entry would never
#: be seen by the code that actually runs. The contextvar follows the call
#: instead, and whichever sandbox executes binds it into its own globals.
_ACTIVE_SESSION: contextvars.ContextVar[Optional[SteeringSession]] = (
    contextvars.ContextVar("unify_steering_session", default=None)
)


@contextlib.contextmanager
def use_session(session: Optional[SteeringSession]) -> typing.Iterator[None]:
    """Mark *session* as steering the call being made inside this block."""
    token = _ACTIVE_SESSION.set(session)
    try:
        yield
    finally:
        _ACTIVE_SESSION.reset(token)


def active_session() -> Optional[SteeringSession]:
    """The session steering the current call, if there is one."""
    return _ACTIVE_SESSION.get()


def current_session(global_state: Dict[str, Any]) -> Optional[SteeringSession]:
    session = global_state.get(SESSION_GLOBAL)
    return session if isinstance(session, SteeringSession) else None


class _FunctionReplacer(ast.NodeTransformer):
    """Swap one function definition for another, wherever it is defined."""

    def __init__(self, name: str, replacement: ast.stmt) -> None:
        self.name = name
        self.replacement = replacement
        self.replaced = False

    def _maybe(self, node: ast.AST) -> ast.AST:
        if getattr(node, "name", None) == self.name:
            self.replaced = True
            return self.replacement
        self.generic_visit(node)
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:  # noqa: N802
        return self._maybe(node)

    def visit_AsyncFunctionDef(
        self,
        node: ast.AsyncFunctionDef,
    ) -> ast.AST:  # noqa: N802
        return self._maybe(node)


def splice_patch(source: str, patch: Patch) -> Optional[str]:
    """Return *source* with ``patch.function_name`` replaced, or None.

    The patch has to go into the source rather than into the namespace.
    Re-running a block re-executes its own ``def`` statements, so a function
    injected into globals is overwritten by the original the moment the retry
    starts — the correction would apply for exactly zero statements.

    None means the correction could not be placed: no source, unparseable, or
    naming a function this block does not define. All three are failed
    corrections the caller must surface rather than retry into identical code.
    """
    if not patch.source.strip():
        return None
    try:
        replacement_tree = ast.parse(patch.source)
        target_tree = ast.parse(source)
    except SyntaxError:
        logger.warning("steering: patch for %s does not parse", patch.function_name)
        return None

    replacement = next(
        (
            node
            for node in replacement_tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == patch.function_name
        ),
        None,
    )
    if replacement is None:
        logger.warning("steering: patch does not define %s", patch.function_name)
        return None

    replacer = _FunctionReplacer(patch.function_name, replacement)
    patched = replacer.visit(target_tree)
    if not replacer.replaced:
        logger.warning(
            "steering: %s is not defined in this block",
            patch.function_name,
        )
        return None

    ast.fix_missing_locations(patched)
    return ast.unparse(patched)


async def run_with_steering(
    source: str,
    run_source: typing.Callable[[str], typing.Awaitable[Any]],
    *,
    session: SteeringSession,
    max_retries: int = MAX_RETRIES,
) -> Any:
    """Run *source*, splicing in corrections and re-running until it completes.

    Each ``ControlledInterruption`` replaces the targeted function in the
    source, evicts whatever the correction says is stale, resets the execution
    position, and hands the new source back to *run_source*.

    The position resets but the cache does not, and that asymmetry is the
    whole mechanism. Keeping the position would look up the replayed prefix
    under keys from the run that was abandoned; clearing the cache would
    re-execute side effects that have already happened.
    """
    current = source
    attempt = 0
    while True:
        try:
            session.bind_source(current)
            return await run_source(current)
        except ControlledInterruption as interruption:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(
                    f"steering: gave up after {max_retries} corrections "
                    f"(last: {interruption})",
                ) from interruption

            request = session.interruption
            session.interruption = None
            applied_any = False
            for patch in request.patches if request else ():
                spliced = splice_patch(current, patch)
                if spliced is None:
                    continue
                current = spliced
                session.cache.invalidate(patch.invalidate)
                session.note_applied(patch)
                applied_any = True

            if not applied_any:
                # Re-running unchanged code reproduces the same interruption.
                raise RuntimeError(
                    f"steering: correction could not be applied ({interruption})",
                ) from interruption

            session.retries = attempt
            session.notify(
                {
                    "type": "steering_retry",
                    "message": (
                        "Correction applied; re-running from the first change. "
                        f"({interruption})"
                    ),
                    "progress": session.progress(),
                },
            )
            session.runtime.reset_position()
