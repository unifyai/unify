from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional
from contextlib import suppress
from .tools_data import ToolsData


class SteerAction(str, Enum):
    """The complete steering surface, unified behind one static dispatcher."""

    STOP = "stop"
    INTERJECT = "interject"
    PAUSE = "pause"
    RESUME = "resume"
    CLARIFY = "clarify"
    CALL = "call"
    ASK = "ask"


STEER_DOC = """Steer a currently-tracked call: stop it, interject guidance, pause/resume it, \
answer a pending clarification, invoke a custom method on its handle, or ask a read-only \
question about its live progress.

`call_id` is the id shown on the original tool call that launched the work (the same id \
that later appears on its `check_status`/progress/clarification tail messages). This tool \
is always available — if `call_id` no longer refers to a live, in-flight call (it already \
finished, or never existed), you get back an instructive error instead of a schema change.

Parameters
----------
call_id : str
    The id of the call to steer (see above).
action : "stop" | "interject" | "pause" | "resume" | "clarify" | "call" | "ask"
    stop      - Cancel the call. `payload` may carry an optional reason.
    interject - Inject additional guidance while the call keeps running.
                `payload` is the guidance text.
    pause     - Pause the call. Fails with an instructive error if it is already paused.
    resume    - Resume a paused call. Fails with an instructive error if it is not paused.
    clarify   - Answer a clarification the call is blocked on. `payload` is the answer text.
                Only valid while that call is actually waiting on a clarification.
    call      - Invoke a custom method the call's handle exposes beyond the core steering
                surface above. `method` names the method; `payload` is a JSON **object**
                string of keyword arguments matched against that method's real signature.
                Blocking: you get the method's return value once it completes, same as
                waiting on any other tool.
    ask       - Ask a read-only question about a call that is still running, to understand
                its live progress or intermediate reasoning without stopping it. `payload`
                is the question. Blocking: spins up a nested inspection turn and returns a
                short answer. Only valid while the call is still running — once it has
                completed, use `ask_about_completed_tool` instead (a different id namespace
                is not needed; the same call_id applies there too).
payload : str | None
    Action-specific content — see above. Omit when the action does not need one
    (stop's reason, pause, resume).
method : str | None
    Required only for action="call": the name of the custom method to invoke.
include_parent_context : bool
    Relevant for action="ask" and action="interject". For "ask", whether to
    pass the parent conversation's context into the nested inspection turn.
    For "interject", whether to forward the parent conversation's
    continuation context alongside the guidance text (only takes effect if
    the target call originally opted into context propagation). Defaults to
    true; set false when the extra context would just add cost with no
    benefit. Ignored by every other action.

Returns
-------
A status acknowledgement, or (for "call"/"ask") the underlying result once it completes.
"""

ASK_ABOUT_COMPLETED_TOOL_DOC = """Ask a follow-up question about a completed tool to \
understand its internal reasoning, intermediate steps, or any details not visible in the \
outer transcript.

Which tools are completed and askable is announced as they finish (look for \
"[askable <call_id>]" tail messages above) — this tool's own description never changes.

Parameters
----------
tool_id : str
    The call_id of the completed tool to query (see the "[askable ...]" announcements).
question : str
    The follow-up question to ask about the completed tool's execution.
"""


class DynamicToolFactory:

    @dataclass
    class _ToolContext:
        fn_name: str
        arg_repr: str
        call_id: str
        safe_call_id: str

    def __init__(self, tools_data: ToolsData):
        # Only ever holds the STATIC, byte-stable surface: wait, steer,
        # ask_about_completed_tool. Nothing here varies with what is
        # pending/completed/paused this turn.
        self.dynamic_tools: Dict[str, Callable] = {}
        # ask_* closures for handles that are still running, keyed by a
        # synthetic per-call name. These exist ONLY to seed recursive
        # inspection-loop tool schemas (ToolsData.get_ask_tools() ->
        # SteerableToolHandle.ask()'s nested loop) — they are never merged
        # into self.dynamic_tools, so they never reach the outer loop's own
        # visible schema (that would reintroduce the per-call-id churn this
        # dispatcher exists to remove).
        self.live_ask_fns: Dict[str, Callable] = {}
        self.tools_data = tools_data

    # Shared steering helpers – reduce duplication across dynamic helper tools
    @staticmethod
    def _adopt_signature_and_annotations(from_callable, to_wrapper) -> None:
        """Copy signature, annotations, and docstring from from_callable to to_wrapper.

        Notes
        -----
        - The 'self' parameter (if any) is stripped from both the signature and annotations.
        - If the source has a docstring, it is copied verbatim (stripped) onto the wrapper.
        - If the source method has no docstring, attempt to fall back to the first
          ancestor in the MRO that defines a docstring for a method with the same name.
        """
        try:
            src = getattr(from_callable, "__func__", from_callable)
            # Build a new signature that removes any leading 'self' parameter
            _sig = inspect.signature(src)
            try:
                _params = list(_sig.parameters.values())
            except Exception:
                _params = []
            try:
                _filtered_params = [p for p in _params if p.name != "self"]
            except Exception:
                _filtered_params = _params
            try:
                to_wrapper.__signature__ = inspect.Signature(
                    parameters=_filtered_params,
                    return_annotation=_sig.return_annotation,
                )
            except Exception:
                # Fallback: if building a filtered signature fails, at least set the original one
                to_wrapper.__signature__ = _sig
            try:
                ann = dict(getattr(src, "__annotations__", {}) or {})
                ann.pop("self", None)
                to_wrapper.__annotations__ = ann
            except Exception:
                pass
            try:
                doc = inspect.getdoc(src)
                if isinstance(doc, str) and doc.strip():
                    to_wrapper.__doc__ = doc.strip()
                else:
                    # Fallback: walk MRO to find a base-class method docstring
                    try:
                        name = getattr(src, "__name__", None) or getattr(
                            from_callable,
                            "__name__",
                            "",
                        )
                        owner_cls = getattr(
                            getattr(from_callable, "__self__", None),
                            "__class__",
                            None,
                        )
                        if isinstance(name, str) and name and owner_cls is not None:
                            for base in getattr(owner_cls, "__mro__", ())[1:]:
                                try:
                                    cand = getattr(base, name, None)
                                except Exception:
                                    cand = None
                                if cand is None:
                                    continue
                                fn_obj = getattr(cand, "__func__", cand)
                                base_doc = inspect.getdoc(fn_obj)
                                if isinstance(base_doc, str) and base_doc.strip():
                                    to_wrapper.__doc__ = base_doc.strip()
                                    break
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass

    @staticmethod
    def _discover_custom_public_methods(handle) -> dict[str, Callable]:
        """
        Return a mapping ``name → bound_method`` of *public* callables on *handle*:
            • name does **not** start with ``_``  _and_
            • name is not a core steering method defined on base async-tool loop handles
              (SteerableToolHandle, AsyncToolLoopHandle) — those are reached via
              ``steer``'s dedicated actions (stop/interject/pause/resume/clarify/ask),
              never via action="call".
        """

        def _management_method_names_for_handle(_h) -> set[str]:
            names: set[str] = set()
            mro = getattr(getattr(_h, "__class__", object), "__mro__", ())
            for base in mro:
                bmod = getattr(base, "__module__", "")
                bname = getattr(base, "__name__", "")
                if bmod == "unify.common.async_tool_loop" and bname in (
                    "SteerableToolHandle",
                    "AsyncToolLoopHandle",
                ):
                    for n, member in inspect.getmembers(base, inspect.isroutine):
                        if not n.startswith("_"):
                            names.add(n)
            return names

        management_names = _management_method_names_for_handle(handle)
        methods: dict[str, Callable] = {}
        for name, attr in inspect.getmembers(handle):
            if name.startswith("_") or name in management_names or not callable(attr):
                continue
            # Bind the method to *handle* (important for late-added attributes).
            try:
                bound = handle.__getattribute__(name)
            except Exception:
                # Attribute access raised – treat as non-callable.
                continue

            methods[name] = bound
        return methods

    # helper: register a freshly-minted coroutine as a *temporary* tool
    def _register_tool(
        self,
        func_name: str,
        fallback_doc: str,
        fn: Callable,
    ) -> None:
        # prefer the function's own docstring if it exists, else fall back
        existing = inspect.getdoc(fn)
        fn.__doc__ = existing.strip() if existing else fallback_doc
        fn.__name__ = func_name[:64]
        fn.__qualname__ = func_name[:64]
        self.dynamic_tools[func_name.lstrip("_")] = fn

    def _create_wait_tool(self) -> None:
        """
        Expose a single, always-present global helper tool `wait` that performs a no-op.

        Purpose
        -------
        Use this when you do not want to take any new action at this time.
        Calling `wait` explicitly instructs the agent to keep waiting for
        any currently running tool calls to finish (or for an interjection
        to arrive) before deciding whether to act next. It does not start,
        stop, pause, resume, or modify any in-flight work.

        If a clarification is currently pending on any call, `wait` is refused —
        answer it first via `steer(call_id=<id>, action="clarify", payload=<answer>)`.
        """

        async def _wait() -> Dict[str, str]:
            return {"status": "waiting"}

        self._register_tool(
            func_name="wait",
            fallback_doc=(
                "No-op: keep waiting on the currently running tool calls. "
                "Use this when you don't need to start/stop/pause/resume anything right now; "
                "decide what to do after the next tool completes or a new interjection arrives. "
                "Refused while a clarification is pending — answer it via "
                'steer(call_id=<id>, action="clarify", payload=<answer>) first.'
            ),
            fn=_wait,
        )

    def _create_steer_tool(self) -> None:
        """Expose the single static steering dispatcher (see STEER_DOC).

        This function is never actually invoked — loop.py special-cases the
        `steer` tool name for execution (same pattern as `wait` and
        `compress_context`). It exists purely so ``method_to_schema`` can
        derive a byte-stable JSON schema (including the action enum) from a
        real, typed Python signature.
        """

        async def steer(
            call_id: str,
            action: SteerAction,
            payload: Optional[str] = None,
            method: Optional[str] = None,
            include_parent_context: bool = True,
        ) -> Dict[str, Any]:
            return {"status": "unreachable"}

        self._register_tool(
            func_name="steer",
            fallback_doc=STEER_DOC,
            fn=steer,
        )

    def _create_ask_about_completed_tool(self) -> None:
        """Expose the single, frozen-docstring dispatcher for asking follow-up
        questions about tools that have already completed.

        The docstring no longer embeds a live listing of completed tools —
        that listing now arrives as appended "[askable <call_id>]" tail
        messages (see ToolsData.record_tool_completed_askable), which keeps
        this tool's schema bytes constant regardless of how many tools have
        completed. Execution is special-cased in loop.py, mirroring `steer`.
        """

        async def ask_about_completed_tool(tool_id: str, question: str) -> Any:
            return {"status": "unreachable"}

        self._register_tool(
            func_name="ask_about_completed_tool",
            fallback_doc=ASK_ABOUT_COMPLETED_TOOL_DOC,
            fn=ask_about_completed_tool,
        )

    def _refresh_task_capabilities(self, task: asyncio.Task) -> None:
        """Refresh per-task bookkeeping that steer()'s execution-time
        validation and the clarification/interjection plumbing depend on.

        This is the bookkeeping half of what used to be `_process_task` —
        the tool-minting half is gone (steer/wait/ask_about_completed_tool
        are static now), but the live state this tracks (is_interjectable,
        clarification queue wiring, an ask() closure for recursive
        inspection) still needs to be refreshed whenever a handle is
        adopted or changes mid-flight.
        """
        info = self.tools_data.info[task]
        handle = info.handle
        handle_available = handle is not None

        # ── DYNAMIC capability refresh (handle may change) ─────
        if handle_available:
            # 1. interjection
            info.is_interjectable = hasattr(handle, "interject")

            # 2. clarification queues
            h_up_q = getattr(
                handle,
                "clarification_up_q",
                info.clar_up_queue,
            )
            h_dn_q = getattr(
                handle,
                "clarification_down_q",
                info.clar_down_queue,
            )

            if (h_up_q is not None) ^ (h_dn_q is not None):
                raise AttributeError(
                    f"Handle of call {info.call_id} now exposes only one "
                    "of clarification queues; both or neither required.",
                )

            # update bookkeeping & channel map
            prev_up_q = info.clar_up_queue
            if h_up_q is not prev_up_q:
                # remove old mapping if any
                self.tools_data.clarification_channels.pop(info.call_id, None)
                if h_up_q is not None:
                    self.tools_data.clarification_channels[info.call_id] = (
                        h_up_q,
                        h_dn_q,
                    )
            info.clar_up_queue = h_up_q
            info.clar_down_queue = h_dn_q

        _call_id: str = info.call_id
        # Compact, sanitized suffix of the call_id for use in the synthetic
        # ask-closure key below (this key is internal — never surfaced to
        # the outer loop's own schema — so the 64-char tool-name budget
        # that used to constrain it no longer applies, but the shape is
        # kept for continuity with the closures' historical naming).
        _safe_call_id: str = _call_id.replace("-", "_").split("_")[-1][-8:]
        _fn_name: str = info.name

        # Synthetic `ask` closure retained ONLY for recursive inspection-loop
        # seeding (ToolsData.get_ask_tools()); never exposed as an outer tool.
        if handle_available and hasattr(handle, "ask"):
            try:
                _arg_dict = None
                import json as _json

                _arg_json = info.call_dict["function"]["arguments"]
                try:
                    _arg_dict = _json.loads(_arg_json)
                    _arg_repr = ", ".join(f"{k}={v!r}" for k, v in _arg_dict.items())
                except Exception:
                    _arg_repr = _arg_json
            except Exception:
                _arg_repr = ""

            async def _ask(_handle=handle, **_kw):
                from .messages import forward_handle_call as _forward_handle_call

                return await _forward_handle_call(
                    _handle,
                    "ask",
                    _kw,
                    fallback_positional_keys=["question"],
                )

            _ask.__doc__ = f"Ask a read-only question about the running call {_fn_name}({_arg_repr})."
            with suppress(Exception):
                self._adopt_signature_and_annotations(getattr(handle, "ask"), _ask)
            ask_key = f"ask_{_fn_name}_{_safe_call_id}"
            _ask.__name__ = ask_key[:64]
            _ask.__qualname__ = ask_key[:64]
            self.live_ask_fns[ask_key] = _ask
            self.tools_data._task_ask_keys[task] = ask_key

    def generate(self):
        # Refresh capability bookkeeping (interjectable flag, clarification
        # channel wiring, live-ask closures) for every pending task. This no
        # longer mints any outer-visible tools — sorting by call_idx is kept
        # only so `live_ask_fns` population order stays deterministic.
        for task in sorted(
            list(self.tools_data.pending),
            key=lambda t: getattr(self.tools_data.info.get(t), "call_idx", 0),
        ):
            self._refresh_task_capabilities(task)

        # The static, byte-stable surface: always present, every turn,
        # regardless of what is pending/completed/paused right now.
        self._create_wait_tool()
        self._create_steer_tool()
        self._create_ask_about_completed_tool()

        self.tools_data._live_ask_fns_ref = self.live_ask_fns
