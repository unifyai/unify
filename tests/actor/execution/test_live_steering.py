"""Deterministic contracts for steering code while it is still running.

Locks the three pieces that let a correction reach a block mid-flight: the
AST instrumentation that places checkpoints, the channel those checkpoints
read, and the dispatch proxy that covers calls the instrumentation cannot
see from outside a statement.

Everything here is symbolic — no LLM is involved, so a failure is a
regression in the machinery rather than a judgement call.
"""

from __future__ import annotations

import ast
import asyncio
import inspect

import pytest

from unify.actor.code_act_actor import CodeActActor
from unify.actor.execution import PythonExecutionSession
from unify.actor.execution.steering import (
    CHANNEL_GLOBAL,
    CHECKPOINT_FN,
    CHECKPOINT_SYNC_FN,
    SteeringChannel,
    SteeringDispatchProxy,
    bind_sandbox_steering_channel,
    current_channel,
    instrument_for_steering,
    restore_sandbox_steering_channel,
)


def _instrumented(source: str) -> str:
    return ast.unparse(instrument_for_steering(ast.parse(source)))


# ── instrumentation ────────────────────────────────────────────────────────
def test_top_level_statements_are_bracketed():
    out = _instrumented("a = 1\nb = 2\n")
    assert out.count(f"await {CHECKPOINT_FN}") == 2


def test_loop_bodies_are_instrumented():
    # The failure this exists for: repeated side effects inside one
    # statement, where a checkpoint placed only between top-level statements
    # would never run while the work was happening.
    out = _instrumented("for v in vendors:\n    send(v)\n")
    assert f"for v in vendors:\n    await {CHECKPOINT_FN}" in out


def test_checkpoint_precedes_the_iteration_body():
    """A correction must land before the next side effect, not after it."""
    out = _instrumented("for v in vendors:\n    send(v)\n")
    body = out.split("for v in vendors:\n")[1].splitlines()
    assert body[0].strip().startswith(f"await {CHECKPOINT_FN}")
    assert body[1].strip() == "send(v)"


def test_nested_loops_are_instrumented_at_every_depth():
    out = _instrumented("for a in xs:\n    for b in ys:\n        f(a, b)\n")
    assert out.count(f"await {CHECKPOINT_FN}") == 3  # top-level + two bodies


def test_while_loops_are_instrumented():
    out = _instrumented("while more():\n    step()\n")
    assert f"while more():\n    await {CHECKPOINT_FN}" in out


def test_sync_function_bodies_use_the_sync_checkpoint():
    """A ``def`` cannot await, so it gets the variant that cannot suspend."""
    out = _instrumented("def helper():\n    for i in xs:\n        f(i)\n")
    inner = out.split("def helper")[1]
    assert CHECKPOINT_SYNC_FN in inner
    assert f"await {CHECKPOINT_FN}(" not in inner


def test_async_function_bodies_use_the_async_checkpoint():
    out = _instrumented("async def helper():\n    for i in xs:\n        f(i)\n")
    inner = out.split("async def helper")[1]
    assert f"await {CHECKPOINT_FN}" in inner


def test_class_bodies_are_left_alone():
    out = _instrumented("class C:\n    for i in xs:\n        pass\n")
    assert CHECKPOINT_FN not in out.split("class C")[1]


def test_instrumented_code_compiles_inside_the_async_wrapper():
    """The sandbox indents the block into an ``async def``; awaits must fit."""
    out = _instrumented(
        "a = 1\nfor i in xs:\n    a += i\ndef h():\n    return a\nh()\n",
    )
    wrapped = "async def __w():\n" + "".join(f"    {ln}\n" for ln in out.splitlines())
    compile(wrapped, "<test>", "exec")


def test_empty_module_is_unchanged():
    assert _instrumented("") == ""


# ── the channel ────────────────────────────────────────────────────────────
def test_drain_is_non_blocking_and_ordered():
    q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=q)
    q.put_nowait("first")
    q.put_nowait("second")
    channel.checkpoint_sync(line=1)
    assert channel.messages == ["first", "second"]


def test_channel_without_a_queue_is_inert():
    channel = SteeringChannel(interject_q=None)
    channel.checkpoint_sync(line=3)
    assert channel.messages == []
    assert channel.active is False


def test_dict_payloads_are_reduced_to_their_text():
    q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=q)
    q.put_nowait({"content": "from a nested loop"})
    channel.checkpoint_sync()
    assert channel.messages == ["from a nested loop"]


def test_progress_quotes_the_line_without_the_repl_return():
    """REPL semantics rewrite a trailing expression; the model never wrote it."""
    channel = SteeringChannel(interject_q=None)
    channel.bind_source("x = compute()\nreturn summarise(x)")
    channel.checkpoint_sync(line=2)
    assert channel.progress()["last_statement"] == "summarise(x)"


# ── binding ────────────────────────────────────────────────────────────────
def test_bind_and_restore_leaves_globals_as_found():
    global_state: dict = {"unrelated": 1}
    channel = SteeringChannel(interject_q=None)
    token = bind_sandbox_steering_channel(global_state, channel)
    assert current_channel(global_state) is channel
    assert callable(global_state[CHECKPOINT_FN])
    restore_sandbox_steering_channel(global_state, token)
    assert global_state == {"unrelated": 1}
    assert current_channel(global_state) is None


def test_nested_binding_restores_the_outer_channel():
    global_state: dict = {}
    outer = SteeringChannel(interject_q=None)
    inner = SteeringChannel(interject_q=None)
    outer_token = bind_sandbox_steering_channel(global_state, outer)
    inner_token = bind_sandbox_steering_channel(global_state, inner)
    assert current_channel(global_state) is inner
    restore_sandbox_steering_channel(global_state, inner_token)
    assert current_channel(global_state) is outer
    restore_sandbox_steering_channel(global_state, outer_token)
    assert global_state.get(CHANNEL_GLOBAL) is None


# ── end to end through the sandbox ─────────────────────────────────────────
@pytest.mark.asyncio
async def test_block_without_a_channel_is_not_instrumented():
    """No channel means no checkpoints and no behaviour change at all."""
    session = PythonExecutionSession()
    try:
        out = await session.execute(
            "total = 0\nfor i in range(5):\n    total += i\ntotal",
        )
    finally:
        await session.close()
    assert out["error"] is None
    assert out["result"] == 10


@pytest.mark.asyncio
async def test_interjection_reaches_a_running_loop():
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    notify_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(
        interject_q=interject_q,
        notification_q=notify_q,
        suspend_timeout=5.0,
    )
    token = bind_sandbox_steering_channel(session.global_state, channel)

    reached: list[int] = []
    session.global_state["record"] = reached.append

    async def steer() -> None:
        # Correct only once the loop is genuinely under way, then wait for the
        # block to actually suspend before resuming it. Sending both together
        # would let one drain take both, which is correct behaviour but tests
        # nothing about the suspend.
        while len(reached) < 2:
            await asyncio.sleep(0)
        await interject_q.put("narrow it to the EU vendors")
        while channel.progress()["suspensions"] < 1:
            await asyncio.sleep(0)
        await interject_q.put("continue")

    steerer = asyncio.create_task(steer())
    try:
        out = await session.execute(
            "import asyncio\n"
            "for i in range(6):\n"
            "    record(i)\n"
            "    await asyncio.sleep(0.02)\n",
        )
        await steerer
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["error"] is None
    assert channel.messages == ["narrow it to the EU vendors", "continue"]
    progress = channel.progress()
    assert progress["suspensions"] == 1
    assert progress["last_line_reached"] is not None
    # The loop ran to completion: resuming means resuming, not restarting.
    assert reached == list(range(6))

    payload = notify_q.get_nowait()
    assert payload["type"] == "steering_checkpoint"
    assert payload["interjections"] == ["narrow it to the EU vendors"]
    assert payload["progress"]["last_line_reached"] is not None


@pytest.mark.asyncio
async def test_suspension_holds_the_block_until_a_decision_arrives():
    """The point of suspending: no further side effect until the model rules."""
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=5.0)
    token = bind_sandbox_steering_channel(session.global_state, channel)

    sent: list[int] = []
    session.global_state["send"] = sent.append
    interject_q.put_nowait("wait")

    exec_task = asyncio.create_task(
        session.execute(
            "import asyncio\nfor i in range(4):\n    send(i)\n    await asyncio.sleep(0)\n",
        ),
    )
    try:
        # The first checkpoint is at the top of the body, before any send.
        await asyncio.sleep(0.1)
        assert sent == [], "block ran past the checkpoint without a decision"

        await interject_q.put("go ahead")
        out = await exec_task
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["error"] is None
    assert sent == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_cancellation_unwinds_a_suspended_block():
    """What ``stop_<tool>_<call_id>`` does: cancel the task under the await."""
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=30.0)
    token = bind_sandbox_steering_channel(session.global_state, channel)
    interject_q.put_nowait("stop what you are doing")

    exec_task = asyncio.create_task(
        session.execute(
            "import asyncio\nfor i in range(50):\n    await asyncio.sleep(0.01)\n",
        ),
    )
    try:
        await asyncio.sleep(0.1)
        exec_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await exec_task
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    # Progress survives the cancellation, so the next turn is not blind.
    assert channel.progress()["checkpoints_passed"] > 0
    assert "stop what you are doing" in channel.messages


@pytest.mark.asyncio
async def test_suspension_resumes_rather_than_stranding_the_loop():
    """A turn that answers in prose must not deadlock the block forever."""
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=0.2)
    token = bind_sandbox_steering_channel(session.global_state, channel)
    interject_q.put_nowait("something")

    try:
        out = await session.execute("x = 1\nx + 1")
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["result"] == 2
    assert any("no steering decision" in m for m in channel.messages)


@pytest.mark.asyncio
async def test_generated_code_can_read_the_interjections():
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=0.2)
    token = bind_sandbox_steering_channel(session.global_state, channel)
    interject_q.put_nowait("only the EU ones")

    try:
        out = await session.execute("x = 1\nlist(steering.messages)")
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["error"] is None
    assert "only the EU ones" in out["result"]


@pytest.mark.asyncio
async def test_dispatch_proxy_checkpoints_inside_a_comprehension():
    """One statement, many dispatches — invisible to statement checkpoints."""
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=0.2)
    token = bind_sandbox_steering_channel(session.global_state, channel)

    sent: list[str] = []

    class _Comms:
        async def send(self, to: str) -> str:
            sent.append(to)
            return f"sent:{to}"

    class _Prims:
        comms = _Comms()

    session.global_state["primitives"] = SteeringDispatchProxy(_Prims(), channel)
    interject_q.put_nowait("hold on")

    try:
        out = await session.execute(
            "vendors = ['a', 'b', 'c']\n"
            "[await primitives.comms.send(v) for v in vendors]\n",
        )
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["error"] is None
    assert channel.progress()["suspensions"] >= 1
    assert sent == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_stored_function_body_is_steerable_when_synthesised():
    """A function's own loop is steerable, not just the call that starts it.

    ``execute_function`` prepends the stored implementation as a preamble when
    the function is not already in the namespace, so the ``def`` reaches the
    sandbox as source and is instrumented like any other block. That makes the
    inside of a long-running stored function reachable, rather than only the
    boundary around it.
    """
    session = PythonExecutionSession()
    interject_q: asyncio.Queue = asyncio.Queue()
    channel = SteeringChannel(interject_q=interject_q, suspend_timeout=5.0)
    token = bind_sandbox_steering_channel(session.global_state, channel)

    sent: list[str] = []
    session.global_state["record"] = sent.append
    interject_q.put_nowait("only the first two")

    preamble_style_code = (
        "async def notify_all(targets):\n"
        "    import asyncio\n"
        "    for t in targets:\n"
        "        record(t)\n"
        "        await asyncio.sleep(0)\n"
        "    return len(targets)\n"
        "await notify_all(['a', 'b', 'c'])\n"
    )

    async def resume_once() -> None:
        while channel.progress()["suspensions"] < 1:
            await asyncio.sleep(0)
        await interject_q.put("go on")

    resumer = asyncio.create_task(resume_once())
    try:
        out = await session.execute(preamble_style_code)
        await resumer
    finally:
        restore_sandbox_steering_channel(session.global_state, token)
        await session.close()

    assert out["error"] is None
    assert out["result"] == 3
    # The suspend happened inside the function body, before its first record.
    assert channel.progress()["suspensions"] == 1
    assert "only the first two" in channel.messages


def test_dispatch_proxy_passes_non_callables_through():
    channel = SteeringChannel(interject_q=None)

    class _Manager:
        label = "contacts"

    class _Prims:
        contacts = _Manager()

    proxy = SteeringDispatchProxy(_Prims(), channel)
    assert proxy.contacts.label == "contacts"


# ── the tool surface ───────────────────────────────────────────────────────
def test_execute_tools_declare_the_interject_queue():
    """The loop keys steerability off the signature, before the tool returns.

    ``ToolsData`` sets ``is_interjectable`` from ``_interject_queue`` being in
    the signature at schedule time, which is what makes the dynamic
    ``interject_<tool>_<call_id>`` helper exist while the block is still
    running rather than only after it returns a handle.
    """
    actor = CodeActActor.__new__(CodeActActor)
    for name in ("execute_code", "execute_function"):
        fn = getattr(actor, name, None)
        if fn is None:
            continue
        assert "_interject_queue" in inspect.signature(fn).parameters


@pytest.mark.asyncio
async def test_call_binding_yields_a_channel_and_restores_globals():
    from unify.actor.execution import _CURRENT_SANDBOX

    session = PythonExecutionSession()
    sandbox_token = _CURRENT_SANDBOX.set(session)
    interject_q: asyncio.Queue = asyncio.Queue()
    try:
        with CodeActActor._sandbox_call_binding(
            clarification_up_q=None,
            clarification_down_q=None,
            interject_q=interject_q,
            notification_q=None,
        ) as channel:
            assert isinstance(channel, SteeringChannel)
            assert current_channel(session.global_state) is channel
        assert current_channel(session.global_state) is None
    finally:
        _CURRENT_SANDBOX.reset(sandbox_token)
        await session.close()
