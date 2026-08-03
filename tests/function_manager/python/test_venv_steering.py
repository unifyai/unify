"""Steering venv-backed execution from the parent side of the RPC channel.

Out-of-process code needs no instrumentation to be steered at its side
effects: every ``primitives.*`` call round-trips to the parent and blocks the
child until the reply arrives, so the reply itself is the checkpoint. These
tests assert that the parent memoises those dispatches for replay, interrupts
the child at a dispatch when a correction lands, and re-runs the patched
source — on both the one-shot subprocess path and the pooled
persistent-connection path, since the pooled path is the one that would no-op
silently if its handler were missed.

Between dispatches the child is invisible to the RPC channel, so the parent
additionally ships instrumented source and pushes interrupt directives over a
control channel; the tests at the end pin that a correction reaches a loop
that makes no primitive call at all.

The mechanism is asserted with the patch supplied directly; whether a real
model writes a usable one is the eval question, covered on the in-process
paths.
"""

from __future__ import annotations

import asyncio
import shutil
from typing import List

import pytest

from tests.helpers import _handle_project
from unify.common.context_registry import ContextRegistry
from unify.function_manager.function_manager import FunctionManager, VenvPool
from unify.function_manager.steering import (
    ControlledInterruption,
    InterruptionRequest,
    Patch,
    SteeringSession,
    interrupt_directive,
    use_session,
)

MINIMAL_VENV_CONTENT = """
[project]
name = "test-steering-venv"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []
""".strip()

IMPLEMENTATION = (
    "async def notify_vendors(vendors):\n"
    "    sent = []\n"
    "    for v in vendors:\n"
    "        sent.append(await primitives.comms.send(to=v))\n"
    "    return sent\n"
)

VENDORS = ["eu-alpha", "us-beta", "us-gamma", "eu-delta"]

EU_ONLY_PATCH = (
    "async def notify_vendors(vendors):\n"
    "    sent = []\n"
    "    for v in vendors:\n"
    "        if v.startswith('eu-'):\n"
    "            sent.append(await primitives.comms.send(to=v))\n"
    "    return sent\n"
)


class _Comms:
    def __init__(self) -> None:
        self.sent: List[str] = []

    async def send(self, to: str) -> str:
        self.sent.append(to)
        # Yield long enough for a watcher task to react between the dispatch
        # and the reply reaching the child.
        await asyncio.sleep(0.01)
        return f"sent:{to}"


class _Prims:
    def __init__(self, comms: _Comms) -> None:
        self.comms = comms


def _bare_manager() -> FunctionManager:
    """A FunctionManager without its constructor's I/O.

    ``_handle_rpc_call`` reads nothing from the instance, so the RPC-boundary
    unit tests can avoid the per-test context setup entirely.
    """
    return FunctionManager.__new__(FunctionManager)


def _patch_after(comms: _Comms, count: int, queue: asyncio.Queue, text: str):
    async def _run() -> None:
        while len(comms.sent) < count:
            await asyncio.sleep(0)
        await queue.put(text)

    return asyncio.create_task(_run())


async def _author(*, interjections, session):
    return InterruptionRequest(
        reason=interjections[0],
        patches=[Patch(function_name="notify_vendors", source=EU_ONLY_PATCH)],
    )


async def _stop_author(*, interjections, session):
    return InterruptionRequest(reason=interjections[0], stop=True)


@pytest.fixture
def function_manager_factory():
    """Factory fixture that creates FunctionManager instances."""
    managers = []

    def _create():
        ContextRegistry.forget(FunctionManager, "Functions/VirtualEnvs")
        ContextRegistry.forget(FunctionManager, "Functions/Compositional")
        ContextRegistry.forget(FunctionManager, "Functions/Primitives")
        ContextRegistry.forget(FunctionManager, "Functions/Meta")
        fm = FunctionManager()
        managers.append(fm)
        return fm

    yield _create

    for fm in managers:
        try:
            fm.clear()
        except Exception:
            pass


def _cleanup_venv(fm: FunctionManager, venv_id: int) -> None:
    venv_dir = fm._get_venv_dir(venv_id)
    if venv_dir.exists():
        shutil.rmtree(venv_dir, ignore_errors=True)


# ── the RPC boundary itself, no subprocess ──────────────────────────────────
@pytest.mark.asyncio
async def test_rpc_dispatch_runs_without_a_session():
    comms = _Comms()
    result = await _bare_manager()._handle_rpc_call(
        path="comms.send",
        kwargs={"to": "eu-alpha"},
        primitives=_Prims(comms),
    )
    assert result == "sent:eu-alpha"
    assert comms.sent == ["eu-alpha"]


@pytest.mark.asyncio
async def test_identical_dispatches_within_one_attempt_both_run():
    """Occurrence indexing: the second send(a) of a run is a new effect."""
    comms = _Comms()
    session = SteeringSession()
    with use_session(session):
        fm = _bare_manager()
        await fm._handle_rpc_call(
            path="comms.send",
            kwargs={"to": "eu-alpha"},
            primitives=_Prims(comms),
        )
        await fm._handle_rpc_call(
            path="comms.send",
            kwargs={"to": "eu-alpha"},
            primitives=_Prims(comms),
        )
    assert comms.sent == ["eu-alpha", "eu-alpha"]


@pytest.mark.asyncio
async def test_dispatches_replay_across_a_position_reset():
    """A retry resets position but keeps the cache, so the prefix replays."""
    comms = _Comms()
    session = SteeringSession()
    fm = _bare_manager()
    with use_session(session):
        first = await fm._handle_rpc_call(
            path="comms.send",
            kwargs={"to": "eu-alpha"},
            primitives=_Prims(comms),
        )
        session.runtime.reset_position()
        second = await fm._handle_rpc_call(
            path="comms.send",
            kwargs={"to": "eu-alpha"},
            primitives=_Prims(comms),
        )
    assert first == second == "sent:eu-alpha"
    assert comms.sent == ["eu-alpha"], "the replay re-dispatched"
    assert session.cache.hits == 1


@pytest.mark.asyncio
async def test_pending_correction_interrupts_at_the_dispatch_boundary():
    comms = _Comms()
    session = SteeringSession()
    session.bind_source(IMPLEMENTATION)
    session.interruption = InterruptionRequest(
        reason="only the EU vendors",
        patches=[Patch(function_name="notify_vendors", source=EU_ONLY_PATCH)],
    )
    with use_session(session):
        with pytest.raises(ControlledInterruption):
            await _bare_manager()._handle_rpc_call(
                path="comms.send",
                kwargs={"to": "us-gamma"},
                primitives=_Prims(comms),
            )
    assert comms.sent == [], "the dispatch ran despite the pending correction"
    assert session.interruption is not None, "the retry loop owns consuming it"


@pytest.mark.asyncio
async def test_correction_for_shell_source_does_not_fire():
    """Shell source defines nothing a patch can name, so nothing interrupts."""
    comms = _Comms()
    session = SteeringSession()
    session.bind_source("unity-primitive comms send --to eu-alpha\n")
    session.interruption = InterruptionRequest(
        reason="stop",
        patches=[Patch(function_name="notify_vendors", source=EU_ONLY_PATCH)],
    )
    with use_session(session):
        result = await _bare_manager()._handle_rpc_call(
            path="comms.send",
            kwargs={"to": "eu-alpha"},
            primitives=_Prims(comms),
        )
    assert result == "sent:eu-alpha"


# ── the full loop, one-shot subprocess ──────────────────────────────────────
@_handle_project
@pytest.mark.asyncio
async def test_correction_reaches_a_one_shot_venv_run(function_manager_factory):
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_author)
    steerer = _patch_after(comms, 2, queue, "only the EU vendors")

    try:
        with use_session(session):
            out = await fm.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                primitives=_Prims(comms),
            )
        await steerer

        assert out["error"] is None, out["error"]
        assert session.retries == 1
        # The correction landed before this one.
        assert "us-gamma" not in comms.sent
        # Already contacted, so replayed rather than contacted again.
        assert comms.sent.count("eu-alpha") == 1
        assert "eu-delta" in comms.sent
        assert session.cache.hits >= 1, "the retry redid the prefix"
        # The patched run's own return value: the replayed send plus the one
        # the patch let through.
        assert out["result"] == ["sent:eu-alpha", "sent:eu-delta"]
    finally:
        _cleanup_venv(fm, venv_id)


@_handle_project
@pytest.mark.asyncio
async def test_one_shot_venv_run_is_unchanged_without_a_session(
    function_manager_factory,
):
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    comms = _Comms()
    try:
        out = await fm.execute_in_venv(
            venv_id=venv_id,
            implementation=IMPLEMENTATION,
            call_kwargs={"vendors": list(VENDORS)},
            is_async=True,
            primitives=_Prims(comms),
        )
        assert out["error"] is None, out["error"]
        assert comms.sent == VENDORS
    finally:
        _cleanup_venv(fm, venv_id)


@_handle_project
@pytest.mark.asyncio
async def test_stop_ends_a_one_shot_venv_run_cleanly(function_manager_factory):
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait("cancel the sends")
    session = SteeringSession(interject_q=queue, patch_author=_stop_author)

    try:
        with use_session(session):
            out = await fm.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                primitives=_Prims(comms),
            )

        assert out["error"] is None
        assert out["result"] == {
            "status": "stopped",
            "reason": "cancel the sends",
        }
        assert comms.sent == []
    finally:
        _cleanup_venv(fm, venv_id)


# ── the full loop, pooled persistent connection ─────────────────────────────
@_handle_project
@pytest.mark.asyncio
async def test_correction_reaches_a_pooled_venv_run(function_manager_factory):
    """The pooled path has its own RPC loop; missing it looks wired and does
    nothing, so it is exercised specifically."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    await fm.prepare_venv(venv_id=venv_id)
    pool = VenvPool()

    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_author)
    steerer = _patch_after(comms, 2, queue, "only the EU vendors")

    try:
        with use_session(session):
            out = await pool.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                session_id=0,
                primitives=_Prims(comms),
                function_manager=fm,
            )
        await steerer

        assert out["error"] is None, out["error"]
        assert session.retries == 1
        assert "us-gamma" not in comms.sent
        assert comms.sent.count("eu-alpha") == 1
        assert "eu-delta" in comms.sent
        assert session.cache.hits >= 1, "the retry redid the prefix"
        assert out["result"] == ["sent:eu-alpha", "sent:eu-delta"]
    finally:
        await pool.close()
        _cleanup_venv(fm, venv_id)


@_handle_project
@pytest.mark.asyncio
async def test_stop_ends_a_pooled_venv_run_cleanly(function_manager_factory):
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    await fm.prepare_venv(venv_id=venv_id)
    pool = VenvPool()
    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait("cancel the sends")
    session = SteeringSession(interject_q=queue, patch_author=_stop_author)

    try:
        with use_session(session):
            out = await pool.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                session_id=0,
                primitives=_Prims(comms),
                function_manager=fm,
            )

        assert out["error"] is None
        assert out["result"] == {
            "status": "stopped",
            "reason": "cancel the sends",
        }
        assert comms.sent == []
    finally:
        await pool.close()
        _cleanup_venv(fm, venv_id)


# ── corrections between dispatches (instrumented child source) ─────────────
COMPUTE_LOOP_IMPLEMENTATION = (
    "async def crunch_numbers():\n"
    "    await primitives.comms.send(to='started')\n"
    "    total = 0\n"
    "    for i in range(2000):\n"
    "        total += i\n"
    "        await asyncio.sleep(0.005)\n"
    "    await primitives.comms.send(to='finished')\n"
    "    return total\n"
)

CUT_SHORT_PATCH = (
    "async def crunch_numbers():\n"
    "    await primitives.comms.send(to='started')\n"
    "    await primitives.comms.send(to='cut-short')\n"
    "    return -1\n"
)


async def _cut_short_author(*, interjections, session):
    return InterruptionRequest(
        reason=interjections[0],
        patches=[Patch(function_name="crunch_numbers", source=CUT_SHORT_PATCH)],
    )


def test_instrumented_source_carries_probes():
    from unify.function_manager.function_manager import _instrument_for_child

    shipped = _instrument_for_child(COMPUTE_LOOP_IMPLEMENTATION)
    assert "_cp(" in shipped and "_int(" in shipped


def test_unparseable_source_ships_unchanged():
    from unify.function_manager.function_manager import _instrument_for_child

    source = "def broken(\n"
    assert _instrument_for_child(source) == source


@pytest.mark.asyncio
async def test_child_int_shim_raises_only_for_targeted_functions():
    from unify.function_manager import venv_runner

    venv_runner._apply_control(
        {
            "type": "control",
            "action": "interrupt",
            "reason": "stop",
            "functions": ["crunch_numbers"],
        },
    )
    try:
        with pytest.raises(venv_runner.ControlledInterruption):
            await venv_runner._int("crunch_numbers")
        await venv_runner._int("unrelated")
        await venv_runner._cp("still a plain yield point")
    finally:
        venv_runner._clear_interrupt()


@pytest.mark.asyncio
async def test_child_checkpoint_raises_for_a_stop_directive():
    from unify.function_manager import venv_runner

    venv_runner._apply_control(
        {
            "type": "control",
            "action": "interrupt",
            "reason": "cancel the run",
            "functions": [],
            "stop": True,
        },
    )
    try:
        with pytest.raises(venv_runner.ControlledInterruption, match="cancel the run"):
            await venv_runner._cp("before a bare statement")
    finally:
        venv_runner._clear_interrupt()


@pytest.mark.asyncio
async def test_relay_sends_one_directive_when_a_correction_targets():
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_cut_short_author)
    sent: list = []

    async def _send_control(request):
        sent.append(interrupt_directive(request))

    relay = asyncio.create_task(
        session.relay_corrections(
            COMPUTE_LOOP_IMPLEMENTATION,
            _send_control,
            poll_interval=0.01,
        ),
    )
    await queue.put("cut it short")
    await asyncio.wait_for(relay, timeout=2.0)

    assert len(sent) == 1
    directive = sent[0]
    assert directive["action"] == "interrupt"
    assert directive["functions"] == ["crunch_numbers"]
    assert session.interruption is not None, "the retry loop owns consuming it"


@pytest.mark.asyncio
async def test_relay_ignores_corrections_that_target_nothing_here():
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_cut_short_author)
    sent: list = []

    async def _send_control(request):
        sent.append(interrupt_directive(request))

    relay = asyncio.create_task(
        session.relay_corrections(
            "async def other_function():\n    return 1\n",
            _send_control,
            poll_interval=0.01,
        ),
    )
    await queue.put("cut it short")
    await asyncio.sleep(0.1)
    relay.cancel()
    with pytest.raises(asyncio.CancelledError):
        await relay

    assert sent == []


@_handle_project
@pytest.mark.asyncio
async def test_correction_reaches_a_non_dispatching_loop_one_shot(
    function_manager_factory,
):
    """The loop makes no primitive call, so only an instrumented checkpoint
    fed by the control channel can see the correction arrive."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)

    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_cut_short_author)
    steerer = _patch_after(comms, 1, queue, "cut it short")

    try:
        with use_session(session):
            out = await fm.execute_in_venv(
                venv_id=venv_id,
                implementation=COMPUTE_LOOP_IMPLEMENTATION,
                call_kwargs={},
                is_async=True,
                primitives=_Prims(comms),
            )
        await steerer

        assert out["error"] is None, out["error"]
        assert session.retries == 1
        # The loop was cut short: its trailing dispatch never ran.
        assert "finished" not in comms.sent
        # Replayed, not re-sent.
        assert comms.sent.count("started") == 1
        assert "cut-short" in comms.sent
        assert out["result"] == -1
        assert session.cache.hits >= 1, "the retry redid the prefix"
    finally:
        _cleanup_venv(fm, venv_id)


@_handle_project
@pytest.mark.asyncio
async def test_correction_reaches_a_non_dispatching_loop_pooled(
    function_manager_factory,
):
    """Same, on the persistent child — whose interrupt state must also come
    back clean for the very next request on that connection."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    await fm.prepare_venv(venv_id=venv_id)
    pool = VenvPool()

    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_cut_short_author)
    steerer = _patch_after(comms, 1, queue, "cut it short")

    try:
        with use_session(session):
            out = await pool.execute_in_venv(
                venv_id=venv_id,
                implementation=COMPUTE_LOOP_IMPLEMENTATION,
                call_kwargs={},
                is_async=True,
                session_id=0,
                primitives=_Prims(comms),
                function_manager=fm,
            )
        await steerer

        assert out["error"] is None, out["error"]
        assert session.retries == 1
        assert "finished" not in comms.sent
        assert comms.sent.count("started") == 1
        assert "cut-short" in comms.sent
        assert out["result"] == -1

        # A fresh steered call on the same connection: instrumented probes
        # run against the same child, and a stale directive would interrupt
        # it immediately.
        later = _Comms()
        later_session = SteeringSession()
        with use_session(later_session):
            again = await pool.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                session_id=0,
                primitives=_Prims(later),
                function_manager=fm,
            )
        assert again["error"] is None, again["error"]
        assert later.sent == VENDORS, "a stale directive steered this call"
    finally:
        await pool.close()
        _cleanup_venv(fm, venv_id)


@_handle_project
@pytest.mark.asyncio
async def test_pooled_connection_outlives_the_session_that_steered_it(
    function_manager_factory,
):
    """The session follows the call, not the connection: a later call on the
    same pooled venv must run unsteered once the session is gone."""
    fm = function_manager_factory()
    venv_id = fm.add_venv(venv=MINIMAL_VENV_CONTENT)
    await fm.prepare_venv(venv_id=venv_id)
    pool = VenvPool()

    comms = _Comms()
    queue: asyncio.Queue = asyncio.Queue()
    session = SteeringSession(interject_q=queue, patch_author=_author)
    steerer = _patch_after(comms, 2, queue, "only the EU vendors")

    try:
        with use_session(session):
            await pool.execute_in_venv(
                venv_id=venv_id,
                implementation=IMPLEMENTATION,
                call_kwargs={"vendors": list(VENDORS)},
                is_async=True,
                session_id=0,
                primitives=_Prims(comms),
                function_manager=fm,
            )
        await steerer

        later = _Comms()
        out = await pool.execute_in_venv(
            venv_id=venv_id,
            implementation=IMPLEMENTATION,
            call_kwargs={"vendors": list(VENDORS)},
            is_async=True,
            session_id=0,
            primitives=_Prims(later),
            function_manager=fm,
        )
        assert out["error"] is None, out["error"]
        assert later.sent == VENDORS, "a dead session still steered this call"
    finally:
        await pool.close()
        _cleanup_venv(fm, venv_id)
