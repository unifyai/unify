"""Unit tests for the sent-watermark append-only transcript invariant.

Covers the mechanism itself (is_mutable, generate_with_preprocess's watermark
advancement and dev-mode integrity assertion) plus every gated call site from
plan-append-only-transcript: the placeholder result write, notification
delivery, clarification delivery, insert_tool_message_after_assistant's
below-watermark gate, prune_wait_tool_call's below-watermark ack path, and
multi-handle child-result delivery. No LLM calls — these are symbolic tests of
the transcript-manipulation infrastructure, not of model behaviour.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from unify.common._async_tool.loop import ToolLoopRuntimeState, _LoopToolFailureTracker
from unify.common._async_tool.messages import (
    emit_completion_pair,
    ensure_placeholders_for_pending,
    generate_with_preprocess,
    insert_tool_message_after_assistant,
    is_mutable,
    prune_wait_tool_call,
)
from unify.common._async_tool.tools_data import ToolsData, _MultiHandleState
from unify.common._async_tool.tools_utils import (
    ToolCallMetadata,
    create_tool_call_message,
)
from tests.async_helpers import _is_bare_pending_tool_placeholder

# ---------------------------------------------------------------------------
# Fakes — no LLM, no event bus, just enough surface for the transcript helpers
# ---------------------------------------------------------------------------


class _FakeClient:
    def __init__(self, messages=None):
        self.messages = list(messages) if messages else []

    def append_messages(self, value):
        self.messages += value
        return self


class _FakeMsgDispatcher:
    def __init__(self, client):
        self._client = client
        self.published: list = []

    async def append_msgs(self, msgs, origin=None, *, skip_event_bus=False, kind=None):
        self._client.append_messages(msgs)

    async def publish_to_event_bus(self, msgs, origin=None, kind=None):
        self.published.append(msgs)


class _FakeLogger:
    log_steps = False

    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def _make_metadata(**overrides) -> ToolCallMetadata:
    defaults = dict(
        name="mytool",
        call_id="c1",
        call_dict={"function": {"arguments": "{}"}},
        call_idx=0,
        chat_context=None,
        assistant_msg={"role": "assistant", "content": None, "tool_calls": []},
        is_interjectable=False,
        tool_schema={},
        llm_arguments={},
        raw_arguments_json="{}",
    )
    defaults.update(overrides)
    return ToolCallMetadata(**defaults)


# ---------------------------------------------------------------------------
# 1. is_mutable — the one enforceable choke point
# ---------------------------------------------------------------------------


def test_is_mutable_respects_watermark_by_identity():
    client = _FakeClient([{"i": 0}, {"i": 1}, {"i": 2}])
    client._sent_watermark = 2

    assert is_mutable(client, client.messages[0]) is False
    assert is_mutable(client, client.messages[1]) is False
    assert is_mutable(client, client.messages[2]) is True

    # A structurally-identical-but-distinct dict is never confused for the
    # sent one — identity, not equality.
    assert is_mutable(client, {"i": 0}) is True

    # Absent from the transcript entirely (e.g. not yet appended) — mutable.
    assert is_mutable(client, {"unrelated": True}) is True


# ---------------------------------------------------------------------------
# 2. generate_with_preprocess — watermark set point + dev-mode hash assertion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watermark_advances_monotonically_on_pre_copy_length():
    client = _FakeClient([{"role": "user", "content": "hi"}])

    async def fake_generate(**kwargs):
        return "ok"

    client.generate = fake_generate

    await generate_with_preprocess(client, None)
    assert client._sent_watermark == 1

    client.messages.append({"role": "assistant", "content": "reply"})
    client.messages.append({"role": "user", "content": "follow-up"})
    await generate_with_preprocess(client, None)
    assert client._sent_watermark == 3

    # Monotonic: explicitly forcing a lower watermark (as if some caller
    # tried to regress it) never sticks — max() with the pre-copy length wins.
    client._sent_watermark = 1
    client._sent_watermark_hash = (
        None  # bypass the dev-mode check for this contrived probe
    )
    await generate_with_preprocess(client, None)
    assert client._sent_watermark == 3


@pytest.mark.asyncio
async def test_watermark_advances_even_when_dispatch_is_cancelled():
    client = _FakeClient([{"role": "user", "content": "hi"}])
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_generate(**kwargs):
        started.set()
        await release.wait()
        return "ok"

    client.generate = slow_generate

    task = asyncio.create_task(generate_with_preprocess(client, None))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The watermark was set synchronously before the await point, so a
    # cancelled/interrupted dispatch still advances it — the provider may
    # have cached the prefix of a stream that never finished.
    assert client._sent_watermark == 1


@pytest.mark.asyncio
async def test_dev_mode_assertion_fires_on_below_watermark_mutation():
    client = _FakeClient([{"role": "user", "content": "hi"}])

    async def fake_generate(**kwargs):
        return "ok"

    client.generate = fake_generate

    await generate_with_preprocess(client, None)
    assert client._sent_watermark == 1

    # Simulate exactly the bug this ticket fixes: an already-dispatched
    # message mutated in place.
    client.messages[0]["content"] = "mutated after being sent"
    client.messages.append({"role": "assistant", "content": "reply"})

    with pytest.raises(
        AssertionError,
        match="Append-only transcript invariant violated",
    ):
        await generate_with_preprocess(client, None)


# ---------------------------------------------------------------------------
# 3. ensure_placeholders_for_pending — self-describing, always-bypass stub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_stub_is_self_describing_and_stays_bare_pending():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "mytool", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())

    task = asyncio.ensure_future(asyncio.sleep(0))
    info = _make_metadata(assistant_msg=asst_msg)
    tools_data.save_task(task, info)

    created = await ensure_placeholders_for_pending(
        tools_data=tools_data,
        assistant_meta={},
        client=client,
        msg_dispatcher=dispatcher,
    )
    assert created == ["c1"]

    stub = info.tool_reply_msg
    assert stub is not None
    parsed = json.loads(stub["content"])
    assert parsed["_placeholder"] == "pending"
    assert "check_status" in parsed["meta:status"]
    # The existing shared test helper (used by ~15 tests via
    # _wait_for_tool_result) must still classify this as a bare-pending
    # placeholder despite the added self-describing text.
    assert _is_bare_pending_tool_placeholder(stub) is True

    # First-ever reply to a call_id is always spliced directly after its
    # assistant message — bypasses the watermark gate on principle.
    assert client.messages[1] is stub

    await task


# ---------------------------------------------------------------------------
# 4. insert_tool_message_after_assistant — the watermark gate + escape hatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insert_below_watermark_routes_to_check_status_pair():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "mytool", "arguments": "{}"},
            },
        ],
    }
    stub = create_tool_call_message(name="mytool", call_id="c1", content="pending")
    client = _FakeClient([asst_msg, stub])
    client._sent_watermark = 2  # both messages already dispatched
    dispatcher = _FakeMsgDispatcher(client)
    assistant_meta = {}

    reply = create_tool_call_message(
        name="mytool",
        call_id="c1",
        content="the real result",
    )
    await insert_tool_message_after_assistant(
        assistant_meta,
        asst_msg,
        reply,
        client,
        dispatcher,
    )

    # Below-watermark bytes are untouched — no splice happened.
    assert client.messages[0] is asst_msg
    assert client.messages[1] is stub
    assert stub["content"] == "pending"
    assert len(client.messages) == 4  # + synthetic assistant/tool check_status pair

    check_stub, check_reply = client.messages[2], client.messages[3]
    assert check_stub["role"] == "assistant"
    assert check_stub["tool_calls"][0]["function"]["name"] == "check_status_c1"
    assert check_reply["role"] == "tool"
    assert check_reply["tool_call_id"] == "c1_completed"
    assert check_reply["content"] == "the real result"


@pytest.mark.asyncio
async def test_insert_below_watermark_with_bypass_splices_adjacently():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "wait", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    client._sent_watermark = 1  # asst_msg already dispatched
    dispatcher = _FakeMsgDispatcher(client)
    assistant_meta = {}

    ack = create_tool_call_message(name="wait", call_id="c1", content="Acknowledged.")
    await insert_tool_message_after_assistant(
        assistant_meta,
        asst_msg,
        ack,
        client,
        dispatcher,
        bypass_watermark=True,
    )

    # Escape hatch: spliced directly after asst_msg despite being below-mark —
    # legality (an otherwise permanently-unanswered tool_calls entry) beats cache.
    assert client.messages == [asst_msg, ack]


@pytest.mark.asyncio
async def test_insert_above_watermark_splices_normally():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "mytool", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    client._sent_watermark = 0  # nothing dispatched yet
    dispatcher = _FakeMsgDispatcher(client)
    assistant_meta = {}

    reply = create_tool_call_message(name="mytool", call_id="c1", content="result")
    await insert_tool_message_after_assistant(
        assistant_meta,
        asst_msg,
        reply,
        client,
        dispatcher,
    )

    assert client.messages == [asst_msg, reply]


# ---------------------------------------------------------------------------
# 5. prune_wait_tool_call — below-mark leaves tool_calls untouched, acks instead
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prune_wait_below_mark_leaves_tool_calls_untouched_and_acks():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "cwait",
                "type": "function",
                "function": {"name": "wait", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    client._sent_watermark = 1  # asst_msg already dispatched
    dispatcher = _FakeMsgDispatcher(client)
    assistant_meta = {}

    await prune_wait_tool_call(
        asst_msg,
        "cwait",
        client=client,
        assistant_meta=assistant_meta,
        msg_dispatcher=dispatcher,
    )

    # The stale wait's tool_calls entry is untouched (no pop, no in-place edit) —
    # mutating it would have shifted every already-dispatched message after it.
    assert asst_msg["tool_calls"] == [
        {
            "id": "cwait",
            "type": "function",
            "function": {"name": "wait", "arguments": "{}"},
        },
    ]
    # Instead it's acknowledged via an appended reply for the same call_id.
    assert len(client.messages) == 2
    ack = client.messages[1]
    assert ack["role"] == "tool"
    assert ack["tool_call_id"] == "cwait"


@pytest.mark.asyncio
async def test_prune_wait_above_mark_still_removes_in_place():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "cwait",
                "type": "function",
                "function": {"name": "wait", "arguments": "{}"},
            },
            {
                "id": "cother",
                "type": "function",
                "function": {"name": "other", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    client._sent_watermark = 0  # nothing dispatched yet — free to mutate

    await prune_wait_tool_call(asst_msg, "cwait", client=client)

    assert [c["id"] for c in asst_msg["tool_calls"]] == ["cother"]
    assert client.messages == [asst_msg]


# ---------------------------------------------------------------------------
# 6. record_progress — coalesce-then-freeze, separate from tool_reply_msg
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_progress_coalesces_above_mark_then_freezes():
    client = _FakeClient()
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())
    frozen_stub = {"role": "tool", "tool_call_id": "c1", "content": "stub"}
    info = _make_metadata(tool_reply_msg=frozen_stub)

    await tools_data.record_progress(info, "c1", "first update", dispatcher)
    assert len(client.messages) == 1
    msg1 = client.messages[0]
    assert msg1["role"] == "user"
    assert msg1["content"] == "[progress c1] first update"
    assert info.progress_msg is msg1

    # Still mutable (nothing dispatched) — coalesces into the same message.
    await tools_data.record_progress(info, "c1", "second update", dispatcher)
    assert len(client.messages) == 1
    assert client.messages[0] is msg1
    assert msg1["content"] == "[progress c1] second update"

    # The tool_reply_msg stub is never touched by progress delivery.
    assert frozen_stub["content"] == "stub"

    # Freeze it (simulate a dispatch), then a new notification starts a fresh
    # tail message rather than reaching back to mutate the sent one.
    client._sent_watermark = len(client.messages)
    await tools_data.record_progress(info, "c1", "third update", dispatcher)
    assert len(client.messages) == 2
    assert msg1["content"] == "[progress c1] second update"  # frozen, unchanged
    msg2 = client.messages[1]
    assert msg2 is not msg1
    assert msg2["content"] == "[progress c1] third update"
    assert info.progress_msg is msg2


# ---------------------------------------------------------------------------
# 7. record_clarification — separate tail message, never touches tool_reply_msg
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_clarification_coalesces_and_never_touches_stub():
    client = _FakeClient()
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())
    frozen_stub = {"role": "tool", "tool_call_id": "c1", "content": "stub"}
    info = _make_metadata(tool_reply_msg=frozen_stub)

    await tools_data.record_clarification(info, "c1", "What color?", dispatcher)
    assert len(client.messages) == 1
    q_msg = client.messages[0]
    assert q_msg["role"] == "user"
    assert q_msg["content"].startswith("[clarification c1]")
    assert "What color?" in q_msg["content"]
    assert info.clarify_msg is q_msg
    assert frozen_stub["content"] == "stub"

    # Coalesces while mutable.
    await tools_data.record_clarification(info, "c1", "What color exactly?", dispatcher)
    assert len(client.messages) == 1
    assert "What color exactly?" in q_msg["content"]
    assert frozen_stub["content"] == "stub"


# ---------------------------------------------------------------------------
# 8. emit_completion_pair — shape of the sole below-watermark result path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_completion_pair_shape():
    client = _FakeClient()
    dispatcher = _FakeMsgDispatcher(client)

    tool_msg = await emit_completion_pair("the result", "c1", dispatcher)

    assert len(client.messages) == 2
    stub, reply = client.messages
    assert stub["role"] == "assistant"
    assert stub["tool_calls"][0]["id"] == "c1_completed"
    assert stub["tool_calls"][0]["function"]["name"] == "check_status_c1"
    assert reply is tool_msg
    assert reply["tool_call_id"] == "c1_completed"
    assert reply["content"] == "the result"


# ---------------------------------------------------------------------------
# 9. multi-handle: FINAL per-child result vs shared placeholder freeze
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_handle_child_result_mutable_then_frozen():
    placeholder = {"role": "tool", "tool_call_id": "parent", "content": "initial"}
    client = _FakeClient([placeholder])
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())

    state = _MultiHandleState(
        parent_call_id="parent",
        parent_name="mytool",
        placeholder_msg=placeholder,
        template={"a": "[h0: steerable]", "b": "[h1: steerable]"},
        results={"h0": None, "h1": None},
    )

    # Still mutable: h0 completing rebuilds the shared placeholder in place.
    client._sent_watermark = 0
    state.results["h0"] = "h0 result"
    await state.record_child_result(
        "parenth0",
        "h0 result",
        tools_data=tools_data,
        msg_dispatcher=dispatcher,
    )
    assert len(client.messages) == 1
    assert "h0 result" in placeholder["content"]
    assert len(dispatcher.published) == 1

    # Now freeze the shared placeholder (simulate a dispatch) before h1 finishes.
    client._sent_watermark = len(client.messages)
    state.results["h1"] = "h1 result"
    await state.record_child_result(
        "parenth1",
        "h1 result",
        tools_data=tools_data,
        msg_dispatcher=dispatcher,
    )

    # The frozen shared placeholder is never rewritten again...
    assert "h1 result" not in placeholder["content"]
    # ...instead h1's own terminal result is delivered on its own call_id via
    # a per-child check_status pair.
    assert len(client.messages) == 3
    check_stub, check_reply = client.messages[1], client.messages[2]
    assert check_stub["tool_calls"][0]["function"]["name"] == "check_status_parenth1"
    assert check_reply["content"] == "h1 result"


# ---------------------------------------------------------------------------
# 10. process_completed_task — the placeholder-result-write site end to end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_completed_task_freezes_stub_below_mark_uses_check_status():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "mytool", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())
    assistant_meta = {}

    async def mytool():
        return "the final result"

    task = asyncio.create_task(mytool())
    info = _make_metadata(assistant_msg=asst_msg)
    tools_data.save_task(task, info)

    await ensure_placeholders_for_pending(
        tools_data=tools_data,
        assistant_meta=assistant_meta,
        client=client,
        msg_dispatcher=dispatcher,
    )
    stub = info.tool_reply_msg
    stub_content_at_dispatch = stub["content"]

    # Simulate the placeholder having been sent in a request before the tool
    # actually finishes — the realistic interrupt-mode shape this ticket fixes.
    client._sent_watermark = len(client.messages)

    await task
    tracker = _LoopToolFailureTracker(3, ToolLoopRuntimeState())
    needs_turn = await tools_data.process_completed_task(
        task=task,
        consecutive_failures=tracker,
        outer_handle_container=[None],
        assistant_meta=assistant_meta,
        msg_dispatcher=dispatcher,
    )

    assert needs_turn is True
    # The stub is never rewritten once below-mark — it stays exactly what it
    # was when it was self-describingly created.
    assert stub["content"] == stub_content_at_dispatch
    assert (
        "check_status" in stub_content_at_dispatch
        or "pending" in stub_content_at_dispatch
    )

    check_stub, check_reply = client.messages[-2], client.messages[-1]
    assert check_stub["tool_calls"][0]["function"]["name"] == "check_status_c1"
    assert "the final result" in check_reply["content"]


@pytest.mark.asyncio
async def test_process_completed_task_writes_in_place_above_mark():
    asst_msg = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "c1",
                "type": "function",
                "function": {"name": "mytool", "arguments": "{}"},
            },
        ],
    }
    client = _FakeClient([asst_msg])
    dispatcher = _FakeMsgDispatcher(client)
    tools_data = ToolsData(tools={}, client=client, logger=_FakeLogger())
    assistant_meta = {}

    async def mytool():
        return "quick result"

    task = asyncio.create_task(mytool())
    info = _make_metadata(assistant_msg=asst_msg)
    tools_data.save_task(task, info)

    await ensure_placeholders_for_pending(
        tools_data=tools_data,
        assistant_meta=assistant_meta,
        client=client,
        msg_dispatcher=dispatcher,
    )
    stub = info.tool_reply_msg

    # Nothing dispatched yet — free to edit in place.
    await task
    tracker = _LoopToolFailureTracker(3, ToolLoopRuntimeState())
    await tools_data.process_completed_task(
        task=task,
        consecutive_failures=tracker,
        outer_handle_container=[None],
        assistant_meta=assistant_meta,
        msg_dispatcher=dispatcher,
    )

    assert len(client.messages) == 2  # asst_msg + the one stub, edited in place
    assert client.messages[1] is stub
    assert "quick result" in stub["content"]
