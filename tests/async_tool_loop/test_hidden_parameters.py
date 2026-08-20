# tests/async_tool_loop/test_hidden_parameters.py
from __future__ import annotations

import asyncio
import pytest

import unify.common.llm_helpers as llmh


# --------------------------------------------------------------------------- #
# Shared assertion helper                                                     #
# --------------------------------------------------------------------------- #
def _assert_internal_queues_hidden(fn) -> None:
    """
    Verify that *_clarification_up_q*, *_clarification_down_q*, and *_notification_up_q* never show up
    in the generated schema nor in the docstring description that is forwarded
    to the LLM.
    """
    schema = llmh.method_to_schema(fn)
    props = schema["function"]["parameters"]["properties"]
    required = schema["function"]["parameters"]["required"]
    desc = schema["function"]["description"]

    for name in ("_clarification_up_q", "_clarification_down_q", "_notification_up_q"):
        # ––– must be absent from JSON schema –––
        assert name not in props
        assert name not in required
        # ––– and also stripped from the docstring forwarded to the model –––
        assert name not in desc


# --------------------------------------------------------------------------- #
# 1. Original “Google style” example (colon after each name)                  #
# --------------------------------------------------------------------------- #
def tool_google_style(
    a: int,
    _clarification_up_q: asyncio.Queue[str],
    _clarification_down_q: asyncio.Queue[str],
    _notification_up_q: asyncio.Queue | None = None,
) -> int:
    """
    Google-style.

    Args:
        a: Some value.
        _clarification_up_q: internal.
        _clarification_down_q: internal.
        _notification_up_q: internal.
    """
    return a


# --------------------------------------------------------------------------- #
# 2. New *slash-separated* NumPy variant: “name_a / name_b : type”            #
# --------------------------------------------------------------------------- #
def tool_numpy_slash(
    a: int,
    _clarification_up_q: asyncio.Queue[str] | None = None,
    _clarification_down_q: asyncio.Queue[str] | None = None,
    _notification_up_q: asyncio.Queue | None = None,
) -> int:
    """
    NumPy style — slash-separated synonyms.

    Parameters
    ----------
    a : int
        Some value.
    _clarification_up_q / _clarification_down_q / _notification_up_q : asyncio.Queue | None
        Internal queues.
    """
    return a


# --------------------------------------------------------------------------- #
# 3. New *comma-separated* variant: “name_a, name_b, …” (no type, no colon)   #
# --------------------------------------------------------------------------- #
def tool_numpy_commas(
    a: int,
    _clarification_up_q: asyncio.Queue[str] | None = None,
    _clarification_down_q: asyncio.Queue[str] | None = None,
    _notification_up_q: asyncio.Queue | None = None,
) -> int:
    """
    NumPy style — comma-separated, no type / no colon.

    Parameters
    ----------
    a : int
        Some value.

    _clarification_up_q, _clarification_down_q, _notification_up_q
        Internal queues.
    """
    return a


# --------------------------------------------------------------------------- #
# Parametrised test that covers all three doc-string flavours                 #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "fn",
    [tool_google_style, tool_numpy_slash, tool_numpy_commas],
    ids=["google", "numpy-slash", "numpy-commas"],
)
def test_hidden_parameters_are_removed_from_schema_and_docs(fn) -> None:
    """
    Any internal queues must be invisible to the API schema and the LLM
    description, regardless of how they are documented in the original
    docstring (Google, NumPy-slash, or NumPy-commas).
    """
    _assert_internal_queues_hidden(fn)


# --------------------------------------------------------------------------- #
# 4. steer()'s schema is fixed regardless of a handle's interject signature   #
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_dynamic_factory_does_not_adopt_custom_interject_args() -> None:
    """
    Pre-steer(), DynamicToolFactory._create_interject_tool adopted a
    per-call-id minted tool's signature from the live handle's own
    `interject` override, including any custom parameters beyond `message`
    (e.g. `priority`, `category`) — that method (and the "hardcode nothing,
    adopt everything" design it was written to prove) no longer exists.
    steer()'s `interject` action always takes a single string `payload`
    (see STEER_DOC / dynamic_tools_factory.py), regardless of what a live
    handle's own `interject()` signature looks like — a handle can no
    longer widen what the model can pass for a core steering action (only
    `action="call"` on a genuinely custom method still adopts a real
    signature). This asserts that boundary directly: steer's own generated
    schema carries none of CustomInterjectHandle's extra parameters.
    """
    from contextlib import suppress

    from unify.common.async_tool_loop import SteerableToolHandle
    from unify.common._async_tool.tools_data import ToolsData
    from unify.common._async_tool.tools_utils import ToolCallMetadata
    from unify.common._async_tool.dynamic_tools_factory import DynamicToolFactory

    class CustomInterjectHandle(SteerableToolHandle):
        """Handle with custom interject parameters."""

        def __init__(self) -> None:
            pass

        async def interject(
            self,
            message: str,
            *,
            priority: int = 1,
            category: str = "general",
            _parent_chat_context_cont: list[dict] | None = None,
        ) -> str:
            """Custom interject with priority and category.

            Parameters
            ----------
            message : str
                The interjection message.
            priority : int
                Priority level (1-5).
            category : str
                Category for routing.
            """
            return "ok"

        async def ask(self, q: str, **kw):
            return self

        def stop(self, r=None, **kw):
            pass

        async def pause(self):
            return "paused"

        async def resume(self):
            return "resumed"

        def done(self):
            return True

        async def result(self):
            return "ok"

        async def next_clarification(self):
            return {}

        async def next_notification(self):
            return {}

        async def answer_clarification(self, cid, ans):
            pass

    # Setup minimal environment
    class _DummyLogger:
        log_steps = False

        def info(self, *a, **kw): ...
        def error(self, *a, **kw): ...

    class _DummyClient:
        def __init__(self):
            self.messages = []

    tools_data = ToolsData({}, client=_DummyClient(), logger=_DummyLogger())
    pending_task = asyncio.create_task(asyncio.sleep(10))

    meta = ToolCallMetadata(
        name="custom_tool",
        call_id="call_456",
        call_dict={"id": "call_456", "function": {"name": "custom", "arguments": "{}"}},
        call_idx=0,
        chat_context=None,
        assistant_msg={},
        is_interjectable=True,
        tool_schema={},
        llm_arguments={},
        raw_arguments_json="{}",
        handle=CustomInterjectHandle(),
        interject_queue=None,
        clar_up_queue=None,
        clar_down_queue=None,
        notification_queue=None,
        pause_event=None,
    )
    tools_data.save_task(pending_task, meta)

    # Generate tools — no per-call interject tool is minted anymore.
    factory = DynamicToolFactory(tools_data)
    factory.generate()

    assert set(factory.dynamic_tools.keys()) == {
        "wait",
        "steer",
        "ask_about_completed_tool",
    }

    steer_fn = factory.dynamic_tools["steer"]
    schema = llmh.method_to_schema(steer_fn, include_class_name=False)
    props = schema["function"]["parameters"]["properties"]

    # steer()'s fixed shape: call_id/action/payload/method/include_parent_context.
    # None of CustomInterjectHandle's extra interject kwargs leak in.
    assert set(props.keys()) == {
        "call_id",
        "action",
        "payload",
        "method",
        "include_parent_context",
    }
    assert "priority" not in props
    assert "category" not in props
    assert "_parent_chat_context_cont" not in props

    with suppress(BaseException):
        pending_task.cancel()
        await pending_task
