"""Contract tests for the ``OutboundTransport`` Protocol.

Phase A.bis.7.1 lands only the protocol itself; concrete
implementations arrive in A.bis.7.2 (in-memory) and A.bis.7.3
(Pub/Sub). The tests here exercise the contract against a tiny inline
implementation to pin the runtime-checkable behaviour and the publish
call shape.
"""

from __future__ import annotations

from typing import Any

import pytest

from unify.gateway.outbound import OutboundTransport


class _CapturingOutbound:
    """Minimal OutboundTransport implementation used to validate the seam.

    Records every publish call into an in-memory list for assertion by
    tests; ``aclose`` flips a flag. Lets tests assert on publish shape
    without depending on any external broker.
    """

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.closed = False

    def publish(
        self,
        topic: str,
        message: bytes,
        *,
        thread: str = "",
        timeout: float | None = None,
    ) -> str:
        if self.closed:
            raise RuntimeError("publish called after aclose")
        msg_id = f"msg-{len(self.published)}"
        self.published.append(
            {
                "topic": topic,
                "message": message,
                "thread": thread,
                "timeout": timeout,
                "id": msg_id,
            },
        )
        return msg_id

    async def aclose(self) -> None:
        self.closed = True


def test_capturing_transport_satisfies_outbound_transport_protocol() -> None:
    transport = _CapturingOutbound()
    assert isinstance(transport, OutboundTransport)


def test_publish_records_topic_and_message_and_thread() -> None:
    transport = _CapturingOutbound()
    msg_id = transport.publish(
        "unity-42-staging",
        b'{"thread":"msg","event":{}}',
        thread="msg",
    )
    assert msg_id == "msg-0"
    assert transport.published == [
        {
            "topic": "unity-42-staging",
            "message": b'{"thread":"msg","event":{}}',
            "thread": "msg",
            "timeout": None,
            "id": "msg-0",
        },
    ]


def test_publish_forwards_optional_timeout() -> None:
    transport = _CapturingOutbound()
    transport.publish("topic", b"data", thread="t", timeout=5.0)
    assert transport.published[0]["timeout"] == 5.0


def test_publish_assigns_distinct_ids_to_each_call() -> None:
    transport = _CapturingOutbound()
    ids = [transport.publish("t", b"a", thread="x") for _ in range(5)]
    assert ids == [f"msg-{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_aclose_flips_closed_flag_and_subsequent_publish_raises() -> None:
    transport = _CapturingOutbound()
    await transport.aclose()
    assert transport.closed
    with pytest.raises(RuntimeError, match="after aclose"):
        transport.publish("t", b"data")


@pytest.mark.asyncio
async def test_aclose_is_idempotent() -> None:
    transport = _CapturingOutbound()
    await transport.aclose()
    await transport.aclose()


def test_publish_shape_pins_three_call_sites_in_comms_utils() -> None:
    """Confirms the protocol shape covers all three current publish call sites.

    The publish() signature must match what
    ``send_unify_message``, ``publish_system_error``, and
    ``publish_assistant_desktop_ready`` need today: positional
    ``topic`` + ``message`` bytes, keyword-only ``thread`` string.
    """
    import inspect

    sig = inspect.signature(OutboundTransport.publish)
    params = sig.parameters
    assert list(params)[:3] == ["self", "topic", "message"]
    assert params["thread"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["timeout"].kind == inspect.Parameter.KEYWORD_ONLY
    # `from __future__ import annotations` keeps return as a string
    assert sig.return_annotation in (str, "str")


def test_module_exports_match_documented_surface() -> None:
    import unify.gateway.outbound as outbound_mod

    assert hasattr(outbound_mod, "OutboundTransport")
