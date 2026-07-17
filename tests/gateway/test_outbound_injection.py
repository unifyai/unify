"""End-to-end wire-in test for ``comms_utils`` + ``OutboundTransport``.

Phase A.bis.7.4 lands ``set_outbound_transport`` /
``get_outbound_transport`` + the shared
``_publish_to_assistant_topic`` helper in
``unify.conversation_manager.domains.comms_utils``, and refactors the
three publish helpers to route through it. This test exercises both
arms:

1. With an injected ``InMemoryOutboundTransport``, calling any of the
   three publish helpers routes through the transport and the
   captured payload matches the legacy on-wire shape exactly.
2. Without an injected transport (the default), the legacy inline
   ``pubsub_v1.PublisherClient`` path is still invoked. Mocked at the
   ``_get_publisher`` boundary to avoid requiring a real Pub/Sub
   project.

Together these guarantee the wire-in is purely additive: legacy
callers keep their behaviour, new callers opt in by calling
``set_outbound_transport``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from unify.conversation_manager.domains import comms_utils
from unify.gateway.outbound_inmemory import InMemoryOutboundTransport

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _stub_settings(
    monkeypatch: pytest.MonkeyPatch,
    *,
    env_suffix: str,
    gcp_project_id: str = "responsive-city-458413-a2",
) -> None:
    """Replace ``comms_utils.SETTINGS`` with a writable stand-in.

    The real SETTINGS object exposes ``ENV_SUFFIX`` as a read-only
    property, so direct monkeypatch.setattr fails on teardown. A
    SimpleNamespace clone with the same field surface lets us set
    arbitrary values without fighting Pydantic.
    """
    stub = SimpleNamespace(
        ENV_SUFFIX=env_suffix,
        GCP_PROJECT_ID=gcp_project_id,
        ORCHESTRA_ADMIN_KEY=comms_utils.SETTINGS.ORCHESTRA_ADMIN_KEY,
        ORCHESTRA_URL=comms_utils.SETTINGS.ORCHESTRA_URL,
        conversation=comms_utils.SETTINGS.conversation,
    )
    monkeypatch.setattr(comms_utils, "SETTINGS", stub)


@pytest.fixture(autouse=True)
def _clean_outbound_transport():
    """Each test starts with no transport configured."""
    comms_utils.set_outbound_transport(None)
    yield
    comms_utils.set_outbound_transport(None)


@pytest.fixture
def _agent_id_42(monkeypatch: pytest.MonkeyPatch) -> int:
    """Pin SESSION_DETAILS.assistant.agent_id + staging settings."""
    monkeypatch.setattr(
        comms_utils.SESSION_DETAILS.assistant,
        "agent_id",
        42,
    )
    _stub_settings(monkeypatch, env_suffix="-staging")
    return 42


# ---------------------------------------------------------------------------
# set/get accessor
# ---------------------------------------------------------------------------


def test_default_transport_is_none() -> None:
    assert comms_utils.get_outbound_transport() is None


def test_set_outbound_transport_persists_value() -> None:
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)
    assert comms_utils.get_outbound_transport() is transport


def test_set_outbound_transport_to_none_clears() -> None:
    comms_utils.set_outbound_transport(InMemoryOutboundTransport())
    comms_utils.set_outbound_transport(None)
    assert comms_utils.get_outbound_transport() is None


# ---------------------------------------------------------------------------
# Injected-transport arm: publishes route through transport
# ---------------------------------------------------------------------------


def test_helper_routes_through_injected_transport(
    _agent_id_42: int,
) -> None:
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    msg_id = comms_utils._publish_to_assistant_topic(
        agent_id=_agent_id_42,
        thread="msg",
        event={"body": "hi"},
    )
    assert msg_id == "inmemory-0"
    assert len(transport.published) == 1
    envelope = transport.published[0]
    assert envelope.topic == "unity-42-staging"
    assert envelope.thread == "msg"
    assert json.loads(envelope.message) == {
        "thread": "msg",
        "event": {"body": "hi"},
    }


def test_helper_forwards_timeout_to_transport(
    _agent_id_42: int,
) -> None:
    transport = MagicMock(spec=InMemoryOutboundTransport)
    transport.publish.return_value = "broker-id"
    comms_utils.set_outbound_transport(transport)

    comms_utils._publish_to_assistant_topic(
        agent_id=_agent_id_42,
        thread="system_error",
        event={"content": "boom"},
        timeout=5,
    )
    transport.publish.assert_called_once()
    args, kwargs = transport.publish.call_args
    assert args[0] == "unity-42-staging"
    assert json.loads(args[1]) == {
        "thread": "system_error",
        "event": {"content": "boom"},
    }
    assert kwargs == {"thread": "system_error", "timeout": 5}


def test_helper_propagates_transport_exceptions(
    _agent_id_42: int,
) -> None:
    transport = MagicMock(spec=InMemoryOutboundTransport)
    transport.publish.side_effect = RuntimeError("simulated transport failure")
    comms_utils.set_outbound_transport(transport)

    with pytest.raises(RuntimeError, match="simulated transport failure"):
        comms_utils._publish_to_assistant_topic(
            agent_id=_agent_id_42,
            thread="msg",
            event={},
        )


# ---------------------------------------------------------------------------
# Legacy-path arm: publishes still go through _get_publisher
# ---------------------------------------------------------------------------


def test_helper_falls_back_to_legacy_publisher_when_no_transport(
    _agent_id_42: int,
) -> None:
    fake_publisher = MagicMock(name="LegacyPublisher")
    fake_publisher.topic_path.return_value = (
        "projects/responsive-city-458413-a2/topics/unity-42-staging"
    )
    fake_future = MagicMock()
    fake_future.result.return_value = "legacy-broker-id"
    fake_publisher.publish.return_value = fake_future

    with patch.object(comms_utils, "_get_publisher", return_value=fake_publisher):
        msg_id = comms_utils._publish_to_assistant_topic(
            agent_id=_agent_id_42,
            thread="msg",
            event={"body": "hi"},
        )

    assert msg_id == "legacy-broker-id"
    fake_publisher.topic_path.assert_called_once_with(
        "responsive-city-458413-a2",
        "unity-42-staging",
    )
    fake_publisher.publish.assert_called_once_with(
        "projects/responsive-city-458413-a2/topics/unity-42-staging",
        b'{"thread": "msg", "event": {"body": "hi"}}',
        thread="msg",
    )
    fake_future.result.assert_called_once_with()


def test_helper_forwards_timeout_to_legacy_publisher(
    _agent_id_42: int,
) -> None:
    fake_publisher = MagicMock(name="LegacyPublisher")
    fake_publisher.topic_path.return_value = "topic/path"
    fake_future = MagicMock()
    fake_future.result.return_value = "legacy-id"
    fake_publisher.publish.return_value = fake_future

    with patch.object(comms_utils, "_get_publisher", return_value=fake_publisher):
        comms_utils._publish_to_assistant_topic(
            agent_id=_agent_id_42,
            thread="system_error",
            event={"content": "boom"},
            timeout=5,
        )

    fake_future.result.assert_called_once_with(timeout=5)


# ---------------------------------------------------------------------------
# Topic naming convention (the format every consumer relies on)
# ---------------------------------------------------------------------------


def test_topic_naming_includes_env_suffix_for_staging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Convention: unity-{agent_id}{env_suffix}. Hosted staging uses -staging."""
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    _stub_settings(monkeypatch, env_suffix="-staging")
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    comms_utils._publish_to_assistant_topic(
        agent_id=42,
        thread="msg",
        event={},
    )
    assert transport.published[0].topic == "unity-42-staging"


def test_topic_naming_omits_env_suffix_for_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hosted production: env_suffix is empty -> unity-{agent_id}."""
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    _stub_settings(monkeypatch, env_suffix="")
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    comms_utils._publish_to_assistant_topic(
        agent_id=42,
        thread="msg",
        event={},
    )
    assert transport.published[0].topic == "unity-42"


# ---------------------------------------------------------------------------
# Public helper integration (the three call sites)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_unify_message_routes_through_injected_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    _stub_settings(monkeypatch, env_suffix="-staging")
    monkeypatch.setattr(comms_utils, "_use_local_comms", lambda: False)
    # No unified chat store in this sandbox: the send falls back to the
    # direct assistant-topic publish, which must ride the injected transport.
    monkeypatch.setattr(
        comms_utils,
        "_post_chat_message_to_orchestra",
        lambda payload: {"success": False, "error": "orchestra config missing"},
    )
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    result = await comms_utils.send_unify_message("hello", contact_id=7)
    assert result == {"success": True}
    assert len(transport.published) == 1
    env = transport.published[0]
    assert env.topic == "unity-42-staging"
    assert env.thread == "unify_message_outbound"
    payload = json.loads(env.message)
    assert payload == {
        "thread": "unify_message_outbound",
        "event": {"content": "hello", "role": "assistant", "contact_id": 7},
    }


def test_publish_system_error_routes_through_injected_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    _stub_settings(monkeypatch, env_suffix="-staging")
    monkeypatch.setattr(comms_utils, "_use_local_comms", lambda: False)
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    comms_utils.publish_system_error("boom", error_type="oom")

    assert len(transport.published) == 1
    env = transport.published[0]
    assert env.thread == "system_error"
    assert json.loads(env.message) == {
        "thread": "system_error",
        "event": {"content": "boom", "error_type": "oom"},
    }


@pytest.mark.asyncio
async def test_publish_assistant_desktop_ready_routes_through_injected_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(comms_utils.SESSION_DETAILS.assistant, "agent_id", 42)
    _stub_settings(monkeypatch, env_suffix="-staging")
    monkeypatch.setattr(comms_utils, "_use_local_comms", lambda: False)
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    await comms_utils.publish_assistant_desktop_ready(
        binding_id="b1",
        desktop_url="https://desktop.example.com",
        liveview_url="https://liveview.example.com",
        vm_type="ubuntu",
    )

    assert len(transport.published) == 1
    env = transport.published[0]
    assert env.thread == "assistant_desktop_ready"
    assert json.loads(env.message) == {
        "thread": "assistant_desktop_ready",
        "event": {
            "binding_id": "b1",
            "desktop_url": "https://desktop.example.com",
            "liveview_url": "https://liveview.example.com",
            "vm_type": "ubuntu",
        },
    }


def test_publish_system_error_no_op_when_agent_id_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-assignment idle pod: no agent_id -> publish_system_error returns silently."""
    monkeypatch.setattr(
        comms_utils.SESSION_DETAILS.assistant,
        "agent_id",
        None,
    )
    transport = InMemoryOutboundTransport()
    comms_utils.set_outbound_transport(transport)

    comms_utils.publish_system_error("boom", error_type="oom")
    assert transport.published_count == 0
