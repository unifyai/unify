"""Symbolic tests for assistant-initiated hang-up plumbing.

Covers:
- ``comms_utils.end_phone_conference`` posts the correct URL/payload/auth.
- ``LivekitCallManager.end_call`` drops the Twilio conference (best-effort) for
  telephony channels and signals the voice agent to stop via ``app:call:status``.
- Unify Meet end signals stop without a conference hangup (no carrier leg).

No LLM or LiveKit calls are involved.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from unity.conversation_manager.domains import comms_utils
from unity.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)

_ASSISTANT_ID = "42"


def _build_call_manager(event_broker) -> LivekitCallManager:
    cfg = CallConfig(
        assistant_id=_ASSISTANT_ID,
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="voice-1",
        assistant_name="Assistant",
        job_name="job-1",
    )
    return LivekitCallManager(cfg, event_broker=event_broker)


@pytest.mark.asyncio
async def test_end_phone_conference_posts_expected_request(monkeypatch):
    """end_phone_conference POSTs ConferenceName to /phone/end-conference."""
    posts: list = []

    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.json = AsyncMock(return_value={"success": True, "status": "completed"})

    fake_post_ctx = MagicMock()
    fake_post_ctx.__aenter__ = AsyncMock(return_value=fake_resp)
    fake_post_ctx.__aexit__ = AsyncMock(return_value=False)

    def _post(url, *, headers=None, json=None):
        posts.append((url, headers, json))
        return fake_post_ctx

    fake_session = MagicMock()
    fake_session.post = _post
    fake_session.__aenter__ = AsyncMock(return_value=fake_session)
    fake_session.__aexit__ = AsyncMock(return_value=False)

    monkeypatch.setattr(
        comms_utils.aiohttp,
        "ClientSession",
        lambda *a, **k: fake_session,
    )
    monkeypatch.setattr(
        comms_utils,
        "_gateway_comms_base_url",
        lambda: "http://comms.test",
    )

    result = await comms_utils.end_phone_conference("Unity_ABC")

    assert result == {"success": True, "status": "completed"}
    assert len(posts) == 1
    url, headers, body = posts[0]
    assert url == "http://comms.test/phone/end-conference"
    assert body == {"ConferenceName": "Unity_ABC"}
    assert "Authorization" in headers


@pytest.mark.asyncio
async def test_end_phone_conference_noop_without_name():
    """No HTTP call is attempted when there is no conference name."""
    result = await comms_utils.end_phone_conference("")
    assert result["success"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["phone_call", "whatsapp_call"])
async def test_end_call_phone_ends_conference_and_signals_stop(monkeypatch, channel):
    """Telephony hang-up drops the Twilio conference and signals agent stop."""
    broker = MagicMock()
    broker.publish = AsyncMock()
    cm = _build_call_manager(broker)
    cm._call_channel = channel
    cm.conference_name = "Unity_CONF"

    ended: list = []

    async def _fake_end_conf(name):
        ended.append(name)
        return {"success": True}

    monkeypatch.setattr(comms_utils, "end_phone_conference", _fake_end_conf)

    await cm.end_call()

    assert ended == ["Unity_CONF"]
    broker.publish.assert_awaited_once()
    channel_arg, payload = broker.publish.await_args.args
    assert channel_arg == "app:call:status"
    assert json.loads(payload)["type"] == "stop"


@pytest.mark.asyncio
async def test_end_call_unify_meet_signals_stop_without_conference(monkeypatch):
    """Unify Meet has no carrier leg: stop is signalled, no conference hangup."""
    broker = MagicMock()
    broker.publish = AsyncMock()
    cm = _build_call_manager(broker)
    cm._call_channel = "unify_meet"
    cm.conference_name = ""

    called: list = []
    monkeypatch.setattr(
        comms_utils,
        "end_phone_conference",
        lambda name: called.append(name),
    )

    await cm.end_call()

    assert called == []
    broker.publish.assert_awaited_once()
    channel_arg, payload = broker.publish.await_args.args
    assert channel_arg == "app:call:status"
    assert json.loads(payload)["type"] == "stop"


@pytest.mark.asyncio
async def test_end_call_phone_skips_conference_when_name_missing(monkeypatch):
    """Outbound calls without a tracked conference still signal stop."""
    broker = MagicMock()
    broker.publish = AsyncMock()
    cm = _build_call_manager(broker)
    cm._call_channel = "phone_call"
    cm.conference_name = ""

    called: list = []
    monkeypatch.setattr(
        comms_utils,
        "end_phone_conference",
        lambda name: called.append(name),
    )

    await cm.end_call()

    assert called == []
    broker.publish.assert_awaited_once()
