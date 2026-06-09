from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from unity.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)


@pytest.mark.asyncio
async def test_start_call_uses_provided_room_name(monkeypatch):
    manager = LivekitCallManager(
        CallConfig(
            assistant_id="123",
            user_id="user-123",
            assistant_bio="",
            assistant_number="+15550000000",
            voice_provider="test",
            voice_id="voice",
        ),
    )
    monkeypatch.setattr(manager, "_ensure_socket_server", AsyncMock())
    start_subprocess = AsyncMock()
    monkeypatch.setattr(manager, "_start_call_subprocess", start_subprocess)

    contact = {"contact_id": 2, "whatsapp_number": "+15550000001"}
    boss = {"contact_id": 1}
    await manager.start_call(
        contact,
        boss,
        channel="whatsapp_call",
        room_name="unity_wa_room_123_CA111",
    )

    assert manager.room_name == "unity_wa_room_123_CA111"
    start_subprocess.assert_awaited_once()
    assert start_subprocess.await_args.args[0] == "unity_wa_room_123_CA111"
