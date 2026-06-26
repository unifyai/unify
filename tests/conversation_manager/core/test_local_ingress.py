from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
import pytest_asyncio

from unity.conversation_manager.in_memory_event_broker import (
    create_in_memory_event_broker,
    reset_in_memory_event_broker,
)
from unity.settings import SETTINGS


async def _get_message_on_channel(pubsub, expected_channel: str, timeout: float = 2.0):
    start = asyncio.get_running_loop().time()
    while asyncio.get_running_loop().time() - start < timeout:
        msg = await pubsub.get_message(
            timeout=0.2,
            ignore_subscribe_messages=True,
        )
        if msg is None:
            continue
        if msg["channel"] == expected_channel:
            return msg
    return None


@pytest_asyncio.fixture
async def broker():
    reset_in_memory_event_broker()
    event_broker = create_in_memory_event_broker()
    yield event_broker
    await event_broker.aclose()
    reset_in_memory_event_broker()


@pytest.fixture
def patched_local_session():
    with (
        patch(
            "unity.conversation_manager.comms_manager.SESSION_DETAILS",
        ) as comms_session,
        patch(
            "unity.conversation_manager.local_ingress.SESSION_DETAILS",
        ) as ingress_session,
    ):
        comms_session.assistant.agent_id = 42
        comms_session.assistant.desktop_mode = "ubuntu"
        comms_session.assistant.desktop_url = ""
        comms_session.user.first_name = "Boss"
        comms_session.user.surname = "User"
        comms_session.user.number = "+15555550000"
        comms_session.user.email = "boss@example.com"
        comms_session.user.whatsapp_number = "+15555550000"
        comms_session.user.id = "user-1"
        comms_session.boss_contact_id = 1

        ingress_session.assistant.agent_id = 42
        ingress_session.user.id = "user-1"
        yield


class TestLocalIngress:
    @pytest.mark.asyncio
    async def test_envelope_endpoint_publishes_sms_event(
        self,
        broker,
        patched_local_session,
    ):
        from unity.conversation_manager.comms_manager import CommsManager
        from unity.conversation_manager.local_ingress import LocalCommsIngress

        with (
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_HOST", "127.0.0.1"),
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_PORT", 0),
            patch(
                "unity.conversation_manager.local_ingress.local_email.is_email_configured",
                return_value=False,
            ),
        ):
            comms_manager = CommsManager(broker)
            comms_manager.loop = asyncio.get_running_loop()
            ingress = LocalCommsIngress(comms_manager)
            await ingress.start()

            try:
                port = ingress._site._server.sockets[0].getsockname()[1]
                async with broker.pubsub() as pubsub:
                    await pubsub.subscribe("app:comms:msg_message")
                    async with aiohttp.ClientSession() as session:
                        response = await session.post(
                            f"http://127.0.0.1:{port}/local/comms/envelope",
                            json={
                                "thread": "msg",
                                "event": {
                                    "body": "Hello from local ingress",
                                    "from_number": "+15551230001",
                                    "contacts": [
                                        {
                                            "contact_id": 7,
                                            "first_name": "Alice",
                                            "surname": "User",
                                            "phone_number": "+15551230001",
                                            "email_address": "alice@example.com",
                                            "whatsapp_number": "+15551230001",
                                        },
                                    ],
                                },
                            },
                        )
                        assert response.status == 200

                    message = await _get_message_on_channel(
                        pubsub,
                        "app:comms:msg_message",
                    )
                    assert message is not None
                    payload = json.loads(message["data"])
                    assert payload["event_name"] == "SMSReceived"
                    assert payload["payload"]["content"] == "Hello from local ingress"
                    assert payload["payload"]["contact"]["contact_id"] == 7
            finally:
                await ingress.stop()

    @pytest.mark.asyncio
    async def test_outbox_endpoint_stores_and_returns_payloads(
        self,
        broker,
        patched_local_session,
    ):
        from unity.conversation_manager.comms_manager import CommsManager
        from unity.conversation_manager.local_ingress import LocalCommsIngress

        with (
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_HOST", "127.0.0.1"),
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_PORT", 0),
            patch(
                "unity.conversation_manager.local_ingress.local_email.is_email_configured",
                return_value=False,
            ),
        ):
            ingress = LocalCommsIngress(CommsManager(broker))
            await ingress.start()

            try:
                port = ingress._site._server.sockets[0].getsockname()[1]
                async with aiohttp.ClientSession() as session:
                    response = await session.post(
                        f"http://127.0.0.1:{port}/local/comms/outbox",
                        json={
                            "thread": "system_error",
                            "event": {"content": "boom"},
                        },
                    )
                    assert response.status == 200

                    outbox = await session.get(
                        f"http://127.0.0.1:{port}/local/comms/outbox",
                    )
                    assert outbox.status == 200
                    body = await outbox.json()
                    assert len(body["items"]) == 1
                    assert body["items"][0]["thread"] == "system_error"
                    assert body["items"][0]["event"] == {"content": "boom"}
                    assert isinstance(body["items"][0]["publish_timestamp"], float)
            finally:
                await ingress.stop()

    @pytest.mark.asyncio
    async def test_twilio_call_uses_call_scoped_room_and_dispatch_rule(
        self,
        broker,
        patched_local_session,
    ):
        from unity.conversation_manager.comms_manager import CommsManager
        from unity.conversation_manager.local_ingress import LocalCommsIngress

        with (
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_HOST", "127.0.0.1"),
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_PORT", 0),
            patch(
                "unity.conversation_manager.local_ingress.local_email.is_email_configured",
                return_value=False,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_twilio.validate_signature",
                return_value=True,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.make_call_scoped_sip_uri",
                return_value=(
                    "sip:16892256176-CA123@example.sip.livekit.cloud",
                    "16892256176-CA123",
                ),
            ) as make_call_scoped_sip_uri,
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.ensure_call_scoped_dispatch_rule",
                new=AsyncMock(return_value="dispatch-rule-1"),
            ) as ensure_call_scoped_dispatch_rule,
            patch(
                "unity.conversation_manager.local_ingress.local_twilio.add_sip_leg_to_conference",
                new=AsyncMock(return_value="sip-leg-1"),
            ) as add_sip_leg,
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.start_room_egress",
                new=AsyncMock(),
            ),
        ):
            comms_manager = CommsManager(broker)
            comms_manager.loop = asyncio.get_running_loop()
            ingress = LocalCommsIngress(comms_manager)
            await ingress.start()

            try:
                port = ingress._site._server.sockets[0].getsockname()[1]
                async with broker.pubsub() as pubsub:
                    await pubsub.subscribe("app:comms:call_received")
                    async with aiohttp.ClientSession() as session:
                        response = await session.post(
                            f"http://127.0.0.1:{port}/local/twilio/call",
                            data={
                                "To": "+16892256176",
                                "From": "+15555550000",
                                "CallSid": "CA123",
                            },
                        )
                        assert response.status == 200
                        twiml = await response.text()
                        assert "unity_phone_conf_CA123" in twiml

                    message = await _get_message_on_channel(
                        pubsub,
                        "app:comms:call_received",
                    )
                    assert message is not None
                    payload = json.loads(message["data"])
                    assert payload["event_name"] == "PhoneCallReceived"
                    assert (
                        payload["payload"]["conference_name"]
                        == "unity_phone_conf_CA123"
                    )
                    assert (
                        payload["payload"]["room_name"] == "unity_phone_room_42_CA123"
                    )
                    assert payload["payload"]["call_session_id"] == "CA123"
                    assert payload["payload"]["provider_call_sid"] == "CA123"

                make_call_scoped_sip_uri.assert_called_once()
                ensure_call_scoped_dispatch_rule.assert_awaited_once_with(
                    base_phone_number="+16892256176",
                    sip_target="16892256176-CA123",
                    room_name="unity_phone_room_42_CA123",
                    call_id="CA123",
                    assistant_id="42",
                )
                add_sip_leg.assert_awaited_once_with(
                    "unity_phone_conf_CA123",
                    "+16892256176",
                    to_uri="sip:16892256176-CA123@example.sip.livekit.cloud",
                )
            finally:
                await ingress.stop()

    @pytest.mark.asyncio
    async def test_twilio_whatsapp_call_records_session_and_dispatches_metadata(
        self,
        broker,
        patched_local_session,
    ):
        from unity.conversation_manager.comms_manager import CommsManager
        from unity.conversation_manager.local_ingress import LocalCommsIngress

        with (
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_HOST", "127.0.0.1"),
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_PORT", 0),
            patch(
                "unity.conversation_manager.local_ingress.local_email.is_email_configured",
                return_value=False,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_twilio.validate_signature",
                return_value=True,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.make_call_scoped_sip_uri",
                return_value=(
                    "sip:447414266034-CAWA123@example.sip.livekit.cloud",
                    "447414266034-CAWA123",
                ),
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.ensure_call_scoped_dispatch_rule",
                new=AsyncMock(return_value="dispatch-rule-wa"),
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_twilio.add_sip_leg_to_conference",
                new=AsyncMock(return_value="sip-leg-wa"),
            ) as add_sip_leg,
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.start_room_egress",
                new=AsyncMock(),
            ),
        ):
            comms_manager = CommsManager(broker)
            comms_manager.loop = asyncio.get_running_loop()
            ingress = LocalCommsIngress(comms_manager)
            ingress._upsert_whatsapp_call_session = AsyncMock(
                side_effect=lambda payload: payload,
            )
            await ingress.start()

            try:
                port = ingress._site._server.sockets[0].getsockname()[1]
                async with broker.pubsub() as pubsub:
                    await pubsub.subscribe("app:comms:whatsapp_call_received")
                    async with aiohttp.ClientSession() as session:
                        response = await session.post(
                            f"http://127.0.0.1:{port}/local/twilio/whatsapp-call",
                            data={
                                "To": "whatsapp:+447414266034",
                                "From": "whatsapp:+15555550000",
                                "CallSid": "CAWA123",
                            },
                        )
                        assert response.status == 200
                        twiml = await response.text()
                        assert "unity_wa_conf_CAWA123" in twiml

                    message = await _get_message_on_channel(
                        pubsub,
                        "app:comms:whatsapp_call_received",
                    )
                    assert message is not None
                    payload = json.loads(message["data"])
                    assert payload["event_name"] == "WhatsAppCallReceived"
                    assert (
                        payload["payload"]["conference_name"] == "unity_wa_conf_CAWA123"
                    )
                    assert payload["payload"]["room_name"] == "unity_wa_room_42_CAWA123"
                    assert payload["payload"]["call_session_id"] == "CAWA123"
                    assert payload["payload"]["provider_call_sid"] == "CAWA123"

                ingress._upsert_whatsapp_call_session.assert_awaited_once()
                session_payload = ingress._upsert_whatsapp_call_session.await_args.args[
                    0
                ]
                assert session_payload["provider_call_sid"] == "CAWA123"
                assert (
                    session_payload["metadata"]["sip_dispatch_rule_id"]
                    == "dispatch-rule-wa"
                )
                add_sip_leg.assert_awaited_once_with(
                    "unity_wa_conf_CAWA123",
                    "+447414266034",
                    to_uri="sip:447414266034-CAWA123@example.sip.livekit.cloud",
                    whatsapp=True,
                )
            finally:
                await ingress.stop()

    @pytest.mark.asyncio
    async def test_twilio_whatsapp_terminal_status_deletes_dispatch_rule(
        self,
        broker,
        patched_local_session,
    ):
        from unity.conversation_manager.comms_manager import CommsManager
        from unity.conversation_manager.local_ingress import LocalCommsIngress

        with (
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_HOST", "127.0.0.1"),
            patch.object(SETTINGS.conversation, "LOCAL_COMMS_PORT", 0),
            patch(
                "unity.conversation_manager.local_ingress.local_email.is_email_configured",
                return_value=False,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_twilio.validate_signature",
                return_value=True,
            ),
            patch(
                "unity.conversation_manager.local_ingress.local_livekit.delete_sip_dispatch_rule",
                new=AsyncMock(),
            ) as delete_dispatch_rule,
        ):
            ingress = LocalCommsIngress(CommsManager(broker))
            ingress._get_whatsapp_call_session = AsyncMock(
                return_value={
                    "provider_call_sid": "CAWA123",
                    "from_number": "+15551230001",
                    "to_number": "+447414266034",
                    "conference_name": "unity_wa_conf_CAWA123",
                    "livekit_room": "unity_wa_room_42_CAWA123",
                    "metadata": {"sip_dispatch_rule_id": "dispatch-rule-wa"},
                },
            )
            ingress._update_whatsapp_call_session = AsyncMock()
            await ingress.start()

            try:
                port = ingress._site._server.sockets[0].getsockname()[1]
                async with aiohttp.ClientSession() as session:
                    response = await session.post(
                        f"http://127.0.0.1:{port}/local/twilio/whatsapp-call-status",
                        data={
                            "CallSid": "CAWA123",
                            "CallStatus": "completed",
                        },
                    )
                    assert response.status == 200

                ingress._get_whatsapp_call_session.assert_awaited_once_with("CAWA123")
                ingress._update_whatsapp_call_session.assert_awaited_once_with(
                    {
                        "provider": "twilio",
                        "provider_call_sid": "CAWA123",
                        "status": "completed",
                    },
                )
                delete_dispatch_rule.assert_awaited_once_with("dispatch-rule-wa")
            finally:
                await ingress.stop()
