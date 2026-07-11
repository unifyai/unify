from __future__ import annotations

import os
import json
import secrets
import time
import uuid

from aiohttp import ClientSession, web

from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON, ICONS
from unify.session_details import SESSION_DETAILS

from .comms_manager import CommsManager
from .local_providers import email as local_email
from .local_providers import livekit as local_livekit
from .local_providers import twilio as local_twilio
from .settings import local_comms_callback_base_url

_LOCAL_PHONE_CALL_SESSIONS: dict[str, dict] = {}
_LOCAL_WHATSAPP_CALL_SESSIONS: dict[str, dict] = {}


def _call_permission_status(button_payload: str) -> tuple[str, str]:
    payload = (button_payload or "").strip()
    if payload == "ACCEPTED":
        return "accepted", "ACCEPTED"
    if payload == "REJECTED":
        return "rejected", "REJECTED"
    return "unknown_interaction", "UNKNOWN"


class LocalCommsIngress:
    """Unity-owned local comms ingress server."""

    def __init__(self, comms_manager: CommsManager):
        self._comms_manager = comms_manager
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._email_poller = local_email.LocalEmailPoller(self._dispatch_payload)
        self._outbox: list[dict] = []
        self._attachments: dict[str, dict] = {}

    @property
    def host(self) -> str:
        from unify.settings import SETTINGS

        return SETTINGS.conversation.LOCAL_COMMS_HOST

    @property
    def port(self) -> int:
        from unify.settings import SETTINGS

        return SETTINGS.conversation.LOCAL_COMMS_PORT

    async def start(self) -> None:
        app = web.Application()
        app.add_routes(
            [
                web.get("/local/comms/health", self._health),
                web.get(
                    "/local/comms/attachments/{attachment_id}",
                    self._get_attachment,
                ),
                web.post("/local/comms/attachments", self._post_attachment),
                web.get("/local/comms/outbox", self._get_outbox),
                web.post("/local/comms/outbox", self._post_outbox),
                web.post("/local/comms/envelope", self._post_envelope),
                web.post("/local/comms/unify-message", self._post_unify_message),
                web.post("/local/comms/unify-reaction", self._post_unify_reaction),
                web.post("/local/comms/api-message", self._post_api_message),
                web.post("/local/comms/unify-meet", self._post_unify_meet),
                web.post("/local/comms/system-event", self._post_system_event),
                web.post("/local/comms/pre-hire", self._post_pre_hire),
                web.post("/local/comms/email", self._post_email),
                web.post("/local/microsoft/outlook", self._post_email),
                web.post("/local/microsoft/teams", self._post_envelope),
                web.post("/local/twilio/sms", self._twilio_sms),
                web.post("/local/twilio/whatsapp", self._twilio_whatsapp),
                web.post("/local/twilio/call", self._twilio_call),
                web.post("/local/twilio/call-status", self._twilio_call_status),
                web.post("/local/twilio/twiml", self._twilio_twiml),
                web.post("/local/twilio/whatsapp-call", self._twilio_whatsapp_call),
                web.post(
                    "/local/twilio/whatsapp-call-status",
                    self._twilio_whatsapp_call_status,
                ),
                web.post("/local/twilio/whatsapp-status", self._ok),
                web.post("/local/twilio/conference-status", self._ok),
                web.post(
                    "/local/livekit/recording-complete",
                    self._livekit_recording_complete,
                ),
            ],
        )

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()
        if local_email.is_email_configured():
            await self._email_poller.start()
        LOGGER.info(
            f"{ICONS['subscription']} Local comms ingress listening on "
            f"http://{self.host}:{self.port}",
        )

    async def stop(self) -> None:
        await self._email_poller.stop()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    async def _dispatch_payload(self, payload: dict) -> None:
        await self._comms_manager.dispatch_envelope_payload(payload)

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def _ok(self, request: web.Request) -> web.Response:
        return web.Response(status=200)

    async def _get_outbox(self, request: web.Request) -> web.Response:
        items = list(self._outbox)
        if request.query.get("clear", "true").lower() != "false":
            self._outbox.clear()
        return web.json_response({"items": items})

    async def _post_outbox(self, request: web.Request) -> web.Response:
        payload = await self._json_or_form(request)
        if "thread" not in payload or "event" not in payload:
            raise web.HTTPBadRequest(text="Expected {thread, event}")
        if "publish_timestamp" not in payload:
            payload["publish_timestamp"] = time.time()
        self._outbox.append(payload)
        return web.json_response({"success": True})

    async def _post_attachment(self, request: web.Request) -> web.Response:
        payload = await self._json_or_form(request)
        attachment_id = payload.get("id", "")
        if not attachment_id:
            raise web.HTTPBadRequest(text="attachment id is required")
        self._attachments[attachment_id] = payload
        return web.json_response({"success": True})

    async def _get_attachment(self, request: web.Request) -> web.Response:
        attachment_id = request.match_info["attachment_id"]
        attachment = self._attachments.get(attachment_id)
        if attachment is None:
            raise web.HTTPNotFound()
        from unify.conversation_manager.domains.comms_utils import (
            _inline_attachment_bytes,
        )

        body = _inline_attachment_bytes(attachment) or b""
        return web.Response(
            body=body,
            content_type=attachment.get("content_type", "application/octet-stream"),
        )

    async def _json_or_form(self, request: web.Request) -> dict:
        content_type = request.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return await request.json()
        form = await request.post()
        return dict(form)

    def _current_assistant_id(self) -> str:
        agent_id = SESSION_DETAILS.assistant.agent_id
        return "" if agent_id is None else str(agent_id)

    def _validate_assistant_id(self, assistant_id: str | None) -> None:
        current = self._current_assistant_id()
        if assistant_id and current and str(assistant_id) != current:
            raise web.HTTPBadRequest(
                text="assistant_id does not match the local session",
            )

    async def _require_admin_key(self, request: web.Request) -> None:
        from unify.settings import SETTINGS

        expected = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if not expected:
            return
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise web.HTTPForbidden(text="Missing admin key")
        received = auth_header.removeprefix("Bearer ").strip()
        if not secrets.compare_digest(received, expected):
            raise web.HTTPForbidden(text="Invalid admin key")

    async def _validate_twilio(self, request: web.Request, *, whatsapp: bool) -> dict:
        from unify.settings import SETTINGS

        form = await request.post()
        params = {key: value for key, value in form.items()}
        signature = request.headers.get("X-Twilio-Signature", "")
        url = (
            f"{local_comms_callback_base_url(SETTINGS.conversation)}"
            f"{request.rel_url}"
        )
        if not local_twilio.validate_signature(
            url,
            params,
            signature,
            whatsapp=whatsapp,
        ):
            raise web.HTTPForbidden(text="Invalid Twilio signature")
        return params

    async def _post_envelope(self, request: web.Request) -> web.Response:
        payload = await self._json_or_form(request)
        if "thread" not in payload or "event" not in payload:
            raise web.HTTPBadRequest(text="Expected {thread, event}")
        self._validate_assistant_id(
            payload.get("event", {}).get("assistant_id") or payload.get("assistant_id"),
        )
        if "publish_timestamp" not in payload:
            payload["publish_timestamp"] = time.time()
        await self._dispatch_payload(payload)
        return web.json_response({"success": True})

    async def _post_unify_message(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        contact_id = payload.get("contact_id")
        if contact_id is None:
            raise web.HTTPBadRequest(text="contact_id is required")
        await self._dispatch_payload(
            {
                "thread": "unify_message",
                "publish_timestamp": time.time(),
                "event": {
                    "contact_id": contact_id,
                    "contacts": payload.get("contacts") or [],
                    "assistant_id": self._current_assistant_id(),
                    "body": payload.get("body", "") or payload.get("Body", "") or "",
                    "attachments": payload.get("attachments") or [],
                },
            },
        )
        return web.json_response({"success": True})

    async def _post_unify_reaction(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        contact_id = payload.get("contact_id")
        target_message_id = payload.get("target_message_id")
        if contact_id is None or target_message_id is None:
            raise web.HTTPBadRequest(
                text="contact_id and target_message_id are required",
            )
        emoji = payload.get("emoji")
        if emoji == "":
            emoji = None
        await self._dispatch_payload(
            {
                "thread": "unify_message_reaction",
                "publish_timestamp": time.time(),
                "event": {
                    "contact_id": contact_id,
                    "contacts": payload.get("contacts") or [],
                    "assistant_id": self._current_assistant_id(),
                    "target_message_id": target_message_id,
                    "emoji": emoji,
                },
            },
        )
        return web.json_response({"success": True})

    async def _post_api_message(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        api_message_id = payload.get("api_message_id", "")
        if not api_message_id:
            raise web.HTTPBadRequest(text="api_message_id is required")
        await self._dispatch_payload(
            {
                "thread": "api_message",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": self._current_assistant_id(),
                    "api_message_id": api_message_id,
                    "contact_id": payload.get("contact_id", 1),
                    "body": payload.get("body", "") or "",
                    "attachments": payload.get("attachments") or [],
                    "tags": payload.get("tags") or [],
                },
            },
        )
        return web.json_response({"success": True})

    async def _post_unify_meet(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        room_name = payload.get("room_name", "") or payload.get("livekit_room", "")
        if not room_name:
            raise web.HTTPBadRequest(text="room_name is required")
        event = {
            "assistant_id": self._current_assistant_id(),
            "contacts": payload.get("contacts") or [],
            "livekit_room": room_name,
            "timestamp": int(time.time() * 1000),
        }
        opening_config = payload.get("opening_config")
        if isinstance(opening_config, str) and opening_config:
            opening_config = json.loads(opening_config)
        if isinstance(opening_config, dict):
            event["opening_config"] = opening_config
        LOGGER.info(
            "%s [LocalIngressDebug] unify_meet.received assistant_id=%s "
            "current_assistant_id=%s room=%s call_session_id=%s opening_mode=%s",
            ICONS["comms"],
            payload.get("assistant_id"),
            event["assistant_id"],
            room_name,
            payload.get("call_session_id") or "",
            opening_config.get("mode") if isinstance(opening_config, dict) else None,
        )
        await self._dispatch_payload(
            {
                "thread": "unify_meet",
                "publish_timestamp": time.time(),
                "event": event,
            },
        )
        LOGGER.info(
            "%s [LocalIngressDebug] unify_meet.dispatched assistant_id=%s room=%s call_session_id=%s",
            ICONS["comms"],
            event["assistant_id"],
            room_name,
            payload.get("call_session_id") or "",
        )
        return web.json_response({"success": True})

    async def _post_system_event(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        event_type = payload.get("event_type", "")
        if not event_type:
            raise web.HTTPBadRequest(text="event_type is required")
        extra_event_fields = payload.get("extra_event_fields") or {}
        if not isinstance(extra_event_fields, dict):
            extra_event_fields = {}
        event = {
            "assistant_id": self._current_assistant_id(),
            "contacts": payload.get("contacts") or [],
            "event_type": event_type,
            "message": payload.get("message", "") or "",
            "task_id": payload.get("task_id"),
            "source_task_log_id": payload.get("source_task_log_id"),
            "activation_revision": payload.get("activation_revision", "") or "",
            "scheduled_for": payload.get("scheduled_for", "") or "",
            "execution_mode": payload.get("execution_mode", "") or "",
            "source_type": payload.get("source_type", "") or "",
            "binding_id": payload.get("binding_id", "") or "",
            "desktop_url": payload.get("desktop_url", "") or "",
            "vm_type": payload.get("vm_type", "") or "",
        }
        event.update(extra_event_fields)
        await self._dispatch_payload(
            {
                "thread": "unity_system_event",
                "publish_timestamp": time.time(),
                "event": event,
            },
        )
        return web.json_response({"success": True})

    async def _post_pre_hire(self, request: web.Request) -> web.Response:
        await self._require_admin_key(request)
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        body = payload.get("body")
        if not isinstance(body, list):
            raise web.HTTPBadRequest(text="body must be a list of {role, msg}")
        await self._dispatch_payload(
            {
                "thread": "log_pre_hire_chats",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": self._current_assistant_id(),
                    "body": body,
                },
            },
        )
        return web.json_response({"success": True})

    async def _post_email(self, request: web.Request) -> web.Response:
        payload = await self._json_or_form(request)
        self._validate_assistant_id(payload.get("assistant_id"))
        await self._dispatch_payload(
            {
                "thread": "email",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": self._current_assistant_id(),
                    "contacts": payload.get("contacts") or [],
                    "from": payload.get("from", "") or "",
                    "subject": payload.get("subject", "") or "",
                    "body": payload.get("body", "") or "",
                    "email_id": payload.get("email_id", "") or "",
                    "thread_id": payload.get("thread_id", "") or "",
                    "attachments": payload.get("attachments") or [],
                    "to": payload.get("to") or [],
                    "cc": payload.get("cc") or [],
                    "bcc": payload.get("bcc") or [],
                },
            },
        )
        return web.json_response({"success": True})

    async def _twilio_sms(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=False)
        await self._dispatch_payload(
            {
                "thread": "msg",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": self._current_assistant_id(),
                    "contacts": [],
                    "to_number": form.get("To", "") or "",
                    "from_number": form.get("From", "") or "",
                    "body": form.get("Body", "") or "",
                },
            },
        )
        return web.Response(
            text=local_twilio.empty_message_response(),
            content_type="text/xml",
        )

    async def _twilio_whatsapp(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=True)
        body = form.get("Body", "") or ""
        from_number = form.get("From", "") or ""
        to_number = form.get("To", "") or ""
        if body == "VOICE_CALL_REQUEST":
            contact_number = from_number.replace("whatsapp:", "").strip()
            pool_number = to_number.replace("whatsapp:", "").strip()
            permission_status, event_payload = _call_permission_status(
                form.get("ButtonPayload", ""),
            )
            await self._record_whatsapp_call_permission(
                pool_number=pool_number,
                contact_number=contact_number,
                status=permission_status,
            )
            await self._dispatch_payload(
                {
                    "thread": "whatsapp",
                    "publish_timestamp": time.time(),
                    "event": {
                        "assistant_id": self._current_assistant_id(),
                        "contacts": [],
                        "type": "call_permission_response",
                        "contact_number": contact_number,
                        "payload": event_payload,
                    },
                },
            )
        else:
            attachments = await local_twilio.fetch_whatsapp_attachments(form)
            message_sid = form.get("MessageSid") or ""
            reaction_type = (form.get("MessageType") or form.get("type") or "").lower()
            reaction_emoji = form.get("Reaction") or form.get("reaction_emoji")
            reacted_to_sid = (
                form.get("OriginalRepliedMessageSid")
                or form.get("reaction_message_id")
                or form.get("RepliedMessageSid")
                or ""
            )
            if reaction_type == "reaction" or reacted_to_sid:
                await self._dispatch_payload(
                    {
                        "thread": "whatsapp_reaction",
                        "publish_timestamp": time.time(),
                        "event": {
                            "assistant_id": self._current_assistant_id(),
                            "contacts": [],
                            "from_number": from_number,
                            "to_number": to_number,
                            "provider_message_sid": reacted_to_sid or message_sid,
                            "message_sid": reacted_to_sid or message_sid,
                            "emoji": reaction_emoji or None,
                        },
                    },
                )
            else:
                await self._dispatch_payload(
                    {
                        "thread": "whatsapp",
                        "publish_timestamp": time.time(),
                        "event": {
                            "assistant_id": self._current_assistant_id(),
                            "contacts": [],
                            "to_number": to_number,
                            "from_number": from_number,
                            "body": body,
                            "attachments": attachments,
                            "message_sid": message_sid,
                        },
                    },
                )
        return web.Response(
            text=local_twilio.empty_message_response(),
            content_type="text/xml",
        )

    async def _record_whatsapp_call_permission(
        self,
        *,
        pool_number: str,
        contact_number: str,
        status: str,
    ) -> None:
        from unify.settings import SETTINGS

        admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if not admin_key:
            return

        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-permission",
                    headers={"Authorization": f"Bearer {admin_key}"},
                    json={
                        "pool_number": pool_number,
                        "contact_number": contact_number,
                        "status": status,
                        "source": "local_ingress",
                    },
                    timeout=10,
                ) as response:
                    response.raise_for_status()
        except Exception as exc:
            LOGGER.error(
                f"{DEFAULT_ICON} Failed to record WhatsApp call permission: {exc}",
            )

    async def _upsert_whatsapp_call_session(self, payload: dict) -> dict:
        from unify.settings import SETTINGS

        provider_call_sid = payload["provider_call_sid"]
        _LOCAL_WHATSAPP_CALL_SESSIONS[provider_call_sid] = payload
        admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if not admin_key:
            return payload

        try:
            async with ClientSession() as session:
                async with session.post(
                    f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session",
                    headers={"Authorization": f"Bearer {admin_key}"},
                    json=payload,
                    timeout=10,
                ) as response:
                    if response.status == 404:
                        return payload
                    response.raise_for_status()
                    data = await response.json()
                    _LOCAL_WHATSAPP_CALL_SESSIONS[provider_call_sid] = data
                    return data
        except Exception as exc:
            LOGGER.error(
                f"{DEFAULT_ICON} Failed to upsert local WhatsApp call session: {exc}",
            )
            return payload

    async def _get_whatsapp_call_session(self, provider_call_sid: str) -> dict | None:
        from unify.settings import SETTINGS

        admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if not admin_key:
            return _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)

        try:
            async with ClientSession() as session:
                async with session.get(
                    f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session/{provider_call_sid}",
                    headers={"Authorization": f"Bearer {admin_key}"},
                    params={"provider": "twilio"},
                    timeout=10,
                ) as response:
                    if response.status == 404:
                        return _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)
                    response.raise_for_status()
                    return await response.json()
        except Exception as exc:
            LOGGER.error(
                f"{DEFAULT_ICON} Failed to read local WhatsApp call session: {exc}",
            )
            return _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)

    async def _update_whatsapp_call_session(self, payload: dict) -> dict | None:
        from unify.settings import SETTINGS

        provider_call_sid = payload["provider_call_sid"]
        existing = _LOCAL_WHATSAPP_CALL_SESSIONS.get(provider_call_sid)
        if existing:
            metadata = dict(existing.get("metadata") or {})
            metadata.update(payload.get("metadata") or {})
            existing.update({k: v for k, v in payload.items() if v is not None})
            existing["metadata"] = metadata

        admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if not admin_key:
            return existing

        try:
            async with ClientSession() as session:
                async with session.patch(
                    f"{SETTINGS.ORCHESTRA_URL}/admin/whatsapp/call-session",
                    headers={"Authorization": f"Bearer {admin_key}"},
                    json=payload,
                    timeout=10,
                ) as response:
                    if response.status == 404:
                        return existing
                    response.raise_for_status()
                    data = await response.json()
                    _LOCAL_WHATSAPP_CALL_SESSIONS[provider_call_sid] = data
                    return data
        except Exception as exc:
            LOGGER.error(
                f"{DEFAULT_ICON} Failed to update local WhatsApp call session: {exc}",
            )
            return existing

    async def _twilio_call(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=False)
        to_number = form.get("To", "") or ""
        from_number = form.get("From", "") or ""
        provider_call_sid = form.get("CallSid", "") or f"missing-{uuid.uuid4()}"
        assistant_id = self._current_assistant_id()
        call_id = provider_call_sid.replace(":", "-")
        conference_name = f"unity_phone_conf_{call_id}"
        room_name = f"unity_phone_room_{assistant_id}_{call_id}"
        sip_uri, sip_target = local_livekit.make_call_scoped_sip_uri(
            to_number,
            call_id,
            headers={
                "X-Unity-Call-Session": call_id,
                "X-Unity-Provider-Call-Sid": provider_call_sid,
                "X-Unity-Room": room_name,
            },
        )
        sip_dispatch_rule_id = await local_livekit.ensure_call_scoped_dispatch_rule(
            base_phone_number=to_number,
            sip_target=sip_target,
            room_name=room_name,
            call_id=call_id,
            assistant_id=assistant_id,
        )
        if not sip_dispatch_rule_id:
            return web.Response(
                text=local_twilio.call_unavailable_response(),
                content_type="text/xml",
            )
        _LOCAL_PHONE_CALL_SESSIONS[provider_call_sid] = {
            "provider_call_sid": provider_call_sid,
            "from_number": from_number,
            "to_number": to_number,
            "conference_name": conference_name,
            "livekit_room": room_name,
            "metadata": {"sip_dispatch_rule_id": sip_dispatch_rule_id},
        }
        await self._dispatch_payload(
            {
                "thread": "call",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": assistant_id,
                    "contacts": [],
                    "conference_name": conference_name,
                    "call_session_id": provider_call_sid,
                    "provider_call_sid": provider_call_sid,
                    "caller_number": from_number,
                    "sip_uri": sip_uri,
                    "livekit_room": room_name,
                    "action": "start_worker",
                    "timestamp": int(time.time() * 1000),
                    "call_metadata": {
                        "twilio_number": to_number,
                        "call_type": "inbound",
                        "room_created": True,
                        "bridge_established": True,
                        "sip_dispatch_rule_id": sip_dispatch_rule_id,
                    },
                },
            },
        )
        await local_twilio.add_sip_leg_to_conference(
            conference_name,
            to_number,
            to_uri=sip_uri,
        )
        try:
            await local_livekit.start_room_egress(
                room_name,
                assistant_id,
                SESSION_DETAILS.user.id,
            )
        except Exception as exc:
            LOGGER.error(f"{DEFAULT_ICON} Failed to start room egress: {exc}")
        return web.Response(
            text=local_twilio.create_conference_response(conference_name),
            content_type="text/xml",
        )

    async def _twilio_call_status(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=False)
        call_status = form.get("CallStatus", "")
        provider_call_sid = form.get("CallSid", "") or ""
        if call_status == "completed":
            thread = None
        else:
            thread = (
                "call_answered"
                if call_status == "in-progress"
                else (
                    "call_not_answered"
                    if call_status in {"no-answer", "busy", "canceled", "failed"}
                    else None
                )
            )
        if thread is None and call_status != "completed":
            return web.Response(status=200)
        call_session = _LOCAL_PHONE_CALL_SESSIONS.get(provider_call_sid, {})
        if call_status in {"no-answer", "busy", "canceled", "failed", "completed"}:
            await local_livekit.delete_sip_dispatch_rule(
                (call_session.get("metadata") or {}).get("sip_dispatch_rule_id"),
            )
            _LOCAL_PHONE_CALL_SESSIONS.pop(provider_call_sid, None)
        if call_status != "completed":
            await self._dispatch_payload(
                {
                    "thread": thread,
                    "publish_timestamp": time.time(),
                    "event": {
                        "assistant_id": self._current_assistant_id(),
                        "contacts": [],
                        "user_number": call_session.get("from_number")
                        or form.get("From", "")
                        or "",
                        "assistant_number": call_session.get("to_number")
                        or form.get("To", "")
                        or "",
                        "call_status": call_status,
                        "call_session_id": provider_call_sid,
                        "provider_call_sid": provider_call_sid,
                        "conference_name": call_session.get("conference_name", ""),
                        "livekit_room": call_session.get("livekit_room", ""),
                        "timestamp": int(time.time() * 1000),
                    },
                },
            )
        return web.Response(status=200)

    async def _twilio_twiml(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=False)
        twilio_number = form.get("From", "") or ""
        phone_number = request.query.get("phone_number", "")
        if not phone_number:
            raise web.HTTPBadRequest(text="phone_number is required")
        return web.Response(
            text=local_twilio.build_outbound_call_twiml(twilio_number, phone_number),
            content_type="text/xml",
        )

    async def _twilio_whatsapp_call(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=True)
        to_number = (form.get("To", "") or "").replace("whatsapp:", "").strip()
        from_number = (form.get("From", "") or "").replace("whatsapp:", "").strip()
        provider_call_sid = form.get("CallSid", "") or f"missing-{uuid.uuid4()}"
        assistant_id = self._current_assistant_id()
        call_id = provider_call_sid.replace(":", "-")
        conference_name = f"unity_wa_conf_{call_id}"
        room_name = f"unity_wa_room_{assistant_id}_{call_id}"
        sip_uri, sip_target = local_livekit.make_call_scoped_sip_uri(
            to_number,
            call_id,
            headers={
                "X-Unity-Call-Session": call_id,
                "X-Unity-Provider-Call-Sid": provider_call_sid,
                "X-Unity-Room": room_name,
            },
        )
        sip_dispatch_rule_id = await local_livekit.ensure_call_scoped_dispatch_rule(
            base_phone_number=to_number,
            sip_target=sip_target,
            room_name=room_name,
            call_id=call_id,
            assistant_id=assistant_id,
        )
        if not sip_dispatch_rule_id:
            return web.Response(
                text=local_twilio.call_unavailable_response(),
                content_type="text/xml",
            )
        await self._upsert_whatsapp_call_session(
            {
                "provider": "twilio",
                "provider_call_sid": provider_call_sid,
                "channel": "whatsapp_call",
                "assistant_id": int(assistant_id),
                "from_number": from_number,
                "to_number": to_number,
                "pool_number": to_number,
                "conference_name": conference_name,
                "livekit_room": room_name,
                "status": "created",
                "metadata": {
                    "sip_uri": sip_uri,
                    "sip_target": sip_target,
                    "sip_dispatch_rule_id": sip_dispatch_rule_id,
                },
            },
        )
        await self._dispatch_payload(
            {
                "thread": "whatsapp_call",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": assistant_id,
                    "contacts": [],
                    "conference_name": conference_name,
                    "call_session_id": provider_call_sid,
                    "provider_call_sid": provider_call_sid,
                    "caller_number": from_number,
                    "sip_uri": sip_uri,
                    "livekit_room": room_name,
                    "action": "start_worker",
                    "timestamp": int(time.time() * 1000),
                    "call_metadata": {
                        "whatsapp_number": to_number,
                        "call_type": "inbound",
                        "room_created": True,
                        "bridge_established": True,
                        "sip_dispatch_rule_id": sip_dispatch_rule_id,
                    },
                },
            },
        )
        await local_twilio.add_sip_leg_to_conference(
            conference_name,
            to_number,
            to_uri=sip_uri,
            whatsapp=True,
        )
        try:
            await local_livekit.start_room_egress(
                room_name,
                assistant_id,
                SESSION_DETAILS.user.id,
            )
        except Exception as exc:
            LOGGER.error(f"{DEFAULT_ICON} Failed to start WhatsApp room egress: {exc}")
        return web.Response(
            text=local_twilio.create_conference_response(conference_name),
            content_type="text/xml",
        )

    async def _twilio_whatsapp_call_status(self, request: web.Request) -> web.Response:
        form = await self._validate_twilio(request, whatsapp=True)
        call_status = form.get("CallStatus", "")
        provider_call_sid = form.get("CallSid", "") or ""
        if call_status == "completed":
            thread = None
        else:
            thread = (
                "whatsapp_call_answered"
                if call_status == "in-progress"
                else (
                    "whatsapp_call_not_answered"
                    if call_status in {"no-answer", "busy", "canceled", "failed"}
                    else None
                )
            )
        if thread is None and call_status != "completed":
            return web.Response(status=200)
        if not provider_call_sid:
            return web.Response(status=200)

        call_session = await self._get_whatsapp_call_session(provider_call_sid)
        if not call_session:
            return web.Response(status=200)

        await self._update_whatsapp_call_session(
            {
                "provider": "twilio",
                "provider_call_sid": provider_call_sid,
                "status": call_status,
            },
        )
        metadata = call_session.get("metadata") or {}
        if call_status in {"no-answer", "busy", "canceled", "failed", "completed"}:
            await local_livekit.delete_sip_dispatch_rule(
                metadata.get("sip_dispatch_rule_id"),
            )
            _LOCAL_WHATSAPP_CALL_SESSIONS.pop(provider_call_sid, None)
        if call_status != "completed":
            await self._dispatch_payload(
                {
                    "thread": thread,
                    "publish_timestamp": time.time(),
                    "event": {
                        "assistant_id": self._current_assistant_id(),
                        "contacts": [],
                        "user_number": call_session.get("from_number", ""),
                        "assistant_number": call_session.get("to_number", ""),
                        "call_status": call_status,
                        "call_session_id": provider_call_sid,
                        "provider_call_sid": provider_call_sid,
                        "conference_name": call_session.get("conference_name", ""),
                        "livekit_room": call_session.get("livekit_room", ""),
                        "timestamp": int(time.time() * 1000),
                    },
                },
            )
        return web.Response(status=200)

    async def _livekit_recording_complete(self, request: web.Request) -> web.Response:
        body = (await request.read()).decode()
        auth_token = request.headers.get("Authorization", "")
        try:
            event = local_livekit.verify_livekit_webhook(body, auth_token)
        except Exception:
            raise web.HTTPUnauthorized()

        if event.event != "egress_ended":
            return web.Response(status=200)
        egress_info = event.egress_info
        if not egress_info.file_results:
            return web.Response(status=200)

        bucket = request.query.get("bucket") or os.environ.get(
            "LIVEKIT_EGRESS_GCS_BUCKET",
            "",
        )
        file_result = egress_info.file_results[0]
        recording_url = (
            f"https://storage.googleapis.com/{bucket}/{file_result.filename}"
            if bucket
            else file_result.filename
        )
        await self._dispatch_payload(
            {
                "thread": "recording_ready",
                "publish_timestamp": time.time(),
                "event": {
                    "assistant_id": request.query.get(
                        "assistant_id",
                        self._current_assistant_id(),
                    ),
                    "user_id": request.query.get("user_id", ""),
                    "conference_name": request.query.get(
                        "room_name",
                        egress_info.room_name,
                    ),
                    "recording_url": recording_url,
                },
            },
        )
        return web.json_response({"success": True})
