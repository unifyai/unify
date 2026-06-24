"""Shared LiveKit SDK helpers for gateway channels.

Promoted from ``communication/common/livekit.py`` for the channels
in ``unity.gateway`` that bridge external calls into LiveKit rooms
(phone, whatsapp, teams). Functionally equivalent to the
communication-side helpers; only the credential resolution path
changes (env reads -> ``CredentialStore.get``) so the channels stay
decoupled from any deployment-specific config layer.

Scope today
===========

This module ships the four helpers Phase B.2 (``phone/``) needs:

* ``get_livekit_api`` -- LiveKit ``LiveKitAPI`` client factory.
* ``make_sip_uri`` -- builds the SIP URI used to bridge a Twilio
  call into a LiveKit inbound trunk.
* ``ensure_phone_dispatch_rule`` -- idempotently maintains the
  per-trunk dispatch rule that routes inbound SIP calls into the
  correct ``unity_{id}_{medium}`` room.
* ``create_room_and_dispatch_agent`` -- creates a LiveKit room and
  dispatches the LiveKit agent that owns the call session.

The other helpers from the communication-side module
(``make_room_name``, ``start_room_egress``, ``verify_livekit_webhook``)
are also already present in
``unity.conversation_manager.local_providers.livekit`` for the
self-hosted single-process path. When the next channel migration
needs them outside that path, port them here and deprecate the
local_providers copy in a focused commit.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import urlencode

from livekit.api import (
    CreateAgentDispatchRequest,
    CreateSIPDispatchRuleRequest,
    LiveKitAPI,
    SIPDispatchRuleInfo,
)
from livekit.protocol.sip import (
    ListSIPDispatchRuleRequest,
    ListSIPInboundTrunkRequest,
    SIPDispatchRule,
    SIPDispatchRuleDirect,
)

from unity.gateway.credentials import CredentialNotFoundError, CredentialStore

_log = logging.getLogger("unity.gateway.common.livekit")


def get_livekit_api(credentials: CredentialStore) -> LiveKitAPI:
    """Construct a LiveKit API client from configured credentials.

    Reads ``LIVEKIT_URL``, ``LIVEKIT_API_KEY``, and
    ``LIVEKIT_API_SECRET`` from ``credentials``. Raises
    ``RuntimeError`` with a clear message naming all three required
    keys when any is missing.
    """
    try:
        url = credentials.get("LIVEKIT_URL")
        api_key = credentials.get("LIVEKIT_API_KEY")
        api_secret = credentials.get("LIVEKIT_API_SECRET")
    except CredentialNotFoundError as exc:
        raise RuntimeError(
            "LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET must be set",
        ) from exc
    return LiveKitAPI(url=url, api_key=api_key, api_secret=api_secret)


def make_sip_uri(phone_number: str, credentials: CredentialStore) -> str:
    """Build the SIP URI for bridging a Twilio call into LiveKit.

    Uses the E.164 phone number as the user part so LiveKit can match
    it against the inbound SIP trunk's ``numbers`` field. A per-trunk
    dispatch rule (created by ``ensure_phone_dispatch_rule``) then
    routes the SIP participant into the correct
    ``unity_{id}_{medium}`` room.

    ``LIVEKIT_SIP_URI`` is the SIP domain (e.g.
    ``mytenant.sip.livekit.cloud``). Missing or empty domain returns
    a URI with an empty host -- preserving the legacy behaviour so
    test fixtures that don't configure the SIP domain still work.
    """
    sip_domain = credentials.get_optional("LIVEKIT_SIP_URI", "")
    normalized = phone_number if phone_number.startswith("+") else f"+{phone_number}"
    return f"sip:{normalized}@{sip_domain}"


def make_call_scoped_sip_uri(
    phone_number: str,
    call_id: str,
    credentials: CredentialStore,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[str, str]:
    sip_domain = credentials.get_optional("LIVEKIT_SIP_URI", "")
    normalized = phone_number if phone_number.startswith("+") else f"+{phone_number}"
    safe_call_id = "".join(ch if ch.isalnum() else "-" for ch in call_id).strip("-")
    sip_user = f"{normalized[1:]}-{safe_call_id}"
    uri = f"sip:{sip_user}@{sip_domain}"
    if headers:
        sip_headers = {
            key if key.lower().startswith("x-") else f"X-{key}": value
            for key, value in headers.items()
        }
        uri = f"{uri}?{urlencode(sip_headers)}"
    return uri, sip_user


async def ensure_phone_dispatch_rule(
    phone_number: str,
    room_name: str,
    credentials: CredentialStore,
) -> None:
    """Idempotently maintain the SIP dispatch rule for ``phone_number``.

    Routes inbound SIP calls to ``phone_number`` into ``room_name``.
    Skips creation when a matching rule already exists; replaces stale
    rules when the room name has changed (number reassigned to a
    different assistant).

    Best-effort: logs and swallows transport errors so a missing or
    deleted inbound trunk doesn't break the outbound call setup.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        normalized = (
            phone_number if phone_number.startswith("+") else f"+{phone_number}"
        )

        trunks = await livekit_api.sip.list_sip_inbound_trunk(
            ListSIPInboundTrunkRequest(),
        )
        trunk_id = None
        for trunk in trunks.items:
            if normalized in list(trunk.numbers):
                trunk_id = trunk.sip_trunk_id
                break
        if trunk_id is None:
            _log.info(
                "no inbound trunk for %s; skipping dispatch rule creation",
                normalized,
            )
            return

        rules = await livekit_api.sip.list_sip_dispatch_rule(
            ListSIPDispatchRuleRequest(),
        )
        for rule in rules.items:
            if trunk_id not in list(rule.trunk_ids):
                continue
            if (
                rule.rule.HasField("dispatch_rule_direct")
                and rule.rule.dispatch_rule_direct.room_name == room_name
            ):
                return
            await livekit_api.sip.delete_sip_dispatch_rule(
                rule.sip_dispatch_rule_id,
            )

        await livekit_api.sip.create_sip_dispatch_rule(
            CreateSIPDispatchRuleRequest(
                dispatch_rule=SIPDispatchRuleInfo(
                    rule=SIPDispatchRule(
                        dispatch_rule_direct=SIPDispatchRuleDirect(
                            room_name=room_name,
                        ),
                    ),
                    name=f"Unity_phone_{normalized}",
                    trunk_ids=[trunk_id],
                ),
            ),
        )
        _log.info("created dispatch rule: %s -> %s", normalized, room_name)
    except Exception as exc:
        _log.warning(
            "failed to ensure dispatch rule for %s: %s",
            phone_number,
            exc,
        )
    finally:
        await livekit_api.aclose()


async def ensure_call_scoped_dispatch_rule(
    *,
    base_phone_number: str,
    sip_target: str,
    room_name: str,
    call_id: str,
    assistant_id: str,
    credentials: CredentialStore,
) -> str | None:
    livekit_api = get_livekit_api(credentials)
    try:
        normalized = (
            base_phone_number
            if base_phone_number.startswith("+")
            else f"+{base_phone_number}"
        )
        trunks = await livekit_api.sip.list_sip_inbound_trunk(
            ListSIPInboundTrunkRequest(),
        )
        trunk_id = None
        for trunk in trunks.items:
            if normalized in list(trunk.numbers):
                trunk_id = trunk.sip_trunk_id
                break
        if trunk_id is None:
            _log.info(
                "no inbound trunk for %s; skipping call-scoped dispatch rule",
                normalized,
            )
            return None

        dispatch = await livekit_api.sip.create_sip_dispatch_rule(
            CreateSIPDispatchRuleRequest(
                dispatch_rule=SIPDispatchRuleInfo(
                    rule=SIPDispatchRule(
                        dispatch_rule_direct=SIPDispatchRuleDirect(
                            room_name=room_name,
                        ),
                    ),
                    name=f"Unity_call_{call_id}",
                    trunk_ids=[trunk_id],
                    numbers=[sip_target],
                    attributes={
                        "call.id": call_id,
                        "assistant.id": str(assistant_id),
                    },
                ),
            ),
        )
        _log.info(
            "created call-scoped dispatch rule: %s -> %s",
            sip_target,
            room_name,
        )
        return dispatch.sip_dispatch_rule_id
    except Exception as exc:
        _log.warning(
            "failed to ensure call-scoped dispatch rule for %s: %s",
            call_id,
            exc,
        )
        return None
    finally:
        await livekit_api.aclose()


async def delete_sip_dispatch_rule(
    dispatch_rule_id: str | None,
    credentials: CredentialStore,
) -> None:
    if not dispatch_rule_id:
        return
    livekit_api = get_livekit_api(credentials)
    try:
        await livekit_api.sip.delete_sip_dispatch_rule(dispatch_rule_id)
        _log.info("deleted dispatch rule %s", dispatch_rule_id)
    except Exception as exc:
        _log.warning("failed to delete dispatch rule %s: %s", dispatch_rule_id, exc)
    finally:
        await livekit_api.aclose()


async def create_room_and_dispatch_agent(
    room_name: str,
    agent_name: str,
    credentials: CredentialStore,
    metadata: dict | None = None,
) -> Any:
    """Create a LiveKit room and dispatch an agent into it.

    Returns the LiveKit dispatch object. Re-raises on failure after
    logging so the caller's error handler sees the exception.
    """
    livekit_api = get_livekit_api(credentials)
    try:
        dispatch_request = CreateAgentDispatchRequest(
            agent_name=agent_name,
            room=room_name,
            metadata=json.dumps(metadata) if metadata else None,
        )
        dispatch = await livekit_api.agent_dispatch.create_dispatch(
            dispatch_request,
        )
        _log.info(
            "created room %r and dispatched LiveKit agent %r (dispatch_id=%s)",
            room_name,
            agent_name,
            getattr(dispatch, "id", "?"),
        )
        return dispatch
    except Exception as exc:
        _log.error(
            "create_room_and_dispatch_agent failed for room %r: %s",
            room_name,
            exc,
        )
        raise
    finally:
        await livekit_api.aclose()


__all__ = [
    "create_room_and_dispatch_agent",
    "delete_sip_dispatch_rule",
    "ensure_call_scoped_dispatch_rule",
    "ensure_phone_dispatch_rule",
    "get_livekit_api",
    "make_call_scoped_sip_uri",
    "make_sip_uri",
]
