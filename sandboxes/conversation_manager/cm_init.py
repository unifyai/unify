"""
ConversationManager initialization helper for the sandbox.

Starts a ConversationManager in-process with:
- No CommsManager (inbound events are injected via the REPL event publisher)
- send_email always stripped (OAuth token complexity makes it unsupported for OSS)
- SMS and outbound call tools kept only when Twilio outbound credentials are
  configured; stripped otherwise even if the local gateway is running
- send_unify_message kept and backed by an in-memory outbound transport so the
  tool succeeds without needing GCP credentials or a live Pub/Sub topic
- is_coordinator synced from the Orchestra API at startup so voice prompts and
  tool routing behave identically to a hosted coordinator session
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from sandboxes.conversation_manager.actor_factory import ActorFactory
from sandboxes.conversation_manager.config_manager import ActorConfig
from unity.conversation_manager.event_broker import get_event_broker, reset_event_broker
from unity.conversation_manager.main import run_conversation_manager
from unity.conversation_manager.domains.managers_utils import init_conv_manager

LG = logging.getLogger("conversation_manager_sandbox")


def _sync_coordinator_flag(cm) -> None:
    """Fetch is_coordinator from Orchestra and apply it to the CM + SESSION_DETAILS.

    The CommsManager startup event normally carries this flag, but in sandbox
    mode CommsManager is disabled.  We call GET /v0/assistant with the user's
    UNIFY_KEY and match by agent_id so voice prompts and tool routing behave
    identically to a hosted coordinator session.

    Errors are swallowed — the sandbox continues with is_coordinator=False if
    the API call fails.
    """
    import os

    import httpx

    from unity.session_details import SESSION_DETAILS

    agent_id = SESSION_DETAILS.assistant.agent_id
    if agent_id is None:
        return

    orchestra_url = os.environ.get("ORCHESTRA_URL", "").rstrip("/")
    unify_key = os.environ.get("UNIFY_KEY", "")
    if not orchestra_url or not unify_key:
        return

    try:
        resp = httpx.get(
            f"{orchestra_url}/assistant",
            headers={"Authorization": f"Bearer {unify_key}"},
            timeout=10,
        )
        if resp.status_code != 200:
            return
        assistants = (resp.json() or {}).get("info") or []
        for a in assistants:
            if str(a.get("id") or a.get("agent_id", "")) == str(agent_id):
                if a.get("is_coordinator"):
                    SESSION_DETAILS.assistant.is_coordinator = True
                    cm.is_coordinator = True
                    cm.call_manager.set_config(cm.get_call_config())
                    LG.info(
                        "Coordinator flag applied from Orchestra (assistant_id=%s)",
                        agent_id,
                    )
                # Populate user identity fields from the assistant record.  In
                # the hosted stack these come from the CommsManager startup
                # event; in sandbox mode we pull them here instead so the
                # voice agent prompt and contact dict reflect the real user.
                _identity = {
                    "user_first_name": (a.get("user_first_name") or ""),
                    "user_surname": (a.get("user_last_name") or ""),
                    "user_number": (a.get("user_phone") or ""),
                    "user_email": (a.get("user_email") or ""),
                }
                for attr, value in _identity.items():
                    if value and not getattr(cm, attr, None):
                        setattr(cm, attr, value)
                        LG.debug("%s set from Orchestra: %s", attr, value)
                break
    except Exception as exc:
        LG.debug("Could not fetch assistant info from Orchestra: %s", exc)


async def initialize_cm(
    *,
    args: Any,
    progress_callback: Optional[Callable[[str], None]] = None,
):
    """
    Initialize ConversationManager for the sandbox.

    Returns:
        ConversationManager instance
    """
    # Ensure global broker is fresh; CM internals use get_event_broker() in places.
    try:
        reset_event_broker()
    except Exception:
        pass
    event_broker = get_event_broker()
    # Sandbox resets the global broker so each run starts clean. Some CM modules
    # cache the broker at import time (e.g. `managers_utils.event_broker`), so
    # repoint those caches to the freshly reset instance to keep all event
    # publications visible to the sandbox subscriber.
    try:  # sandbox-only best-effort patching
        from unity.conversation_manager.domains import managers_utils as _managers_utils

        _managers_utils.event_broker = event_broker  # type: ignore[attr-defined]
    except Exception:
        pass

    cfg: ActorConfig = getattr(
        args,
        "_actor_config",
        ActorConfig(actor_type="codeact_real"),
    )

    cm = await run_conversation_manager(
        project_name=args.project_name,
        event_broker=event_broker,
        stop_event=asyncio.Event(),
        enable_comms_manager=False,
        apply_test_mocks=False,
    )

    # Without CommsManager the outbound transport singleton is never set, so
    # send_unify_message falls back to the raw GCP Pub/Sub path and fails with
    # a 400 (empty GCP_PROJECT_ID). Inject an in-memory transport so the tool
    # returns success. The REPL display is driven by the event broker, not
    # Pub/Sub, so no response visibility is lost.
    try:
        from unity.gateway.outbound_inmemory import InMemoryOutboundTransport
        from unity.conversation_manager.domains.comms_utils import (
            set_outbound_transport,
        )

        set_outbound_transport(InMemoryOutboundTransport())
    except Exception:
        pass

    actor = ActorFactory.create_actor(
        cfg,
        args=args,
        progress_callback=progress_callback or print,
    ).actor
    await init_conv_manager(cm, actor=actor)

    # Sync coordinator flag from Orchestra so voice prompts/tool routing match
    # a hosted coordinator session (no CommsManager startup event in sandbox).
    try:
        _sync_coordinator_flag(cm)
    except Exception:
        pass

    # Strip outbound channel tools based on what the gateway supports.
    #
    # send_email: always stripped — OAuth token lifecycle is too complex for OSS.
    # send_sms / make_call: stripped unless Twilio outbound credentials are
    #   configured, even though the local gateway always runs for UniLLM proxying.
    try:
        from sandboxes.conversation_manager.gateway_bootstrap import (
            outbound_comms_configured,
        )

        if not outbound_comms_configured():
            cm.assistant_number = ""
        cm.assistant_email = ""
    except Exception:
        pass

    # Ensure the system user contact (contact_id=1) has basic identity details
    # populated in the Contacts table.
    #
    # ContactManager provisions system contacts from `SESSION_DETAILS` / API.
    # When those aren't initialized (common locally), the default user contact
    # may be missing `phone_number` / `email_address`, which makes brain actions
    # like `send_sms` / `send_email` fail with an Error event.
    #
    # Reads from env vars: USER_FIRST_NAME, USER_SURNAME, USER_NUMBER, USER_EMAIL.
    try:
        from sandboxes.conversation_manager.event_publisher import (
            get_user_contact,
        )

        u = get_user_contact(cm)
        if getattr(cm, "contact_manager", None) is not None:
            cm.contact_manager.update_contact(  # type: ignore[union-attr]
                contact_id=1,
                first_name=u.get("first_name") or None,
                surname=u.get("surname") or None,
                phone_number=u.get("phone_number") or None,
                email_address=u.get("email_address") or None,
                should_respond=True,
            )
    except Exception:
        pass

    LG.info(
        "ConversationManager initialized (actor=%s)",
        cfg.actor_type,
    )
    return cm


async def shutdown_cm(cm) -> None:
    """Best-effort shutdown of the running CM instance."""
    try:
        cm.stop.set()
    except Exception:
        pass
    try:
        await asyncio.wait_for(cm.cleanup(), timeout=10.0)
    except Exception:
        pass
