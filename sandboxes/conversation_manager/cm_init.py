"""
ConversationManager initialization helper for the sandbox.

Starts a ConversationManager in-process with:
- No CommsManager (inbound events are injected via the REPL event publisher)
- SMS, email, and outbound call tools stripped from the brain's tool set
  (these require a local gateway and provider credentials OSS users don't have)
- send_unify_message kept (uses Pub/Sub; no gateway admin auth required)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Optional

from sandboxes.conversation_manager.actor_factory import ActorFactory
from sandboxes.conversation_manager.config_manager import ActorConfig
from droid.conversation_manager.event_broker import get_event_broker, reset_event_broker
from droid.conversation_manager.main import run_conversation_manager
from droid.conversation_manager.domains.managers_utils import init_conv_manager

LG = logging.getLogger("conversation_manager_sandbox")


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
        from droid.conversation_manager.domains import managers_utils as _managers_utils

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

    actor = ActorFactory.create_actor(
        cfg,
        args=args,
        progress_callback=progress_callback or print,
    ).actor
    await init_conv_manager(cm, actor=actor)

    # Strip outbound channel tools that require a local gateway and provider
    # credentials (Twilio, Gmail, etc.). Clearing these fields removes send_sms,
    # make_call, and send_email from the brain's available tools and system prompt.
    # send_unify_message is unaffected and stays available.
    try:
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
            get_simulated_user_contact,
        )

        u = get_simulated_user_contact()
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
