"""One flow exercising the core state managers through the live brain.

Folds the contact and knowledge coverage together with secret storage so a
single smoke test confirms the assistant can read and write the main
assistant-owned state surfaces (contacts, knowledge, secrets) against real
Orchestra, then recall that state in a grounded reply.
"""

from __future__ import annotations

import uuid

import pytest

from tests.flows.harness import FlowHarness, assert_primitive_invoked
from tests.helpers import capture_events


@pytest.mark.asyncio
async def test_core_state_managers_persist_and_recall(
    flow_session: FlowHarness,
) -> None:
    """contacts + knowledge + secrets: write via the brain, then recall.

    Each turn asserts both the user-visible outcome (persisted row / grounded
    reply) AND that the brain reached it through the intended primitive — the
    CodeAct actor could otherwise satisfy these requests with a shell command or
    a direct Orchestra write, which would pass an outcome-only check while
    silently bypassing the state-manager surface the product depends on.
    """

    token = uuid.uuid4().hex[:8]

    # Knowledge: seed a typed claim the assistant must ground its later answer on.
    codename = f"orbit-{token}"
    flow_session.seed_knowledge_claims(
        claims=[
            {
                "title": f"Launch codename {token}",
                "content": f"The launch codename is {codename}.",
                "kind": "fact",
                "topics": ["launch", "flows"],
            },
        ],
    )

    # Contacts: natural-language create -> primitives.contacts.update -> row.
    contact_first = f"Elena{token}"
    contact_email = f"elena.{token}@example.com"
    async with capture_events("ManagerMethod") as contact_events:
        await flow_session.inject_unify_message(
            f"Add a new contact named {contact_first} Flow with email {contact_email}.",
        )
        contact = await flow_session.wait_for_contact_email(
            contact_email,
            timeout=240.0,
        )
    persisted_email = (
        contact.get("email_address")
        if isinstance(contact, dict)
        else contact.email_address
    )
    assert persisted_email == contact_email
    assert_primitive_invoked(contact_events, "ContactManager", "update")

    # Secrets: natural-language store -> primitives.secrets.update -> secret.
    secret_name = f"FLOW_SECRET_{token.upper()}"
    secret_value = f"flow-val-{token}"
    async with capture_events("ManagerMethod") as secret_events:
        await flow_session.inject_unify_message(
            f"Store a secret named {secret_name} with the value {secret_value}. "
            "Confirm once it is saved.",
        )
        await flow_session.wait_for_secret_name(secret_name, timeout=240.0)
    assert_primitive_invoked(secret_events, "SecretManager", "update")

    # Knowledge recall: the assistant must answer via KnowledgeManager JSON
    # tools (search/filter/get) rather than guessing or reading another way.
    async with capture_events("ManagerMethod") as knowledge_events:
        await flow_session.inject_unify_message(
            f"What is the launch codename stored in knowledge? "
            "Reply with just the codename.",
        )
        reply = await flow_session.wait_for_unify_reply_containing(
            codename,
            timeout=300.0,
        )
    assert codename in str(reply.content or "")
    observed_methods = {
        (getattr(e, "payload", {}) or {}).get("method")
        for e in knowledge_events
        if (getattr(e, "payload", {}) or {}).get("manager") == "KnowledgeManager"
    }
    assert observed_methods & {
        "search",
        "filter",
        "get_knowledge",
    }, f"Expected KnowledgeManager read tools, saw: {observed_methods}"
