"""One flow exercising the core state managers through the live brain.

Folds the contact and knowledge coverage together with secret storage so a
single smoke test confirms the assistant can read and write the main
assistant-owned state surfaces (contacts, knowledge, secrets) against real
Orchestra, then recall that state in a grounded reply.
"""

from __future__ import annotations

import uuid

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_core_state_managers_persist_and_recall(
    flow_session: FlowHarness,
) -> None:
    """contacts + knowledge + secrets: write via the brain, then recall."""

    token = uuid.uuid4().hex[:8]

    # Knowledge: seed a table the assistant must ground its later answer on.
    codename = f"orbit-{token}"
    table_name = f"FlowFacts{token}"
    flow_session.seed_knowledge_table(
        table_name=table_name,
        rows=[{"content": f"The launch codename is {codename}."}],
    )

    # Contacts: natural-language create -> primitives.contacts -> persisted row.
    contact_first = f"Elena{token}"
    contact_email = f"elena.{token}@example.com"
    await flow_session.inject_unify_message(
        f"Add a new contact named {contact_first} Flow with email {contact_email}.",
    )
    contact = await flow_session.wait_for_contact_email(contact_email, timeout=240.0)
    persisted_email = (
        contact.get("email_address")
        if isinstance(contact, dict)
        else contact.email_address
    )
    assert persisted_email == contact_email

    # Secrets: natural-language store -> primitives.secrets -> persisted secret.
    secret_name = f"FLOW_SECRET_{token.upper()}"
    await flow_session.inject_unify_message(
        f"Store a secret named {secret_name} with the value sk-flow-{token}. "
        "Confirm once it is saved.",
    )
    await flow_session.wait_for_secret_name(secret_name, timeout=240.0)

    # Knowledge recall: the assistant must answer from the seeded table.
    await flow_session.inject_unify_message(
        f"What is the launch codename in knowledge table {table_name}? "
        "Reply with just the codename.",
    )
    reply = await flow_session.wait_for_unify_reply_containing(codename, timeout=300.0)
    assert codename in str(reply.content or "")
