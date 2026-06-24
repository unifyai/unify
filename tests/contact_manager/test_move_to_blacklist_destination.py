from __future__ import annotations

import uuid
import time

import unify

from tests.helpers import _handle_project
from unity.common.context_registry import ContextRegistry
from unity.contact_manager.contact_manager import ContactManager
from unity.conversation_manager.cm_types import Medium
from unity.manager_registry import ManagerRegistry
from unity.session_details import SESSION_DETAILS


def _configure_space() -> int:
    team_id = 50_000_000 + uuid.uuid4().int % 1_000_000_000
    SESSION_DETAILS.team_ids = [team_id]
    SESSION_DETAILS.team_summaries = [
        {
            "team_id": team_id,
            "name": "Shared Support",
            "description": "Shared support workspace with vendor contacts.",
        },
    ]
    ContextRegistry.clear()
    ManagerRegistry.clear()
    return team_id


def _reset_space(team_id: int) -> None:
    for context in (
        f"Teams/{team_id}/Contacts",
        f"Teams/{team_id}/BlackList",
    ):
        try:
            unify.delete_context(context)
        except Exception:
            pass
    SESSION_DETAILS.team_ids = []
    SESSION_DETAILS.team_summaries = []
    ContextRegistry.clear()
    ManagerRegistry.clear()


@_handle_project
def test_move_to_blacklist_preserves_shared_team_destination():
    team_id = _configure_space()
    email = f"blocked.vendor.{uuid.uuid4().hex}@example.com"
    manager = ContactManager()

    try:
        created = manager._create_contact(
            first_name="Blocked",
            surname="Vendor",
            email_address=email,
            destination=f"team:{team_id}",
        )
        contact_id = created["details"]["contact_id"]

        manager._move_to_blacklist(
            contact_id=contact_id,
            reason="spam",
            destination=f"team:{team_id}",
        )

        contact_rows = []
        for _ in range(10):
            contact_rows = unify.get_logs(
                context=f"Teams/{team_id}/Contacts",
                filter=f"contact_id == {contact_id}",
            )
            if not contact_rows:
                break
            time.sleep(0.2)
        blacklist_rows = unify.get_logs(
            context=f"Teams/{team_id}/BlackList",
            filter=f"contact_detail == '{email}'",
        )

        assert contact_rows == []
        assert len(blacklist_rows) == 1
        assert blacklist_rows[0].entries["medium"] == Medium.EMAIL.value
    finally:
        _reset_space(team_id)
