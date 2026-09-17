"""Tests for broader-context assembly."""

from types import SimpleNamespace

from unify.memory_manager import broader_context
from unify.session_details import SESSION_DETAILS


class _ContactManager:
    def filter_contacts(self, *, filter: str, limit: int) -> dict:
        contacts = [
            SimpleNamespace(
                contact_id=0,
                first_name="Ava",
                surname="Assistant",
                bio="Assistant bio.",
                job_title="Operations",
            ),
            SimpleNamespace(
                contact_id=1,
                first_name="Boss",
                surname="User",
                bio="Boss bio.",
            ),
        ]
        if filter == "contact_id == 1":
            contacts = [contacts[1]]
        return {"contacts": contacts[:limit]}


def test_broader_context_assembles_bios_and_activity_then_caches(monkeypatch):
    """The broader context introduces the assistant, then the user, then the
    activity log, and is cached until explicitly reset."""

    from unify.memory_manager.memory_manager import MemoryManager

    activity = {"text": "Recent work."}
    monkeypatch.setattr(
        MemoryManager,
        "get_rolling_activity",
        lambda: activity["text"],
    )
    SESSION_DETAILS.reset()
    broader_context.reset()

    first = broader_context.get_broader_context(contact_manager=_ContactManager())
    assert first.startswith("You are a personal assistant named Ava Assistant.")
    assert "Your main role / specialization is: Operations." in first
    assert "You work directly for Boss User." in first
    assert first.index("A bit about yourself") < first.index("A bit about Boss User")
    assert first.index("A bit about Boss User") < first.index("# Activity Logs")
    assert first.endswith("# Activity Logs\nRecent work.")

    activity["text"] = "Newer work."
    assert (
        broader_context.get_broader_context(contact_manager=_ContactManager()) == first
    )

    broader_context.reset()
    refreshed = broader_context.get_broader_context(contact_manager=_ContactManager())
    assert refreshed.endswith("# Activity Logs\nNewer work.")

    broader_context.set_broader_context("pinned")
    assert broader_context.get_broader_context(contact_manager=_ContactManager()) == (
        "pinned"
    )
    broader_context.reset()
    SESSION_DETAILS.reset()
