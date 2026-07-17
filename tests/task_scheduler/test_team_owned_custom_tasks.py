"""Team-owned assistants must sync custom tasks onto the owning-team Tasks tree."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unify.session_details import SESSION_DETAILS
from unify.task_scheduler.task_scheduler import TaskScheduler


@pytest.fixture
def team_owned_session():
    original_owner = SESSION_DETAILS.owner_team_id
    original_agent = SESSION_DETAILS.assistant.agent_id
    SESSION_DETAILS.owner_team_id = 11
    SESSION_DETAILS.assistant.agent_id = 1406
    try:
        yield
    finally:
        SESSION_DETAILS.owner_team_id = original_owner
        SESSION_DETAILS.assistant.agent_id = original_agent


def test_task_context_personal_destination_uses_owner_team(team_owned_session):
    scheduler = TaskScheduler.__new__(TaskScheduler)
    with patch(
        "unify.task_scheduler.task_scheduler.ContextRegistry.write_root",
        return_value="Teams/11",
    ) as write_root:
        ctx = scheduler._task_context_for_destination("personal")
    assert ctx == "Teams/11/Tasks"
    write_root.assert_called_once()
    assert write_root.call_args.kwargs["destination"] == "personal"


def test_sync_custom_tasks_refuses_non_team_context_when_team_owned(
    team_owned_session,
    monkeypatch,
):
    scheduler = TaskScheduler.__new__(TaskScheduler)
    monkeypatch.setattr(
        scheduler,
        "_sync_destination_contexts",
        lambda destination: (
            "cli3t/1406/Tasks",
            "cli3t/1406/Tasks/Meta",
            True,
        ),
    )
    with pytest.raises(RuntimeError, match="Refusing custom-tasks sync"):
        scheduler.sync_custom_tasks(source_tasks={})


def test_sync_custom_tasks_allows_personal_label_when_resolved_to_team(
    team_owned_session,
    monkeypatch,
):
    scheduler = TaskScheduler.__new__(TaskScheduler)
    monkeypatch.setattr(
        scheduler,
        "_sync_destination_contexts",
        lambda destination: (
            "Teams/11/Tasks",
            "Teams/11/Tasks/Meta",
            True,
        ),
    )

    class _MetaCtx:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(
        scheduler,
        "_temporary_tasks_meta_context",
        lambda ctx: _MetaCtx(),
    )
    monkeypatch.setattr(scheduler, "_get_stored_custom_tasks_hash", lambda: "same")
    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.compute_custom_tasks_hash",
        lambda **_: "same",
    )
    scheduler._custom_tasks_synced = False
    scheduler._custom_tasks_synced_contexts = set()
    scheduler._ctx = "Teams/11/Tasks"
    scheduler._store = MagicMock()
    scheduler._root_stores = {}
    scheduler._active_task_root_context = None

    # Must not raise: destination label 'personal' is fine when resolved under Teams/.
    assert scheduler.sync_custom_tasks(source_tasks={}, destination=None) is False
