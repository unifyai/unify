from __future__ import annotations

import pytest
import unify

from droid.common.tool_outcome import ToolError
from droid.function_manager.function_manager import VenvPool
from droid.secret_manager.secret_manager import SecretManager


def _rows(context: str) -> list[dict]:
    return [log.entries for log in unify.get_logs(context=context)]


def test_secret_writes_route_to_destination_and_reads_merge_roots(
    secret_manager_context,
    secret_manager_teams,
):
    """Credential writes land in one vault while read tools see every reachable vault."""
    first_team, second_team = secret_manager_teams
    manager = SecretManager()

    manager._create_secret(
        name="shared_api",
        value="personal-value",
        description="Private API key",
    )
    manager._create_secret(
        name="shared_api",
        value="team-one-value",
        description="Patch team service account",
        destination=f"team:{first_team}",
    )
    manager._create_secret(
        name="family_calendar",
        value="team-two-value",
        description="Family calendar shared key",
        destination=f"team:{second_team}",
    )

    personal_rows = _rows(manager._ctx)
    first_team_rows = _rows(f"Teams/{first_team}/Secrets")
    second_team_rows = _rows(f"Teams/{second_team}/Secrets")

    assert [row["value"] for row in personal_rows] == ["personal-value"]
    assert [row["value"] for row in first_team_rows] == ["team-one-value"]
    assert [row["value"] for row in second_team_rows] == ["team-two-value"]

    merged_rows = manager._filter_secrets()
    assert [row.name for row in merged_rows].count("shared_api") == 2
    assert {row.name for row in merged_rows} == {"shared_api", "family_calendar"}
    assert {(row.name, row.destination) for row in merged_rows} == {
        ("shared_api", "personal"),
        ("shared_api", f"team:{first_team}"),
        ("family_calendar", f"team:{second_team}"),
    }
    assert set(manager._list_secret_keys()) == {"shared_api", "family_calendar"}

    shared_only = manager._filter_secrets(
        filter=f"name == 'shared_api' and destination == 'team:{first_team}'",
    )
    assert [(row.name, row.destination) for row in shared_only] == [
        ("shared_api", f"team:{first_team}"),
    ]


def test_create_secret_invalid_destination_returns_tool_error(
    secret_manager_context,
    secret_manager_teams,
):
    """Invalid shared-team destinations return a structured tool error."""
    manager = SecretManager()

    outcome = manager._create_secret(
        name="bad_destination",
        value="nope",
        destination="team:987699",
    )

    assert isinstance(outcome, dict)
    assert outcome["error_kind"] == "invalid_destination"
    assert outcome["details"]["destination"] == "team:987699"


def test_mutating_secret_invalid_destination_returns_tool_error(
    secret_manager_context,
    secret_manager_teams,
):
    """Update and delete operations reject inaccessible shared-team destinations."""
    manager = SecretManager()

    update_outcome = manager._update_secret(
        name="missing",
        value="ignored",
        destination="team:987699",
    )
    delete_outcome = manager._delete_secret(
        name="missing",
        destination="team:987699",
    )

    assert update_outcome["error_kind"] == "invalid_destination"
    assert delete_outcome["error_kind"] == "invalid_destination"
    assert update_outcome["details"]["destination"] == "team:987699"
    assert delete_outcome["details"]["destination"] == "team:987699"


def test_get_credential_reads_exact_destination_root(
    secret_manager_context,
    secret_manager_teams,
):
    """Credential use reads one vault and never falls back across scopes."""
    team_id, _ = secret_manager_teams
    manager = SecretManager()

    manager._create_secret(name="sendgrid", value="personal-sendgrid")
    manager._create_secret(
        name="sendgrid",
        value="team-sendgrid",
        destination=f"team:{team_id}",
    )
    manager._create_secret(
        name="space_only",
        value="team-only",
        destination=f"team:{team_id}",
    )

    assert manager.get_credential("sendgrid") == "personal-sendgrid"
    assert (
        manager.get_credential("sendgrid", destination=f"team:{team_id}")
        == "team-sendgrid"
    )

    with pytest.raises(KeyError):
        manager.get_credential("space_only")


@pytest.mark.asyncio
async def test_placeholder_resolution_inherits_task_destination(
    monkeypatch,
    secret_manager_context,
    secret_manager_teams,
):
    """Shared task execution resolves placeholders from the task's destination vault."""
    team_id, _ = secret_manager_teams
    manager = SecretManager()

    manager._create_secret(name="mail_key", value="personal-mail")
    manager._create_secret(
        name="mail_key",
        value="team-mail",
        destination=f"team:{team_id}",
    )

    monkeypatch.setenv("TASK_DESTINATION", f"team:{team_id}")

    assert await manager.from_placeholder("token=${mail_key}") == "token=team-mail"


@pytest.mark.asyncio
async def test_placeholder_resolution_rejects_invalid_task_destination(
    monkeypatch,
    secret_manager_context,
):
    """Invalid TASK_DESTINATION values fail closed during placeholder resolution."""
    manager = SecretManager()
    monkeypatch.setenv("TASK_DESTINATION", "org_default")

    with pytest.raises(ValueError, match="Destination must be"):
        await manager.from_placeholder("token=${mail_key}")


def test_credential_writes_invalidate_pooled_subprocesses(
    monkeypatch,
    secret_manager_context,
    secret_manager_teams,
):
    """Every successful credential mutation invalidates stateful execution pools."""
    calls: list[str] = []

    def record_invalidation(cls) -> int:
        calls.append("invalidate")
        return 0

    monkeypatch.setattr(
        VenvPool,
        "invalidate_all_pools",
        classmethod(record_invalidation),
    )

    team_id, _ = secret_manager_teams
    manager = SecretManager()

    error: ToolError = manager._create_secret(
        name="bad_destination",
        value="nope",
        destination="team:987699",
    )  # type: ignore[assignment]
    assert error["error_kind"] == "invalid_destination"
    assert calls == []

    manager._create_secret(name="rotating", value="v1", destination=f"team:{team_id}")
    manager._update_secret(name="rotating", value="v2", destination=f"team:{team_id}")
    manager._delete_secret(name="rotating", destination=f"team:{team_id}")

    assert calls == ["invalidate", "invalidate", "invalidate"]


def test_personal_env_sync_failure_does_not_skip_invalidation(
    monkeypatch,
    secret_manager_context,
):
    """A backend credential write still invalidates processes if .env sync fails."""
    calls: list[str] = []

    def record_invalidation(cls) -> int:
        calls.append("invalidate")
        return 0

    monkeypatch.setattr(
        VenvPool,
        "invalidate_all_pools",
        classmethod(record_invalidation),
    )

    manager = SecretManager()
    monkeypatch.setattr(
        manager,
        "_env_set",
        lambda name, value: (_ for _ in ()).throw(OSError("disk unavailable")),
    )

    result = manager._create_secret(name="env_failure", value="stored")

    assert result["outcome"] == "secret created"
    assert calls == ["invalidate"]
    assert manager.get_credential("env_failure") == "stored"
