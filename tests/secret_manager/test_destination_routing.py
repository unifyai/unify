from __future__ import annotations

import pytest
from unify import db
from unify.common.tool_outcome import ToolError
from unify.function_manager.function_manager import VenvPool
from unify.secret_manager.secret_manager import SecretManager


def _rows(context: str) -> list[dict]:
    return [log.entries for log in db.get_logs(context=context)]


def test_secret_writes_land_in_the_personal_vault(secret_manager_context):
    """Credential writes land in the personal vault whichever spelling names it."""
    manager = SecretManager()

    manager._create_secret(
        name="implicit_api",
        value="implicit-value",
        description="Private API key",
    )
    manager._create_secret(
        name="explicit_api",
        value="explicit-value",
        description="Private API key filed explicitly",
        destination="personal",
    )

    assert {row["name"]: row["value"] for row in _rows(manager._ctx)} == {
        "implicit_api": "implicit-value",
        "explicit_api": "explicit-value",
    }

    rows = manager._filter_secrets()
    assert {(row.name, row.destination) for row in rows} == {
        ("implicit_api", "personal"),
        ("explicit_api", "personal"),
    }
    assert set(manager._list_secret_keys()) == {"implicit_api", "explicit_api"}

    personal_only = manager._filter_secrets(
        filter="name == 'explicit_api' and destination == 'personal'",
    )
    assert [(row.name, row.destination) for row in personal_only] == [
        ("explicit_api", "personal"),
    ]


def test_create_secret_invalid_destination_returns_tool_error(
    secret_manager_context,
):
    """A destination other than the personal vault returns a structured tool error."""
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
):
    """Update and delete operations reject destinations other than the personal vault."""
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


def test_get_credential_reads_the_personal_vault(secret_manager_context):
    """Credential use resolves the personal vault and raises for unknown names."""
    manager = SecretManager()

    manager._create_secret(name="sendgrid", value="personal-sendgrid")

    assert manager.get_credential("sendgrid") == "personal-sendgrid"
    assert (
        manager.get_credential("sendgrid", destination="personal")
        == "personal-sendgrid"
    )

    with pytest.raises(KeyError):
        manager.get_credential("missing_credential")


@pytest.mark.asyncio
async def test_placeholder_resolution_reads_the_personal_vault(
    secret_manager_context,
):
    """Placeholders resolve from the personal vault, with either destination spelling."""
    manager = SecretManager()

    manager._create_secret(name="mail_key", value="personal-mail")

    assert await manager.from_placeholder("token=${mail_key}") == "token=personal-mail"
    assert (
        await manager.from_placeholder("token=${mail_key}", destination="personal")
        == "token=personal-mail"
    )


def test_credential_writes_invalidate_pooled_subprocesses(
    monkeypatch,
    secret_manager_context,
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

    manager = SecretManager()

    error: ToolError = manager._create_secret(
        name="bad_destination",
        value="nope",
        destination="team:987699",
    )  # type: ignore[assignment]
    assert error["error_kind"] == "invalid_destination"
    assert calls == []

    manager._create_secret(name="rotating", value="v1")
    manager._update_secret(name="rotating", value="v2")
    manager._delete_secret(name="rotating")

    assert calls == ["invalidate", "invalidate", "invalidate"]


def test_personal_env_sync_failure_does_not_skip_invalidation(
    monkeypatch,
    secret_manager_context,
):
    """A credential write still invalidates processes if .env sync fails."""
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
