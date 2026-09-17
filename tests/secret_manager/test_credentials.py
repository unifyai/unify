from __future__ import annotations

import pytest
from unify.function_manager.function_manager import VenvPool
from unify.secret_manager.secret_manager import SecretManager


def test_get_credential_returns_the_stored_value(secret_manager_context):
    """Credential use returns the raw value and raises for unknown names."""
    manager = SecretManager()

    manager._create_secret(name="sendgrid", value="sendgrid-value")

    assert manager.get_credential("sendgrid") == "sendgrid-value"

    with pytest.raises(KeyError):
        manager.get_credential("missing_credential")


@pytest.mark.asyncio
async def test_placeholder_resolution_leaves_unknown_names(secret_manager_context):
    """Placeholders resolve to stored values; unknown names stay as written."""
    manager = SecretManager()

    manager._create_secret(name="mail_key", value="mail-value")

    assert await manager.from_placeholder("token=${mail_key}") == "token=mail-value"
    assert await manager.from_placeholder("token=${unknown}") == "token=${unknown}"


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

    manager._create_secret(name="rotating", value="v1")
    manager._update_secret(name="rotating", value="v2")
    manager._delete_secret(name="rotating")

    assert calls == ["invalidate", "invalidate", "invalidate"]


def test_env_sync_failure_does_not_skip_invalidation(
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
