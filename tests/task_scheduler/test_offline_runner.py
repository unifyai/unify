"""Tests for the headless offline task runner."""

import pytest


def _seed_env(monkeypatch, *, source_type="dashboard_action"):
    monkeypatch.setenv("ASSISTANT_ID", "42")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_RUN_KEY", "offline:scheduled:42:101:rev")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_ID", "101")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_FUNCTION_ID", "777")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_REQUEST", "Send the daily summary email.")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_SOURCE_TYPE", source_type)
    monkeypatch.setenv("UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID", "555")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_ACTIVATION_REVISION", "rev-123")
    monkeypatch.setenv("ORCHESTRA_URL", "https://orchestra.test")
    monkeypatch.setenv("ORCHESTRA_ADMIN_KEY", "admin-key")


def _stub_runtime_initialization(monkeypatch, offline_runner):
    monkeypatch.setattr(
        offline_runner.SESSION_DETAILS,
        "populate_from_env",
        lambda: None,
    )
    monkeypatch.setattr(
        offline_runner,
        "_ensure_offline_client_bundle",
        lambda: None,
    )
    monkeypatch.setattr(
        offline_runner.unify,
        "ensure_initialised",
        lambda *, project_name: None,
    )


def test_offline_runner_normalizes_unknown_source_type_to_explicit(monkeypatch):
    """Unknown source types normalize to explicit and enter the scheduler lane."""

    from unify.task_scheduler import offline_runner

    _seed_env(monkeypatch)
    updates = []
    _stub_runtime_initialization(monkeypatch, offline_runner)
    monkeypatch.setattr(
        offline_runner,
        "_update_task_run",
        lambda assistant_id, run_key, **kwargs: updates.append(
            (assistant_id, run_key, kwargs.get("updates") or kwargs.get("payload")),
        ),
    )
    monkeypatch.setattr(
        offline_runner,
        "_mark_source_task_failed",
        lambda *args, **kwargs: None,
    )

    config = offline_runner._load_config_from_env()
    assert config.source_type == offline_runner.RunSource.explicit

    exit_code = offline_runner.main()

    assert exit_code == 1
    assert updates[0][0] == "42"
    assert updates[0][2]["state"] == "failed"


def test_offline_runner_initializes_before_scheduler_delegate_execution(monkeypatch):
    """Scheduler-owned offline tasks should initialize Unity before execution."""

    from unify.common.task_execution_context import current_task_execution_delegate
    from unify.task_scheduler import offline_runner

    _seed_env(monkeypatch, source_type="scheduled")
    events = []
    updates = []

    class _FakeHandle:
        async def result(self):
            events.append("handle.result")
            return "scheduler-owned result"

    class _FakeScheduler:
        async def execute(
            self,
            *,
            task_id,
            trigger_attempt_token,
            _activated_by,
        ):
            events.append("scheduler.execute")
            assert task_id == 101
            assert trigger_attempt_token is None
            assert str(_activated_by) == "schedule"
            assert events[:3] == [
                "populate_from_env",
                "ensure_client_bundle",
                "ensure_initialised",
            ]
            assert current_task_execution_delegate.get() is not None
            return _FakeHandle()

    monkeypatch.setattr(
        offline_runner.SESSION_DETAILS,
        "populate_from_env",
        lambda: events.append("populate_from_env"),
    )
    monkeypatch.setattr(
        offline_runner,
        "_ensure_offline_client_bundle",
        lambda: events.append("ensure_client_bundle"),
    )
    monkeypatch.setattr(
        offline_runner.unify,
        "ensure_initialised",
        lambda *, project_name: events.append("ensure_initialised"),
    )
    monkeypatch.setattr(
        offline_runner,
        "TaskScheduler",
        lambda: _FakeScheduler(),
    )
    monkeypatch.setattr(
        offline_runner,
        "_update_task_run",
        lambda assistant_id, run_key, payload: updates.append(
            (assistant_id, run_key, payload),
        ),
    )

    exit_code = offline_runner.main()

    assert exit_code == 0
    assert updates == []
    assert events == [
        "populate_from_env",
        "ensure_client_bundle",
        "ensure_initialised",
        "scheduler.execute",
        "handle.result",
    ]
    assert current_task_execution_delegate.get() is None


def test_offline_runner_explicit_uses_activated_by_explicit(monkeypatch):
    """REST-triggered offline runs keep explicit provenance (no trigger token)."""

    from unify.common.task_execution_context import current_task_execution_delegate
    from unify.task_scheduler import offline_runner
    from unify.task_scheduler.types.activated_by import ActivatedBy

    _seed_env(monkeypatch, source_type="explicit")
    monkeypatch.setenv(
        "UNITY_OFFLINE_TASK_RUN_KEY",
        "offline:explicit:42:101:rev:abc123",
    )
    monkeypatch.setenv("UNITY_OFFLINE_TASK_SOURCE_REF", "req-rest-1")
    captured = {}

    class _FakeHandle:
        async def result(self):
            return "explicit offline result"

    class _FakeScheduler:
        async def execute(
            self,
            *,
            task_id,
            trigger_attempt_token,
            _activated_by,
        ):
            captured["task_id"] = task_id
            captured["trigger_attempt_token"] = trigger_attempt_token
            captured["activated_by"] = _activated_by
            assert current_task_execution_delegate.get() is not None
            return _FakeHandle()

    _stub_runtime_initialization(monkeypatch, offline_runner)
    monkeypatch.setattr(offline_runner, "TaskScheduler", lambda: _FakeScheduler())
    monkeypatch.setattr(
        offline_runner,
        "_update_task_run",
        lambda *args, **kwargs: None,
    )

    exit_code = offline_runner.main()

    assert exit_code == 0
    assert captured["task_id"] == 101
    assert captured["trigger_attempt_token"] is None
    assert captured["activated_by"] == ActivatedBy.explicit
    provenance = offline_runner._build_offline_provenance(
        offline_runner._load_config_from_env(),
    )
    assert provenance.source_type == "explicit"
    assert provenance.attempt_token is None
    assert provenance.source_ref == "req-rest-1"


def test_offline_delegate_runs_agentic_task_through_actor(monkeypatch):
    """Agentic offline tasks should use the shared actor substrate."""

    from unify.task_scheduler import offline_runner

    captured = {}

    class _FakeHandle:
        async def result(self):
            return "agentic offline result"

    class _FakeActor:
        async def act(self, request, **kwargs):
            captured["request"] = request
            captured["kwargs"] = kwargs
            return _FakeHandle()

        async def close(self):
            captured["closed"] = True

    monkeypatch.setattr(
        offline_runner,
        "_build_offline_actor",
        lambda _config: _FakeActor(),
    )
    config = offline_runner.OfflineTaskConfig(
        assistant_id="42",
        run_key="offline:scheduled:42:101:rev:once",
        task_id=101,
        function_id=None,
        request="Send the agentic offline summary.",
        source_type="scheduled",
        source_task_log_id=555,
        activation_revision="rev-123",
        scheduled_for="2026-04-10T09:00:00+00:00",
    )

    async def _run():
        delegate = offline_runner._OfflineTaskExecutionDelegate(config)
        try:
            handle = await delegate.start_task_run(
                task_description="Send the agentic offline summary.",
                entrypoint=None,
            )
            assert "agentic offline result" in await handle.result()
        finally:
            await delegate.close()

    import asyncio

    asyncio.run(_run())
    assert captured["request"] == "Send the agentic offline summary."
    assert captured["kwargs"]["entrypoint"] is None
    assert captured["kwargs"]["entrypoint_kwargs"] is None
    assert captured["kwargs"]["clarification_enabled"] is False
    assert captured["kwargs"]["persist"] is False
    assert captured["closed"] is True


def test_load_config_from_env_canonicalizes_destination(monkeypatch):
    """Offline env destination labels are normalized before execution."""

    from unify.task_scheduler import offline_runner

    _seed_env(monkeypatch)
    monkeypatch.setenv("TASK_DESTINATION", "team:007")

    config = offline_runner._load_config_from_env()

    assert config.destination == "team:7"


def test_load_config_from_env_reads_resource_flags(monkeypatch):
    """Resource knobs load from UNITY_OFFLINE_TASK_REQUIRES_* env vars."""

    from unify.task_scheduler import offline_runner

    _seed_env(monkeypatch, source_type="scheduled")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM", "1")
    monkeypatch.setenv("UNITY_OFFLINE_TASK_REQUIRES_COMPUTER", "true")

    config = offline_runner._load_config_from_env()

    assert config.requires_filesystem is True
    assert config.requires_computer is True


def test_execute_offline_task_requires_desktop_env_when_resources_needed(monkeypatch):
    """Missing desktop injection fails loudly when resources are required."""

    from unify.task_scheduler import offline_runner

    config = offline_runner.OfflineTaskConfig(
        assistant_id="42",
        run_key="offline:scheduled:42:101:rev:once",
        task_id=101,
        function_id=None,
        request="Touch Local files.",
        source_type="scheduled",
        source_task_log_id=555,
        activation_revision="rev-123",
        requires_filesystem=True,
    )

    async def _run():
        with pytest.raises(RuntimeError, match="ASSISTANT_DESKTOP_URL"):
            await offline_runner._execute_offline_task(config)

    import asyncio

    asyncio.run(_run())


def test_build_offline_actor_omits_computer_unless_required(monkeypatch):
    """ComputerEnvironment is only mounted when requires_computer is True."""

    from unify.actor.environments import ComputerEnvironment
    from unify.task_scheduler import offline_runner

    captured: list[list] = []

    class _FakeActor:
        def __init__(self, environments=None, **kwargs):
            captured.append(list(environments or []))

    monkeypatch.setattr(offline_runner, "CodeActActor", _FakeActor)

    offline_runner._build_offline_actor(
        offline_runner.OfflineTaskConfig(
            assistant_id="42",
            run_key="offline:scheduled:42:101:rev:once",
            task_id=101,
            function_id=None,
            request="No computer.",
            source_type="scheduled",
            source_task_log_id=555,
            activation_revision="rev-123",
            requires_computer=False,
        ),
    )
    offline_runner._build_offline_actor(
        offline_runner.OfflineTaskConfig(
            assistant_id="42",
            run_key="offline:scheduled:42:101:rev:once",
            task_id=101,
            function_id=None,
            request="Use the desktop.",
            source_type="scheduled",
            source_task_log_id=555,
            activation_revision="rev-123",
            requires_computer=True,
        ),
    )

    assert not any(isinstance(env, ComputerEnvironment) for env in captured[0])
    assert any(isinstance(env, ComputerEnvironment) for env in captured[1])
