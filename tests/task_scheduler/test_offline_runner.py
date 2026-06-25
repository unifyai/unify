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
        offline_runner.unity,
        "ensure_initialised",
        lambda *, project_name: None,
    )


def test_offline_runner_rejects_non_scheduler_source_type(monkeypatch):
    """The offline task runner should not keep a direct function execution lane."""

    from unity.task_scheduler import offline_runner

    _seed_env(monkeypatch)
    updates = []
    _stub_runtime_initialization(monkeypatch, offline_runner)
    monkeypatch.setattr(
        offline_runner,
        "_update_task_run",
        lambda assistant_id, run_key, payload: updates.append(
            (assistant_id, run_key, payload),
        ),
    )

    exit_code = offline_runner.main()

    assert exit_code == 1
    assert updates[0][0] == "42"
    assert updates[0][2]["state"] == "failed"
    assert "scheduler-managed" in updates[0][2]["result_summary"]


def test_offline_runner_initializes_before_scheduler_delegate_execution(monkeypatch):
    """Scheduler-owned offline tasks should initialize Unity before execution."""

    from unity.common.task_execution_context import current_task_execution_delegate
    from unity.task_scheduler import offline_runner

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
            assert events[:2] == ["populate_from_env", "ensure_initialised"]
            assert current_task_execution_delegate.get() is not None
            return _FakeHandle()

    monkeypatch.setattr(
        offline_runner.SESSION_DETAILS,
        "populate_from_env",
        lambda: events.append("populate_from_env"),
    )
    monkeypatch.setattr(
        offline_runner.unity,
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
        "ensure_initialised",
        "scheduler.execute",
        "handle.result",
    ]
    assert current_task_execution_delegate.get() is None


def test_offline_delegate_runs_agentic_task_through_actor(monkeypatch):
    """Agentic offline tasks should use the shared actor substrate."""

    from unity.task_scheduler import offline_runner

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

    monkeypatch.setattr(offline_runner, "_build_offline_actor", _FakeActor)
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

    from unity.task_scheduler import offline_runner

    _seed_env(monkeypatch)
    monkeypatch.setenv("TASK_DESTINATION", "team:007")

    config = offline_runner._load_config_from_env()

    assert config.destination == "team:7"


def test_load_config_from_env_rejects_invalid_destination(monkeypatch):
    """Offline runner fails fast on invalid destination labels."""

    from unity.task_scheduler import offline_runner

    _seed_env(monkeypatch)
    monkeypatch.setenv("TASK_DESTINATION", "org_default")

    with pytest.raises(RuntimeError, match="Invalid TASK_DESTINATION"):
        offline_runner._load_config_from_env()
