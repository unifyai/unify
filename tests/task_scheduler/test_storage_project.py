from types import SimpleNamespace

from unify.task_scheduler.machine_state import TASK_MACHINE_STATE_PROJECT
from unify.task_scheduler.storage import TasksStore
from unify.task_scheduler.types.trigger import CommunicationTrigger


def test_tasks_store_get_rows_passes_explicit_project_override(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_get_logs(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("unify.task_scheduler.storage.unisdk.get_logs", _fake_get_logs)

    store = TasksStore("Tasks/Executions", project=TASK_MACHINE_STATE_PROJECT)
    rows = store.get_rows(limit=5)

    assert rows == []
    assert captured["project"] == TASK_MACHINE_STATE_PROJECT
    assert captured["context"] == "Tasks/Executions"


def test_tasks_store_defaults_to_active_project(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_get_logs(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("unify.task_scheduler.storage.unisdk.get_logs", _fake_get_logs)
    monkeypatch.setattr(
        "unify.task_scheduler.storage.unisdk.active_project",
        lambda: "Assistants",
    )

    store = TasksStore("42/7/Tasks")
    store.get_rows(limit=5)

    assert captured["project"] == "Assistants"


def test_tasks_store_log_passes_explicit_project_to_helper(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_log(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(id=1)

    monkeypatch.setattr("unify.task_scheduler.storage.unity_log", _fake_log)

    store = TasksStore("42/7/Tasks", project=TASK_MACHINE_STATE_PROJECT)
    result = store.log(entries={"task_id": 101})

    assert result.id == 1
    assert captured["project"] == TASK_MACHINE_STATE_PROJECT
    assert captured["context"] == "42/7/Tasks"


def test_tasks_store_log_keeps_trigger_as_dict_with_explicit_type(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_log(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(id=1, entries=kwargs)

    monkeypatch.setattr("unify.task_scheduler.storage.unity_log", _fake_log)

    store = TasksStore("42/7/Tasks", project=TASK_MACHINE_STATE_PROJECT)
    store.log(
        entries={
            "name": "Triggered",
            "description": "Has a communication trigger",
            "trigger": CommunicationTrigger(medium="sms_message"),
        },
    )

    assert isinstance(captured["trigger"], dict)
    assert captured["trigger"]["kind"] == "communication"
    assert captured["trigger"]["medium"] == "sms_message"
    assert captured["explicit_types"]["trigger"]["type"] == "dict"
