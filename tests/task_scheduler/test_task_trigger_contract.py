"""Authored task trigger union contract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from unify.task_scheduler.types.trigger import (
    CommunicationTrigger,
    ProviderEventTrigger,
    parse_task_trigger,
)

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def _load_fixture(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_parse_task_trigger_coerces_legacy_medium_only_row() -> None:
    trigger = parse_task_trigger(
        _load_fixture("task_trigger.communication.legacy.json"),
    )
    assert isinstance(trigger, CommunicationTrigger)
    assert trigger.kind == "communication"
    assert trigger.medium.value == "email"
    assert trigger.from_contact_ids == [17]
    assert trigger.recurring is True


def test_parse_task_trigger_accepts_json_string_payload() -> None:
    payload = _load_fixture("task_trigger.communication.v1.json")
    trigger = parse_task_trigger(json.dumps(payload))
    assert isinstance(trigger, CommunicationTrigger)
    assert trigger.kind == "communication"
    assert trigger.medium.value == "email"


def test_task_model_hydrates_trigger_from_json_string() -> None:
    from unify.task_scheduler.types.task import TaskBase
    from unify.task_scheduler.types.status import Status
    from unify.task_scheduler.types.priority import Priority

    payload = _load_fixture("task_trigger.communication.v1.json")
    task = TaskBase(
        name="Invoice follow-up",
        description="Draft a reply when an invoice email arrives.",
        status=Status.triggerable,
        priority=Priority.normal,
        trigger=json.dumps(payload),
    )
    assert isinstance(task.trigger, CommunicationTrigger)
    assert task.trigger.medium.value == "email"


def test_parse_task_trigger_accepts_provider_event_kind() -> None:
    trigger = parse_task_trigger(_load_fixture("task_trigger.provider_event.v1.json"))
    assert isinstance(trigger, ProviderEventTrigger)
    assert trigger.state == "enabled"
    assert trigger.provider_trigger_slug == "GITHUB_ISSUE_CREATED_TRIGGER"
    assert trigger.trigger_config["owner"] == "YushaArif99"
    assert trigger.trigger_config["repo"] == "triggers-test-repo"


def test_parse_task_trigger_rejects_unknown_kind() -> None:
    payload = _load_fixture("task_trigger.communication.v1.json")
    payload["kind"] = "unknown"
    with pytest.raises(Exception):
        parse_task_trigger(payload)
