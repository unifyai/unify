"""Contract tests for mirrored task row field vocabulary."""

from __future__ import annotations

import json
from pathlib import Path

from unify.task_scheduler.types.task_row_field import (
    AuthoredTaskField,
    RuntimeTaskField,
    RuntimeTaskStatus,
)

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "task_trigger_contract"
    / "task_row_field_contract.v1.json"
)


def test_authored_task_field_matches_shared_fixture() -> None:
    fixture = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    assert sorted(AuthoredTaskField.values()) == sorted(fixture["authored_fields"])


def test_runtime_task_field_matches_shared_fixture() -> None:
    fixture = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    assert sorted(RuntimeTaskField.values()) == sorted(fixture["runtime_fields"])


def test_runtime_task_status_matches_shared_fixture() -> None:
    fixture = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    assert sorted(RuntimeTaskStatus.values()) == sorted(
        fixture["runtime_status_values"],
    )
