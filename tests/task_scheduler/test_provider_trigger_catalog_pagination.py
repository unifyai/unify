"""Pure unit tests for provider-trigger catalog filter/pagination forwarding."""

from __future__ import annotations

from typing import Any

import pytest

from unify.session_details import SESSION_DETAILS
from unify.task_scheduler import typed_tasks_client
from unify.task_scheduler.task_scheduler import TaskScheduler


def test_get_trigger_catalog_forwards_optional_params_as_query_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SESSION_DETAILS.assistant, "agent_id", 42)
    captured: dict[str, Any] = {}

    def fake_request(method: str, path: str, **kwargs: Any) -> Any:
        captured["method"] = method
        captured["path"] = path
        captured["params"] = kwargs.get("params")
        return {"info": {"available": True, "triggers": []}}

    monkeypatch.setattr(typed_tasks_client, "_request", fake_request)
    monkeypatch.setattr(typed_tasks_client, "_info", lambda response: response["info"])

    result = typed_tasks_client.get_trigger_catalog(
        canonical_app_slug="google_calendar",
        limit=10,
        offset=20,
    )

    assert captured["method"] == "get"
    assert captured["path"] == "/assistants/42/provider-triggers"
    assert captured["params"] == {
        "canonical_app_slug": "google_calendar",
        "limit": 10,
        "offset": 20,
    }
    assert result == {"available": True, "triggers": []}


def test_get_trigger_catalog_omits_unset_optional_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SESSION_DETAILS.assistant, "agent_id", 7)
    captured: dict[str, Any] = {}

    def fake_request(method: str, path: str, **kwargs: Any) -> Any:
        captured["params"] = kwargs.get("params")
        return {"info": {"available": True, "triggers": []}}

    monkeypatch.setattr(typed_tasks_client, "_request", fake_request)
    monkeypatch.setattr(typed_tasks_client, "_info", lambda response: response["info"])

    typed_tasks_client.get_trigger_catalog()

    assert captured["params"] == {}


def test_list_provider_trigger_catalog_tool_forwards_params_to_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_get_trigger_catalog(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"available": True, "triggers": []}

    monkeypatch.setattr(
        typed_tasks_client,
        "get_trigger_catalog",
        fake_get_trigger_catalog,
    )

    # `_list_provider_trigger_catalog` never touches `self`, so the unbound
    # method can be exercised without constructing a full TaskScheduler.
    outcome = TaskScheduler._list_provider_trigger_catalog(
        object(),
        canonical_app_slug="github",
        limit=5,
        offset=15,
    )

    assert captured == {
        "canonical_app_slug": "github",
        "limit": 5,
        "offset": 15,
    }
    assert outcome["outcome"] == "provider trigger catalog listed"
    assert outcome["details"]["available"] is True


def test_list_provider_trigger_catalog_tool_defaults_forward_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_get_trigger_catalog(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"available": True, "triggers": []}

    monkeypatch.setattr(
        typed_tasks_client,
        "get_trigger_catalog",
        fake_get_trigger_catalog,
    )

    TaskScheduler._list_provider_trigger_catalog(object())

    assert captured == {
        "canonical_app_slug": None,
        "limit": None,
        "offset": None,
    }
