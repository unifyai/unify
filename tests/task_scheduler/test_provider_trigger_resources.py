"""Pure-helper tests for native provider-trigger resource listing."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from unify.task_scheduler.provider_trigger_resources import (
    FAMILY_GOOGLE_CHAT,
    FAMILY_GOOGLE_CHAT_BATCH,
    FAMILY_GOOGLE_DRIVE,
    FAMILY_GOOGLE_MEET_USER,
    FAMILY_MS_APP_ONLY,
    FAMILY_MS_DELEGATED,
    MY_DRIVE,
    list_provider_trigger_resources,
)


def _response(payload: dict[str, Any], *, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        content=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://example.test/"),
    )


def test_list_provider_trigger_resources_returns_notes_for_no_config_families() -> None:
    for family in (
        FAMILY_GOOGLE_MEET_USER,
        FAMILY_GOOGLE_CHAT_BATCH,
        FAMILY_MS_APP_ONLY,
    ):
        result = list_provider_trigger_resources(target_resource_family=family)
        assert result["target_resource_family"] == family
        assert result["resources"] == []
        assert isinstance(result.get("note"), str) and result["note"]


def test_list_provider_trigger_resources_lists_drive_roots_browse_and_search() -> None:
    def http_get(url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        params = params or {}
        if url.endswith("/drives"):
            return _response({"drives": [{"id": "shared1", "name": "Team Drive"}]})
        if url.endswith("/files") and "name contains" in str(params.get("q", "")):
            return _response(
                {
                    "files": [
                        {
                            "id": "folder99",
                            "name": "Budget",
                            "mimeType": "application/vnd.google-apps.folder",
                            "driveId": "shared1",
                        },
                    ],
                },
            )
        if url.endswith("/files"):
            assert params.get("q") == "'root' in parents and trashed = false"
            return _response(
                {
                    "files": [
                        {
                            "id": "file1",
                            "name": "Notes",
                            "mimeType": "application/vnd.google-apps.document",
                        },
                        {
                            "id": "folder1",
                            "name": "Projects",
                            "mimeType": "application/vnd.google-apps.folder",
                        },
                    ],
                },
            )
        raise AssertionError(f"unexpected url {url} params={params}")

    roots = list_provider_trigger_resources(
        target_resource_family=FAMILY_GOOGLE_DRIVE,
        http_get=http_get,
    )
    by_name = {item["name"]: item for item in roots["resources"]}
    assert by_name["My Drive"]["selectable"] is False
    assert by_name["My Drive"]["browse_only"] is True
    assert by_name["My Drive"]["drive_id"] == MY_DRIVE
    assert by_name["My Drive"]["trigger_config"] is None
    assert by_name["Team Drive"]["selectable"] is True
    assert by_name["Team Drive"]["trigger_config"] == {
        "target_resource": "drives/shared1",
    }

    children = list_provider_trigger_resources(
        target_resource_family=FAMILY_GOOGLE_DRIVE,
        drive_id=MY_DRIVE,
        parent_item_id="root",
        http_get=http_get,
    )
    child_by_name = {item["name"]: item for item in children["resources"]}
    assert child_by_name["Notes"]["trigger_config"] == {
        "target_resource": "files/file1",
    }
    assert child_by_name["Projects"]["kind"] == "folder"
    assert child_by_name["Projects"]["trigger_config"] == {
        "target_resource": "files/folder1",
    }

    search = list_provider_trigger_resources(
        target_resource_family=FAMILY_GOOGLE_DRIVE,
        query="Budget",
        http_get=http_get,
    )
    assert len(search["resources"]) == 1
    assert search["resources"][0]["trigger_config"] == {
        "target_resource": "files/folder99",
    }
    assert search["resources"][0]["drive_id"] == "shared1"


def test_list_provider_trigger_resources_lists_chat_spaces_with_spaces_dash_fallback() -> (
    None
):
    def http_get(url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        assert url.endswith("/spaces")
        return _response(
            {
                "spaces": [
                    {"name": "spaces/AAAA", "displayName": "Engineering"},
                    {"name": "spaces/BBBB", "displayName": "Sales"},
                ],
            },
        )

    all_spaces = list_provider_trigger_resources(
        target_resource_family=FAMILY_GOOGLE_CHAT,
        http_get=http_get,
    )
    assert all_spaces["resources"][0]["trigger_config"] == {
        "target_resource": "spaces/-",
    }
    names = {item["name"] for item in all_spaces["resources"]}
    assert {"Engineering", "Sales"} <= names

    filtered = list_provider_trigger_resources(
        target_resource_family=FAMILY_GOOGLE_CHAT,
        query="sales",
        http_get=http_get,
    )
    assert [item["trigger_config"]["target_resource"] for item in filtered["resources"]] == [
        "spaces/BBBB",
    ]
    assert all(
        item["trigger_config"]["target_resource"] != "spaces/-"
        for item in filtered["resources"]
    )


def test_list_provider_trigger_resources_lists_delegated_microsoft_meetings_by_online_meeting_id() -> (
    None
):
    def http_get(url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        params = params or {}
        if url.endswith("/me/calendarView"):
            return _response(
                {
                    "value": [
                        {
                            "subject": "Weekly sync",
                            "onlineMeeting": {
                                "joinUrl": "https://teams.microsoft.com/l/meetup-join/abc",
                            },
                        },
                        {
                            "subject": "No meeting link",
                        },
                    ],
                },
            )
        if url.endswith("/me/onlineMeetings"):
            assert "JoinWebUrl eq" in str(params.get("$filter", ""))
            return _response({"value": [{"id": "meeting-123"}]})
        raise AssertionError(f"unexpected url {url}")

    result = list_provider_trigger_resources(
        target_resource_family=FAMILY_MS_DELEGATED,
        query="weekly",
        http_get=http_get,
    )
    assert len(result["resources"]) == 1
    assert result["resources"][0]["trigger_config"] == {
        "online_meeting_id": "meeting-123",
    }
    assert "target_resource" not in result["resources"][0]["trigger_config"]


def test_list_provider_trigger_resources_rejects_unknown_family() -> None:
    with pytest.raises(ValueError, match="Unsupported target_resource_family"):
        list_provider_trigger_resources(target_resource_family="not_a_family")
