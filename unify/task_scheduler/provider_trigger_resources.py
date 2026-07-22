"""List workspace resources for native provider-event ``trigger_config``.

Trusted-runtime helpers used by TaskScheduler ask tools. Calls Google Drive,
Google Chat, and Microsoft Graph with the assistant's workspace OAuth token so
the twin can resolve NL resource mentions into authored config fields before
enable.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx

from unify.common.runtime_oauth import get_provider_access_token

GOOGLE_DRIVE_BASE = "https://www.googleapis.com/drive/v3"
GOOGLE_CHAT_BASE = "https://chat.googleapis.com/v1"
MICROSOFT_GRAPH_BASE = "https://graph.microsoft.com/v1.0"

MY_DRIVE = "my-drive"
_FOLDER_MIME = "application/vnd.google-apps.folder"

FAMILY_GOOGLE_MEET_USER = "google_meet_user"
FAMILY_GOOGLE_DRIVE = "google_drive_resource"
FAMILY_GOOGLE_CHAT = "google_chat_space"
FAMILY_GOOGLE_CHAT_BATCH = "google_chat_batch_delivery"
FAMILY_MS_DELEGATED = "microsoft_graph_delegated"
FAMILY_MS_APP_ONLY = "microsoft_graph_app_only"

_NO_CONFIG_NOTE = (
    "This trigger needs no resource in trigger_config; leave trigger_config empty."
)
_CHAT_BATCH_NOTE = (
    "Chat batch event types are delivery-only. Enable a base Chat space trigger "
    "instead; batch deliveries arrive on that subscription."
)
_MS_APP_ONLY_NOTE = (
    "This Microsoft shape requires app-only / org-admin credentials and is not "
    "live_ready in this milestone. Do not enable it."
)
_DRIVE_BROWSE_HINT = (
    "My Drive is browse-only (Google has no user-level Drive subscription). "
    "Select a folder, file, or shared drive. Pass drive_id + parent_item_id to "
    "list children, or query= to search by name."
)


def list_provider_trigger_resources(
    *,
    target_resource_family: str,
    query: str | None = None,
    drive_id: str | None = None,
    parent_item_id: str | None = None,
    http_get: Callable[..., httpx.Response] | None = None,
) -> dict[str, Any]:
    """Return selectable resources for one native target-resource family.

    ``http_get`` is an injectable transport for deterministic tests; production
    uses the assistant workspace OAuth token against provider REST APIs.
    """

    family = (target_resource_family or "").strip()
    if not family:
        raise ValueError("target_resource_family is required")

    if family == FAMILY_GOOGLE_MEET_USER:
        return _empty_family_result(family, note=_NO_CONFIG_NOTE)
    if family == FAMILY_GOOGLE_CHAT_BATCH:
        return _empty_family_result(family, note=_CHAT_BATCH_NOTE)
    if family == FAMILY_MS_APP_ONLY:
        return _empty_family_result(family, note=_MS_APP_ONLY_NOTE)
    if family == FAMILY_GOOGLE_DRIVE:
        return _list_google_drive_resources(
            query=query,
            drive_id=drive_id,
            parent_item_id=parent_item_id,
            http_get=http_get,
        )
    if family == FAMILY_GOOGLE_CHAT:
        return _list_google_chat_spaces(query=query, http_get=http_get)
    if family == FAMILY_MS_DELEGATED:
        return _list_microsoft_delegated_resources(query=query, http_get=http_get)
    raise ValueError(f"Unsupported target_resource_family {family!r}.")


def drive_resource_from_item(
    *,
    kind: str,
    drive_id: str,
    item_id: str,
    name: str,
) -> dict[str, Any]:
    """Shape one Drive browse hit for actor ``trigger_config`` authoring."""

    if kind == "drive" and drive_id == MY_DRIVE:
        return {
            "name": name,
            "kind": "drive",
            "drive_id": drive_id,
            "item_id": item_id,
            "selectable": False,
            "browse_only": True,
            "trigger_config": None,
        }
    if kind == "drive":
        target = f"drives/{drive_id}"
    else:
        target = f"files/{item_id}"
    return {
        "name": name,
        "kind": kind,
        "drive_id": drive_id,
        "item_id": item_id,
        "selectable": True,
        "browse_only": False,
        "trigger_config": {"target_resource": target},
    }


def chat_space_resource(*, name: str, space_name: str) -> dict[str, Any]:
    """Shape one Chat space for actor ``trigger_config`` authoring."""

    return {
        "name": name,
        "kind": "space",
        "space_name": space_name,
        "selectable": True,
        "browse_only": False,
        "trigger_config": {"target_resource": space_name},
    }


def microsoft_meeting_resource(
    *,
    name: str,
    online_meeting_id: str,
) -> dict[str, Any]:
    """Shape one delegated online meeting for actor ``trigger_config`` authoring."""

    return {
        "name": name,
        "kind": "online_meeting",
        "online_meeting_id": online_meeting_id,
        "selectable": True,
        "browse_only": False,
        "trigger_config": {"online_meeting_id": online_meeting_id},
    }


def _empty_family_result(family: str, *, note: str) -> dict[str, Any]:
    return {
        "target_resource_family": family,
        "resources": [],
        "note": note,
    }


def _http_get(
    url: str,
    *,
    provider: str,
    params: dict[str, Any] | None = None,
    http_get: Callable[..., httpx.Response] | None = None,
    client: httpx.Client | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    if http_get is not None:
        response = http_get(url, params=params)
    else:
        bearer = token or get_provider_access_token(provider)
        headers = {"Authorization": f"Bearer {bearer}"}
        if client is not None:
            response = client.get(url, params=params, headers=headers)
        else:
            with httpx.Client(timeout=30.0) as owned:
                response = owned.get(url, params=params, headers=headers)
    if response.status_code >= 400:
        detail = response.text[:300] if response.text else response.reason_phrase
        raise ValueError(
            f"Provider resource listing failed ({response.status_code}): {detail}",
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Provider resource listing returned a non-object payload.")
    return payload


def _list_google_drive_resources(
    *,
    query: str | None,
    drive_id: str | None,
    parent_item_id: str | None,
    http_get: Callable[..., httpx.Response] | None,
) -> dict[str, Any]:
    cleaned_query = (query or "").strip()
    if cleaned_query:
        resources = _search_google_drive(cleaned_query, http_get=http_get)
        return {
            "target_resource_family": FAMILY_GOOGLE_DRIVE,
            "resources": resources,
            "browse_hint": _DRIVE_BROWSE_HINT,
        }

    if drive_id and parent_item_id:
        resources = _list_google_drive_children(
            drive_id=drive_id,
            parent_item_id=parent_item_id,
            http_get=http_get,
        )
        return {
            "target_resource_family": FAMILY_GOOGLE_DRIVE,
            "resources": resources,
            "browse_hint": _DRIVE_BROWSE_HINT,
        }

    resources = _list_google_drive_roots(http_get=http_get)
    return {
        "target_resource_family": FAMILY_GOOGLE_DRIVE,
        "resources": resources,
        "browse_hint": _DRIVE_BROWSE_HINT,
    }


def _list_google_drive_roots(
    *,
    http_get: Callable[..., httpx.Response] | None,
) -> list[dict[str, Any]]:
    resources = [
        drive_resource_from_item(
            kind="drive",
            drive_id=MY_DRIVE,
            item_id="root",
            name="My Drive",
        ),
    ]
    payload = _http_get(
        f"{GOOGLE_DRIVE_BASE}/drives",
        provider="google",
        params={"pageSize": 100, "fields": "drives(id,name)"},
        http_get=http_get,
    )
    for drive in payload.get("drives") or []:
        if not isinstance(drive, dict):
            continue
        drive_key = str(drive.get("id") or "").strip()
        if not drive_key:
            continue
        resources.append(
            drive_resource_from_item(
                kind="drive",
                drive_id=drive_key,
                item_id=drive_key,
                name=str(drive.get("name") or "Shared drive"),
            ),
        )
    return resources


def _list_kwargs(drive_id: str) -> dict[str, Any]:
    if drive_id == MY_DRIVE:
        return {"spaces": "drive"}
    return {
        "corpora": "drive",
        "driveId": drive_id,
        "includeItemsFromAllDrives": "true",
        "supportsAllDrives": "true",
    }


def _list_google_drive_children(
    *,
    drive_id: str,
    parent_item_id: str,
    http_get: Callable[..., httpx.Response] | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "q": f"'{parent_item_id}' in parents and trashed = false",
        "fields": "files(id,name,mimeType)",
        "pageSize": 100,
        "orderBy": "folder,name",
        **_list_kwargs(drive_id),
    }
    payload = _http_get(
        f"{GOOGLE_DRIVE_BASE}/files",
        provider="google",
        params=params,
        http_get=http_get,
    )
    resources: list[dict[str, Any]] = []
    for item in payload.get("files") or []:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        kind = "folder" if item.get("mimeType") == _FOLDER_MIME else "file"
        resources.append(
            drive_resource_from_item(
                kind=kind,
                drive_id=drive_id,
                item_id=item_id,
                name=str(item.get("name") or item_id),
            ),
        )
    return resources


def _search_google_drive(
    query: str,
    *,
    http_get: Callable[..., httpx.Response] | None,
) -> list[dict[str, Any]]:
    escaped = query.replace("\\", "\\\\").replace("'", "\\'")
    params: dict[str, Any] = {
        "q": f"name contains '{escaped}' and trashed = false",
        "fields": "files(id,name,mimeType,driveId)",
        "pageSize": 50,
        "corpora": "allDrives",
        "includeItemsFromAllDrives": "true",
        "supportsAllDrives": "true",
    }
    payload = _http_get(
        f"{GOOGLE_DRIVE_BASE}/files",
        provider="google",
        params=params,
        http_get=http_get,
    )
    resources: list[dict[str, Any]] = []
    for item in payload.get("files") or []:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        kind = "folder" if item.get("mimeType") == _FOLDER_MIME else "file"
        item_drive = str(item.get("driveId") or MY_DRIVE).strip() or MY_DRIVE
        resources.append(
            drive_resource_from_item(
                kind=kind,
                drive_id=item_drive,
                item_id=item_id,
                name=str(item.get("name") or item_id),
            ),
        )
    return resources


def _list_google_chat_spaces(
    *,
    query: str | None,
    http_get: Callable[..., httpx.Response] | None,
) -> dict[str, Any]:
    payload = _http_get(
        f"{GOOGLE_CHAT_BASE}/spaces",
        provider="google",
        params={"pageSize": 100},
        http_get=http_get,
    )
    cleaned_query = (query or "").strip().lower()
    resources: list[dict[str, Any]] = []
    # Only offer spaces/- when the user did not name a space; a named query
    # should resolve to a concrete space, not a user-level watch.
    if not cleaned_query:
        resources.append(
            chat_space_resource(
                name="All spaces (user-level, where Google permits)",
                space_name="spaces/-",
            ),
        )
    for space in payload.get("spaces") or []:
        if not isinstance(space, dict):
            continue
        space_name = str(space.get("name") or "").strip()
        if not space_name.startswith("spaces/"):
            continue
        display = str(
            space.get("displayName") or space.get("spaceName") or space_name,
        )
        if cleaned_query and cleaned_query not in display.lower():
            continue
        resources.append(chat_space_resource(name=display, space_name=space_name))
    return {
        "target_resource_family": FAMILY_GOOGLE_CHAT,
        "resources": resources,
        "browse_hint": (
            "Prefer a named space when the user named one. Use spaces/- only when "
            "the trigger's schema allows user-level delivery and the user did not "
            "name a specific space."
        ),
    }


_MS_MEETING_RESOLVE_LIMIT = 10


def _list_microsoft_delegated_resources(
    *,
    query: str | None,
    http_get: Callable[..., httpx.Response] | None,
) -> dict[str, Any]:
    """List recent calendar online meetings for meeting-scoped Graph triggers.

    Most delegated Microsoft shapes need no resource config. When the catalog
    schema requires ``online_meeting_id``, resolve meetings from the calendar
    view and map join URLs to Graph onlineMeeting ids.
    """

    cleaned_query = (query or "").strip().lower()
    # Rolling two-week window is enough for twin disambiguation without paging.
    params = {
        "startDateTime": _calendar_window_start(),
        "endDateTime": _calendar_window_end(),
        "$select": "subject,onlineMeeting,onlineMeetingUrl",
        "$orderby": "start/dateTime",
        "$top": "50",
    }

    def _list_with_transport(
        *,
        client: httpx.Client | None = None,
        token: str | None = None,
    ) -> list[dict[str, Any]]:
        payload = _http_get(
            f"{MICROSOFT_GRAPH_BASE}/me/calendarView",
            provider="microsoft",
            params=params,
            http_get=http_get,
            client=client,
            token=token,
        )
        resources: list[dict[str, Any]] = []
        for event in payload.get("value") or []:
            if len(resources) >= _MS_MEETING_RESOLVE_LIMIT:
                break
            if not isinstance(event, dict):
                continue
            subject = str(event.get("subject") or "Untitled meeting")
            if cleaned_query and cleaned_query not in subject.lower():
                continue
            online = event.get("onlineMeeting")
            join_url = None
            if isinstance(online, dict):
                join_url = online.get("joinUrl")
            if not join_url:
                join_url = event.get("onlineMeetingUrl")
            if not isinstance(join_url, str) or not join_url.strip():
                continue
            meeting_id = _resolve_online_meeting_id(
                join_url,
                http_get=http_get,
                client=client,
                token=token,
            )
            if not meeting_id:
                continue
            resources.append(
                microsoft_meeting_resource(name=subject, online_meeting_id=meeting_id),
            )
        return resources

    if http_get is not None:
        resources = _list_with_transport()
    else:
        token = get_provider_access_token("microsoft")
        with httpx.Client(timeout=30.0) as client:
            resources = _list_with_transport(client=client, token=token)

    note = None
    if not resources:
        note = (
            "No online meetings found in the recent calendar window, or this "
            "delegated trigger needs no resource (empty config_schema). If the "
            "schema requires online_meeting_id, ask which meeting and retry with "
            "query=. Delegated Microsoft triggers remain not live_ready until "
            "Graph transport lands."
        )
    return {
        "target_resource_family": FAMILY_MS_DELEGATED,
        "resources": resources,
        "note": note,
        "browse_hint": (
            "Use listed online_meeting_id values only when describe_provider_trigger "
            "requires that field. Empty config_schema means leave trigger_config {}."
        ),
    }


def _resolve_online_meeting_id(
    join_url: str,
    *,
    http_get: Callable[..., httpx.Response] | None,
    client: httpx.Client | None = None,
    token: str | None = None,
) -> str | None:
    filter_expr = f"JoinWebUrl eq '{join_url.replace(chr(39), chr(39) * 2)}'"
    payload = _http_get(
        f"{MICROSOFT_GRAPH_BASE}/me/onlineMeetings",
        provider="microsoft",
        params={"$filter": filter_expr},
        http_get=http_get,
        client=client,
        token=token,
    )
    for meeting in payload.get("value") or []:
        if not isinstance(meeting, dict):
            continue
        meeting_id = str(meeting.get("id") or "").strip()
        if meeting_id:
            return meeting_id
    return None


def _calendar_window_start() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
        "%Y-%m-%dT%H:%M:%SZ",
    )


def _calendar_window_end() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=7)).strftime(
        "%Y-%m-%dT%H:%M:%SZ",
    )
