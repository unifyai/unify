"""FastAPI routes for the Google Drive browse channel.

Parallel to ``droid/gateway/channels/sharepoint/views.py`` but for Google
Drive. Exposes read-only enumeration (roots / children / item / search) so the
Console picker can render an unfiltered Drive tree for the workspace
file-access allowlist, and so the assistant runtime can resolve item ancestry
when enforcing that allowlist.

Credentials come from the connected BYOD account: the assistant's
``GOOGLE_ACCESS_TOKEN`` secret, resolved by email via the shared Orchestra
lookup (mirrors ``get_gmail_service_async``). All routes are admin-authed at
the aggregator (see ``droid/gateway/app.py``).

The unified item identity used across providers is ``(drive_id, item_id)``.
For Google we model the personal corpus as the sentinel drive ``"my-drive"``
with root folder id ``"root"``; each shared drive is its own ``drive_id`` whose
root folder id equals the drive id.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient.discovery import build

from droid.gateway.common.orchestra import lookup_assistant
from droid.gateway.credentials import EnvCredentialStore

logger = logging.getLogger("droid.gateway.channels.drive")

router = APIRouter()

MY_DRIVE = "my-drive"
_FOLDER_MIME = "application/vnd.google-apps.folder"
_FILE_FIELDS = "id,name,mimeType,parents,modifiedTime,size,webViewLink"
_LIST_FIELDS = f"nextPageToken,files({_FILE_FIELDS})"


async def _get_drive_service(user_email: str) -> Any:
    """Build a Drive v3 client from the connected account's BYOD token."""
    assistant = await lookup_assistant(user_email, EnvCredentialStore())
    access_token = (assistant.get("secrets") or {}).get("GOOGLE_ACCESS_TOKEN")
    if not access_token:
        raise HTTPException(
            status_code=409,
            detail="No connected Google account for this assistant.",
        )
    return build("drive", "v3", credentials=OAuthCredentials(token=access_token))


def _item_dict(file: dict[str, Any], drive_id: str) -> dict[str, Any]:
    parents = file.get("parents") or []
    return {
        "drive_id": drive_id,
        "item_id": file.get("id"),
        "name": file.get("name"),
        "kind": "folder" if file.get("mimeType") == _FOLDER_MIME else "file",
        "mime_type": file.get("mimeType"),
        "size": file.get("size"),
        "modified": file.get("modifiedTime"),
        "web_url": file.get("webViewLink"),
        "parent_id": parents[0] if parents else None,
    }


def _list_kwargs(drive_id: str) -> dict[str, Any]:
    """Drive ``files.list`` kwargs scoped to the personal or a shared drive."""
    if drive_id == MY_DRIVE:
        return {"spaces": "drive"}
    return {
        "corpora": "drive",
        "driveId": drive_id,
        "includeItemsFromAllDrives": True,
        "supportsAllDrives": True,
    }


# ---------------------------------------------------------------------------
# Roots: My Drive + shared drives
# ---------------------------------------------------------------------------


@router.get("/roots")
async def list_roots(user_email: str):
    """List the top-level corpora: personal My Drive plus shared drives."""
    service = await _get_drive_service(user_email)

    def _fetch() -> list[dict[str, Any]]:
        roots: list[dict[str, Any]] = [
            {
                "drive_id": MY_DRIVE,
                "item_id": "root",
                "name": "My Drive",
                "kind": "drive",
            },
        ]
        shared = service.drives().list(pageSize=100, fields="drives(id,name)").execute()
        for drive in shared.get("drives", []):
            roots.append(
                {
                    "drive_id": drive["id"],
                    "item_id": drive["id"],
                    "name": drive.get("name") or "Shared drive",
                    "kind": "drive",
                },
            )
        return roots

    try:
        return {"roots": await asyncio.to_thread(_fetch)}
    except Exception as exc:
        logger.error("failed to list drive roots: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Children of a folder (lazy)
# ---------------------------------------------------------------------------


@router.get("/children")
async def list_children(user_email: str, drive_id: str, item_id: str):
    """List the immediate children of folder *item_id* within *drive_id*."""
    service = await _get_drive_service(user_email)

    def _fetch() -> list[dict[str, Any]]:
        query = f"'{item_id}' in parents and trashed = false"
        items: list[dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = (
                service.files()
                .list(
                    q=query,
                    fields=_LIST_FIELDS,
                    pageSize=200,
                    orderBy="folder,name",
                    pageToken=page_token,
                    **_list_kwargs(drive_id),
                )
                .execute()
            )
            items.extend(_item_dict(f, drive_id) for f in resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return items

    try:
        return {"items": await asyncio.to_thread(_fetch)}
    except Exception as exc:
        logger.error("failed to list drive children: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Single item metadata (with parents, for ancestry resolution)
# ---------------------------------------------------------------------------


@router.get("/item")
async def get_item(user_email: str, drive_id: str, item_id: str):
    """Return metadata (including ``parents``) for a single Drive item."""
    service = await _get_drive_service(user_email)

    def _fetch() -> dict[str, Any]:
        file = (
            service.files()
            .get(fileId=item_id, fields=_FILE_FIELDS, supportsAllDrives=True)
            .execute()
        )
        return _item_dict(file, drive_id)

    try:
        return await asyncio.to_thread(_fetch)
    except Exception as exc:
        logger.error("failed to get drive item: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


@router.get("/search")
async def search_files(user_email: str, drive_id: str, q: str):
    """Search items by name within a drive."""
    service = await _get_drive_service(user_email)
    escaped = q.replace("'", "\\'")

    def _fetch() -> list[dict[str, Any]]:
        query = f"name contains '{escaped}' and trashed = false"
        resp = (
            service.files()
            .list(
                q=query,
                fields=_LIST_FIELDS,
                pageSize=100,
                **_list_kwargs(drive_id),
            )
            .execute()
        )
        return [_item_dict(f, drive_id) for f in resp.get("files", [])]

    try:
        return {"results": await asyncio.to_thread(_fetch)}
    except Exception as exc:
        logger.error("failed to search drive: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


__all__ = ["router"]
