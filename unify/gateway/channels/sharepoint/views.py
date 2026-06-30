"""FastAPI routes for the SharePoint channel.

Ports ``communication/sharepoint/views.py``. Only one translation
needed: ``from communication.helpers import get_graph_client`` ->
``from unify.gateway.common.graph import get_graph_client`` (the
helper was promoted to common in Phase B.4.prep). Everything else
is direct Microsoft Graph SDK usage that needs no rewrite.

Wire behaviour preserved bit-for-bit so the gateway aggregator can
mount this router at ``/sharepoint`` and external callers see no
change.
"""

from __future__ import annotations

import base64
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Response
from msgraph.generated.models.drive_item import DriveItem
from msgraph.generated.models.folder import Folder

from unify.gateway.common.graph import get_graph_client

logger = logging.getLogger("unify.gateway.channels.sharepoint")

router = APIRouter()


# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------


@router.get("/sites")
async def list_sites(user_email: str, search: Optional[str] = None):
    """List SharePoint sites the user has access to."""
    try:
        graph = await get_graph_client(user_email)

        if search:
            sites = await graph.sites.get(
                request_configuration=lambda c: setattr(
                    c.query_parameters,
                    "search",
                    search,
                ),
            )
        else:
            sites = await graph.sites.get()

        return {
            "sites": [
                {
                    "id": site.id,
                    "name": site.display_name,
                    "web_url": site.web_url,
                    "description": site.description,
                }
                for site in (sites.value or [])
            ],
        }
    except Exception as exc:
        logger.error("failed to list sites: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/sites/{site_id}")
async def get_site(user_email: str, site_id: str):
    """Get details of a specific SharePoint site."""
    try:
        graph = await get_graph_client(user_email)
        site = await graph.sites.by_site_id(site_id).get()

        return {
            "id": site.id,
            "name": site.display_name,
            "web_url": site.web_url,
            "description": site.description,
        }
    except Exception as exc:
        logger.error("failed to get site: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Drives (document libraries)
# ---------------------------------------------------------------------------


@router.get("/drives")
async def list_user_drives(
    user_email: Optional[str] = None,
    assistant_id: Optional[str] = None,
):
    """List user's OneDrive and accessible drives."""
    try:
        graph = await get_graph_client(user_email, assistant_id=assistant_id)

        my_drive = await graph.me.drive.get()
        drives = await graph.me.drives.get()

        all_drives = []
        if my_drive:
            all_drives.append(
                {
                    "id": my_drive.id,
                    "name": my_drive.name or "OneDrive",
                    "drive_type": my_drive.drive_type,
                    "web_url": my_drive.web_url,
                    "is_personal": True,
                },
            )

        for drive in drives.value or []:
            if drive.id != my_drive.id:
                all_drives.append(
                    {
                        "id": drive.id,
                        "name": drive.name,
                        "drive_type": drive.drive_type,
                        "web_url": drive.web_url,
                        "is_personal": False,
                    },
                )

        return {"drives": all_drives}
    except Exception as exc:
        logger.error("failed to list drives: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/sites/{site_id}/drives")
async def list_site_drives(user_email: str, site_id: str):
    """List drives (document libraries) in a SharePoint site."""
    try:
        graph = await get_graph_client(user_email)
        drives = await graph.sites.by_site_id(site_id).drives.get()

        return {
            "drives": [
                {
                    "id": drive.id,
                    "name": drive.name,
                    "drive_type": drive.drive_type,
                    "web_url": drive.web_url,
                }
                for drive in (drives.value or [])
            ],
        }
    except Exception as exc:
        logger.error("failed to list site drives: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Files + folders
# ---------------------------------------------------------------------------


def _drive_ref(graph, drive_id: str):
    """Return the Graph drive request builder for ``drive_id`` (or ``me``)."""
    if drive_id == "me":
        return graph.me.drive
    return graph.drives.by_drive_id(drive_id)


@router.get("/drives/{drive_id}/items")
async def list_items(
    drive_id: str,
    path: Optional[str] = None,
    item_id: Optional[str] = None,
    user_email: Optional[str] = None,
    assistant_id: Optional[str] = None,
):
    """List files and folders in a drive (by root, by path, or by item_id)."""
    try:
        graph = await get_graph_client(user_email, assistant_id=assistant_id)
        drive = _drive_ref(graph, drive_id)

        if item_id:
            items = await drive.items.by_drive_item_id(item_id).children.get()
        elif path:
            items = await drive.root.item_with_path(path).children.get()
        else:
            items = await drive.root.children.get()

        return {
            "items": [
                {
                    "id": item.id,
                    "name": item.name,
                    "type": "folder" if item.folder else "file",
                    "size": item.size,
                    "mime_type": item.file.mime_type if item.file else None,
                    "created": (
                        item.created_date_time.isoformat()
                        if item.created_date_time
                        else None
                    ),
                    "modified": (
                        item.last_modified_date_time.isoformat()
                        if item.last_modified_date_time
                        else None
                    ),
                    "web_url": item.web_url,
                }
                for item in (items.value or [])
            ],
        }
    except Exception as exc:
        logger.error("failed to list items: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/drives/{drive_id}/items/{item_id}")
async def get_item(user_email: str, drive_id: str, item_id: str):
    """Get metadata for a specific file or folder."""
    try:
        graph = await get_graph_client(user_email)

        if drive_id == "me":
            item = await graph.me.drive.items.by_drive_item_id(item_id).get()
        else:
            item = (
                await graph.drives.by_drive_id(drive_id)
                .items.by_drive_item_id(item_id)
                .get()
            )

        return {
            "id": item.id,
            "name": item.name,
            "type": "folder" if item.folder else "file",
            "size": item.size,
            "mime_type": item.file.mime_type if item.file else None,
            "created": (
                item.created_date_time.isoformat() if item.created_date_time else None
            ),
            "modified": (
                item.last_modified_date_time.isoformat()
                if item.last_modified_date_time
                else None
            ),
            "web_url": item.web_url,
            "parent_path": (
                item.parent_reference.path if item.parent_reference else None
            ),
        }
    except Exception as exc:
        logger.error("failed to get item: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/drives/{drive_id}/items/{item_id}/content")
async def download_file(user_email: str, drive_id: str, item_id: str):
    """Download a file's content."""
    try:
        graph = await get_graph_client(user_email)
        drive = _drive_ref(graph, drive_id)

        item = await drive.items.by_drive_item_id(item_id).get()
        if item.folder:
            raise HTTPException(status_code=400, detail="Cannot download a folder")

        content = await drive.items.by_drive_item_id(item_id).content.get()

        return Response(
            content=content,
            media_type=(
                item.file.mime_type if item.file else "application/octet-stream"
            ),
            headers={"Content-Disposition": f"attachment; filename={item.name}"},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to download file: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.put("/drives/{drive_id}/upload")
async def upload_file(request: Request, user_email: str, drive_id: str):
    """Upload a file to a drive.

    Request body: ``{"path": "folder/file.txt", "content": "<b64>", ...}``.
    Files > 4MB require chunked upload (not implemented here).
    """
    try:
        data = await request.json()
        path = data.get("path")
        content = data.get("content")

        if not path or content is None:
            raise HTTPException(
                status_code=400,
                detail="Missing required fields: path, content",
            )

        graph = await get_graph_client(user_email)
        drive = _drive_ref(graph, drive_id)

        try:
            file_bytes = base64.b64decode(content)
        except Exception:
            file_bytes = content.encode("utf-8")

        result = await drive.root.item_with_path(path).content.put(file_bytes)

        return {
            "success": True,
            "id": result.id,
            "name": result.name,
            "web_url": result.web_url,
            "size": result.size,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to upload file: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/drives/{drive_id}/folder")
async def create_folder(request: Request, user_email: str, drive_id: str):
    """Create a folder."""
    try:
        data = await request.json()
        folder_name = data.get("name")
        parent_path = data.get("parent_path")

        if not folder_name:
            raise HTTPException(status_code=400, detail="Missing required field: name")

        graph = await get_graph_client(user_email)
        drive = _drive_ref(graph, drive_id)

        new_folder = DriveItem(name=folder_name, folder=Folder())

        if parent_path:
            result = await drive.root.item_with_path(parent_path).children.post(
                new_folder,
            )
        else:
            result = await drive.root.children.post(new_folder)

        return {
            "success": True,
            "id": result.id,
            "name": result.name,
            "web_url": result.web_url,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("failed to create folder: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/drives/{drive_id}/items/{item_id}")
async def delete_item(user_email: str, drive_id: str, item_id: str):
    """Delete a file or folder."""
    try:
        graph = await get_graph_client(user_email)

        if drive_id == "me":
            await graph.me.drive.items.by_drive_item_id(item_id).delete()
        else:
            await (
                graph.drives.by_drive_id(drive_id)
                .items.by_drive_item_id(item_id)
                .delete()
            )

        return {"success": True}
    except Exception as exc:
        logger.error("failed to delete item: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


@router.get("/drives/{drive_id}/search")
async def search_files(user_email: str, drive_id: str, q: str):
    """Search for files in a drive."""
    try:
        graph = await get_graph_client(user_email)

        if drive_id == "me":
            results = await graph.me.drive.root.search_with_q(q).get()
        else:
            results = (
                await graph.drives.by_drive_id(drive_id).root.search_with_q(q).get()
            )

        return {
            "results": [
                {
                    "id": item.id,
                    "name": item.name,
                    "type": "folder" if item.folder else "file",
                    "path": (
                        item.parent_reference.path if item.parent_reference else None
                    ),
                    "web_url": item.web_url,
                }
                for item in (results.value or [])
            ],
        }
    except Exception as exc:
        logger.error("failed to search files: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


__all__ = ["router"]
