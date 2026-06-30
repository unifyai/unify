"""WorkspaceFilesManager: the single enforced door to connected workspace files.

Exposed to the actor as ``primitives.workspace_files.*``. Every Drive /
SharePoint / OneDrive read flows through here and is filtered against the
per-assistant allowlist (see ``policy``), so any disallowed file or folder is
masked out of listings/search and reports as not-found on direct access.

The connector runs in the trusted runtime (not the restricted ``execute_code``
sandbox) and holds the file-scoped OAuth token via ``get_oauth_access_token``.
The sandbox is denied raw Drive/Graph access, so this manager is the only path
by which the assistant can reach workspace files.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

from unify.common.runtime_oauth import get_oauth_access_token
from unify.workspace_files.policy import get_policy_store

logger = logging.getLogger(__name__)

_FOLDER_MIME = "application/vnd.google-apps.folder"
_GOOGLE_BASE = "https://www.googleapis.com/drive/v3"
_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_MY_DRIVE = "my-drive"
_MAX_ANCESTRY_DEPTH = 40


class WorkspaceFileNotFound(Exception):
    """Raised when an item is absent or masked by the allowlist.

    Masked items deliberately surface as "not found" so the assistant cannot
    distinguish a disallowed item from a non-existent one.
    """


class WorkspaceFilesManager:
    """Allowlist-enforced access to a connected Google/Microsoft account's files."""

    # Discovered by ToolSurfaceRegistry via this constant (no Base* class).
    _PRIMITIVE_METHODS = (
        "list_roots",
        "list_children",
        "get_item",
        "search",
        "read_file",
    )

    def __init__(self) -> None:
        self._ancestry_cache: dict[tuple[str, str, str], list[tuple[str, str]]] = {}

    # ── Provider / token resolution ──────────────────────────────────────

    @staticmethod
    def _secret(name: str) -> Optional[str]:
        from unify.manager_registry import ManagerRegistry

        sm = ManagerRegistry.get_secret_manager()
        getter = getattr(sm, "_get_secret_value", None)
        if callable(getter):
            value = getter(name)
            if isinstance(value, str) and value:
                return value
        return os.environ.get(name) or None

    def _provider(self) -> str:
        """Detect the connected provider from stored OAuth grants."""
        if self._secret("GOOGLE_GRANTED_SCOPES") or self._secret("GOOGLE_ACCESS_TOKEN"):
            return "google"
        if self._secret("MICROSOFT_GRANTED_SCOPES") or self._secret(
            "MICROSOFT_ACCESS_TOKEN",
        ):
            return "microsoft"
        raise WorkspaceFileNotFound(
            "No connected Google or Microsoft account is available.",
        )

    def _headers(self, provider: str) -> dict[str, str]:
        token = get_oauth_access_token(provider)
        return {"Authorization": f"Bearer {token}"}

    # ── Low-level provider calls (unfiltered) ────────────────────────────

    async def _google_children(
        self,
        drive_id: str,
        item_id: str,
    ) -> list[dict[str, Any]]:
        params = {
            "q": f"'{item_id}' in parents and trashed = false",
            "fields": "nextPageToken,files(id,name,mimeType,parents,webViewLink)",
            "pageSize": "200",
            "orderBy": "folder,name",
        }
        if drive_id and drive_id != _MY_DRIVE:
            params.update(
                {
                    "corpora": "drive",
                    "driveId": drive_id,
                    "includeItemsFromAllDrives": "true",
                    "supportsAllDrives": "true",
                },
            )
        out: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30) as http:
            page_token: Optional[str] = None
            while True:
                if page_token:
                    params["pageToken"] = page_token
                resp = await http.get(
                    f"{_GOOGLE_BASE}/files",
                    params=params,
                    headers=self._headers("google"),
                )
                resp.raise_for_status()
                data = resp.json()
                out.extend(
                    self._google_node(f, drive_id) for f in data.get("files", [])
                )
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
        return out

    async def _google_get(self, drive_id: str, item_id: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(
                f"{_GOOGLE_BASE}/files/{item_id}",
                params={
                    "fields": "id,name,mimeType,parents,webViewLink",
                    "supportsAllDrives": "true",
                },
                headers=self._headers("google"),
            )
        if resp.status_code == 404:
            raise WorkspaceFileNotFound(item_id)
        resp.raise_for_status()
        return self._google_node(resp.json(), drive_id)

    async def _google_search(self, drive_id: str, query: str) -> list[dict[str, Any]]:
        escaped = query.replace("'", "\\'")
        params = {
            "q": f"name contains '{escaped}' and trashed = false",
            "fields": "files(id,name,mimeType,parents,webViewLink)",
            "pageSize": "100",
        }
        if drive_id and drive_id != _MY_DRIVE:
            params.update(
                {
                    "corpora": "drive",
                    "driveId": drive_id,
                    "includeItemsFromAllDrives": "true",
                    "supportsAllDrives": "true",
                },
            )
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(
                f"{_GOOGLE_BASE}/files",
                params=params,
                headers=self._headers("google"),
            )
        resp.raise_for_status()
        return [self._google_node(f, drive_id) for f in resp.json().get("files", [])]

    async def _google_content(self, item_id: str) -> bytes:
        async with httpx.AsyncClient(timeout=60) as http:
            resp = await http.get(
                f"{_GOOGLE_BASE}/files/{item_id}",
                params={"alt": "media", "supportsAllDrives": "true"},
                headers=self._headers("google"),
            )
        if resp.status_code == 404:
            raise WorkspaceFileNotFound(item_id)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _google_node(raw: dict[str, Any], drive_id: str) -> dict[str, Any]:
        parents = raw.get("parents") or []
        return {
            "drive_id": drive_id,
            "item_id": raw.get("id"),
            "name": raw.get("name"),
            "kind": "folder" if raw.get("mimeType") == _FOLDER_MIME else "file",
            "mime_type": raw.get("mimeType"),
            "web_url": raw.get("webViewLink"),
            "parent_id": parents[0] if parents else None,
        }

    async def _ms_children(self, drive_id: str, item_id: str) -> list[dict[str, Any]]:
        if not item_id or item_id == "root":
            path = f"{_GRAPH_BASE}/drives/{drive_id}/root/children"
        else:
            path = f"{_GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(path, headers=self._headers("microsoft"))
        resp.raise_for_status()
        return [self._ms_node(n, drive_id) for n in resp.json().get("value", [])]

    async def _ms_get(self, drive_id: str, item_id: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(
                f"{_GRAPH_BASE}/drives/{drive_id}/items/{item_id}",
                params={"$select": "id,name,folder,file,parentReference,webUrl"},
                headers=self._headers("microsoft"),
            )
        if resp.status_code == 404:
            raise WorkspaceFileNotFound(item_id)
        resp.raise_for_status()
        return self._ms_node(resp.json(), drive_id)

    async def _ms_search(self, drive_id: str, query: str) -> list[dict[str, Any]]:
        escaped = query.replace("'", "''")
        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(
                f"{_GRAPH_BASE}/drives/{drive_id}/root/search(q='{escaped}')",
                headers=self._headers("microsoft"),
            )
        resp.raise_for_status()
        return [self._ms_node(n, drive_id) for n in resp.json().get("value", [])]

    async def _ms_content(self, drive_id: str, item_id: str) -> bytes:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
            resp = await http.get(
                f"{_GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content",
                headers=self._headers("microsoft"),
            )
        if resp.status_code == 404:
            raise WorkspaceFileNotFound(item_id)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _ms_node(raw: dict[str, Any], drive_id: str) -> dict[str, Any]:
        parent_ref = raw.get("parentReference") or {}
        return {
            "drive_id": drive_id,
            "item_id": raw.get("id"),
            "name": raw.get("name"),
            "kind": "folder" if raw.get("folder") else "file",
            "mime_type": (raw.get("file") or {}).get("mimeType"),
            "web_url": raw.get("webUrl"),
            "parent_id": parent_ref.get("id"),
        }

    # ── Ancestry + enforcement ───────────────────────────────────────────

    async def _ancestry_chain(
        self,
        provider: str,
        drive_id: str,
        item_id: str,
    ) -> list[tuple[str, str]]:
        """Return ``(drive_id, id)`` tuples from *item_id* outward to the root."""
        cache_key = (provider, drive_id, item_id)
        if cache_key in self._ancestry_cache:
            return self._ancestry_cache[cache_key]

        chain: list[tuple[str, str]] = []
        current = item_id
        seen: set[str] = set()
        for _ in range(_MAX_ANCESTRY_DEPTH):
            if not current or current in seen:
                break
            seen.add(current)
            chain.append((drive_id, current))
            try:
                node = (
                    await self._google_get(drive_id, current)
                    if provider == "google"
                    else await self._ms_get(drive_id, current)
                )
            except WorkspaceFileNotFound:
                break
            parent = node.get("parent_id")
            if not parent or parent == current:
                break
            current = parent
        self._ancestry_cache[cache_key] = chain
        return chain

    async def _is_allowed(self, provider: str, drive_id: str, item_id: str) -> bool:
        policy = get_policy_store().get(provider)
        if policy is None:
            # No configured allowlist => unrestricted (opt-in restriction).
            return True
        chain = await self._ancestry_chain(provider, drive_id, item_id)
        return policy.allows(chain)

    # ── Public primitives ────────────────────────────────────────────────

    async def list_roots(self) -> list[dict[str, Any]]:
        """List the connected account's top-level drives / corpora.

        Returns one entry per browsable root (Google: My Drive plus shared
        drives; Microsoft: OneDrive plus accessible document libraries). Each
        entry has ``drive_id``, ``item_id`` (the root folder id), ``name``, and
        ``kind == "drive"``. Use the returned ids with ``list_children``.
        """
        provider = self._provider()
        if provider == "google":
            roots = [
                {
                    "drive_id": _MY_DRIVE,
                    "item_id": "root",
                    "name": "My Drive",
                    "kind": "drive",
                },
            ]
            async with httpx.AsyncClient(timeout=30) as http:
                resp = await http.get(
                    f"{_GOOGLE_BASE}/drives",
                    params={"pageSize": "100", "fields": "drives(id,name)"},
                    headers=self._headers("google"),
                )
            resp.raise_for_status()
            for d in resp.json().get("drives", []):
                roots.append(
                    {
                        "drive_id": d["id"],
                        "item_id": d["id"],
                        "name": d.get("name") or "Shared drive",
                        "kind": "drive",
                    },
                )
            return roots

        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.get(
                f"{_GRAPH_BASE}/me/drives",
                headers=self._headers("microsoft"),
            )
        resp.raise_for_status()
        return [
            {
                "drive_id": d["id"],
                "item_id": "root",
                "name": d.get("name") or "Drive",
                "kind": "drive",
            }
            for d in resp.json().get("value", [])
        ]

    async def list_children(self, drive_id: str, item_id: str) -> list[dict[str, Any]]:
        """List the accessible children of a folder.

        Disallowed items are masked out entirely. A folder you are allowed to
        access exposes all its descendants (subject to any deeper explicit
        deny), so newly-added files inside an allowed folder are visible.

        Parameters
        ----------
        drive_id : str
            Drive/corpus id from ``list_roots`` (or a child's ``drive_id``).
        item_id : str
            Folder id whose children to list (``"root"`` for a drive root).
        """
        provider = self._provider()
        children = (
            await self._google_children(drive_id, item_id)
            if provider == "google"
            else await self._ms_children(drive_id, item_id)
        )

        policy = get_policy_store().get(provider)
        if policy is None:
            return children

        parent_allowed = await self._is_allowed(provider, drive_id, item_id)
        visible: list[dict[str, Any]] = []
        for child in children:
            explicit = policy.decision_for(drive_id, child.get("item_id") or "")
            allowed = explicit if explicit is not None else parent_allowed
            if allowed:
                visible.append(child)
        return visible

    async def get_item(self, drive_id: str, item_id: str) -> dict[str, Any]:
        """Return metadata for a single item, or raise if it is masked/absent.

        Parameters
        ----------
        drive_id : str
            Drive/corpus id.
        item_id : str
            Item id to fetch.
        """
        provider = self._provider()
        if not await self._is_allowed(provider, drive_id, item_id):
            raise WorkspaceFileNotFound(item_id)
        if provider == "google":
            return await self._google_get(drive_id, item_id)
        return await self._ms_get(drive_id, item_id)

    async def search(
        self,
        query: str,
        drive_id: str = _MY_DRIVE,
    ) -> list[dict[str, Any]]:
        """Search items by name within a drive, returning only accessible hits.

        Parameters
        ----------
        query : str
            Substring to match against item names.
        drive_id : str
            Drive/corpus id to search within (defaults to the personal drive).
        """
        provider = self._provider()
        results = (
            await self._google_search(drive_id, query)
            if provider == "google"
            else await self._ms_search(drive_id, query)
        )
        policy = get_policy_store().get(provider)
        if policy is None:
            return results
        visible: list[dict[str, Any]] = []
        for hit in results:
            if await self._is_allowed(provider, drive_id, hit.get("item_id") or ""):
                visible.append(hit)
        return visible

    async def read_file(self, drive_id: str, item_id: str) -> bytes:
        """Download a file's raw bytes, or raise if it is masked/absent.

        Parameters
        ----------
        drive_id : str
            Drive/corpus id.
        item_id : str
            File id to download.
        """
        provider = self._provider()
        if not await self._is_allowed(provider, drive_id, item_id):
            raise WorkspaceFileNotFound(item_id)
        if provider == "google":
            return await self._google_content(item_id)
        return await self._ms_content(drive_id, item_id)
