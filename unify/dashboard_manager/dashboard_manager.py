"""
Concrete DashboardManager implementation.

Delegates tile/dashboard CRUD to DataManager for Unify context operations
and to token_ops for Orchestra token registration. Stays thin.

Docstrings are inherited from BaseDashboardManager via @functools.wraps.
"""

from __future__ import annotations

import functools
import logging
from contextlib import contextmanager
from threading import RLock
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import unisdk
from pydantic import TypeAdapter

from unify.common.context_registry import (
    ContextRegistry,
    INVALID_DESTINATION_ERROR,
    TEAM_DESTINATION_PREFIX,
    TableContext,
)
from unify.common.log_utils import create_logs as unity_create_logs
from unify.common.model_to_fields import model_to_fields
from unify.common.tool_outcome import ToolErrorException
from unify.dashboard_manager.base import DASHBOARD_DATA_SCOPE, BaseDashboardManager
from unify.dashboard_manager.custom_dashboards import (
    LAYOUTS_NAMESPACE,
    TILES_NAMESPACE,
    compute_custom_dashboards_hash,
)
from unify.dashboard_manager.types.meta import DashboardMeta
from unify.dashboard_manager.ops.dashboard_ops import (
    build_dashboard_record_row,
    deserialize_layout,
    serialize_layout,
)
from unify.dashboard_manager.ops.action_ops import (
    delete_tile_actions,
    replace_tile_actions,
    validate_tile_actions,
)
from unify.dashboard_manager.ops.tile_ops import (
    _contexts_for_binding,
    build_tile_record_row,
    ensure_binding_aliases,
    resolve_binding_contexts,
    serialize_bindings,
    validate_data_bindings,
    validate_on_data,
    verify_data_bindings,
)
from unify.dashboard_manager.ops.token_ops import (
    delete_token,
    generate_token,
    register_token,
)
from unify.dashboard_manager.types.dashboard import (
    DashboardRecord,
    DashboardRecordRow,
    DashboardResult,
    TilePosition,
)
from unify.dashboard_manager.types.action import ActionRecordRow, TileAction
from unify.dashboard_manager.types.tile import (
    DataBinding,
    TileRecord,
    TileRecordRow,
    TileResult,
)
from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

logger = logging.getLogger(__name__)

TILES_TABLE = "Dashboards/Tiles"
ACTIONS_TABLE = "Dashboards/Actions"
LAYOUTS_TABLE = "Dashboards/Layouts"
DASHBOARDS_META_TABLE = "Dashboards/Meta"


def _get_active_project() -> str:
    """Get the currently active Unify project name."""
    try:
        import unisdk

        project = unisdk.active_project()
        return project or ""
    except Exception:
        return ""


def _build_tile_url(token: str) -> str:
    console_url = getattr(SETTINGS, "CONSOLE_URL", "https://console.unify.ai")
    if isinstance(console_url, str):
        console_url = console_url.rstrip("/")
    else:
        console_url = "https://console.unify.ai"
    return f"{console_url}/tile/view/{token}"


def _build_dashboard_url(token: str) -> str:
    console_url = getattr(SETTINGS, "CONSOLE_URL", "https://console.unify.ai")
    if isinstance(console_url, str):
        console_url = console_url.rstrip("/")
    else:
        console_url = "https://console.unify.ai"
    return f"{console_url}/dashboard/view/{token}"


def _require_dashboard_context(context: Optional[str], suffix: str) -> str:
    if not context:
        raise RuntimeError(f"Dashboard context {suffix!r} could not be resolved")
    expected = f"Dashboards/{suffix}"
    if context == expected or "/" not in context:
        raise RuntimeError(
            f"Dashboard context {suffix!r} is not fully qualified: {context}",
        )
    if not context.endswith(f"/{expected}"):
        raise RuntimeError(
            f"Dashboard context {suffix!r} resolved outside Dashboards namespace: "
            f"{context}",
        )
    if context.startswith("Data/Dashboards/") or "/Data/Dashboards/" in context:
        raise RuntimeError(
            f"Dashboard context {suffix!r} resolved under Data namespace: {context}",
        )
    return context


class DashboardManager(BaseDashboardManager):
    """Concrete DashboardManager backed by Unify contexts and Orchestra tokens."""

    class Config:
        """Context registration for DashboardManager's owned namespace."""

        required_contexts = [
            TableContext(
                name="Dashboards/Tiles",
                description=(
                    "Registry of dashboard tiles (HTML visualization artifacts). "
                    "Each row stores tile metadata and the full HTML content."
                ),
                fields=model_to_fields(TileRecordRow),
                unique_keys={"tile_id": "int"},
                auto_counting={"tile_id": None},
            ),
            TableContext(
                name=ACTIONS_TABLE,
                description=(
                    "Registry of tile actions that authenticated Console can "
                    "dispatch. Each row wires a Functions-catalogue id to a "
                    "tile button (label, icon, result_mode)."
                ),
                fields=model_to_fields(ActionRecordRow),
                unique_keys={"action_id": "int"},
                auto_counting={"action_id": None},
            ),
            TableContext(
                name="Dashboards/Layouts",
                description=(
                    "Registry of composed dashboard layouts. "
                    "Each row stores layout metadata and tile positions as JSON."
                ),
                fields=model_to_fields(DashboardRecordRow),
                unique_keys={"dashboard_id": "int"},
                auto_counting={"dashboard_id": None},
            ),
            TableContext(
                name=DASHBOARDS_META_TABLE,
                description="Metadata for source-defined custom dashboard sync state.",
                fields=model_to_fields(DashboardMeta),
                unique_keys={"meta_id": "int"},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._meta_ctx = ContextRegistry.get_context(self, DASHBOARDS_META_TABLE)
        self._custom_dashboards_synced = False
        self._custom_dashboards_synced_contexts: set[str] = set()
        self._destination_context_lock = RLock()
        logger.debug("DashboardManager initialized")

    def _get_dm(self):
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_data_manager()

    def _get_fm(self):
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_function_manager()

    def _table_context_for_root(self, root_context: str, table_name: str) -> str:
        """Return the concrete dashboard context under one registry root."""
        context = f"{root_context.strip('/')}/{table_name}"
        suffix = table_name.rsplit("/", 1)[-1]
        return _require_dashboard_context(context, suffix)

    def _table_context_for_destination(
        self,
        table_name: str,
        destination: str | None,
    ) -> str:
        """Resolve a dashboard row destination into a concrete table context."""
        root_context = ContextRegistry.write_root(
            self,
            table_name,
            destination=destination,
        )
        return self._table_context_for_root(root_context, table_name)

    def _read_table_contexts(self, table_name: str) -> list[str]:
        """Return ordered concrete dashboard table contexts visible to this assistant."""
        return [
            self._table_context_for_root(root_context, table_name)
            for root_context in ContextRegistry.read_roots(self, table_name)
        ]

    def _resolved_root_context(
        self,
        table_name: str,
        destination: str | None,
    ) -> str:
        """Return the root context for a destination without provisioning."""
        _, _, root_context = ContextRegistry.resolve_root(
            self,
            table_name,
            destination=destination,
        )
        return root_context

    def _data_binding_root(
        self,
        table_name: str,
        *,
        destination: str | None,
        data_scope: str,
    ) -> str:
        """Resolve which root live tile data bindings should read from."""
        if data_scope == DASHBOARD_DATA_SCOPE:
            return self._resolved_root_context(table_name, destination)
        if not data_scope.startswith(TEAM_DESTINATION_PREFIX):
            raise ToolErrorException(
                {
                    "error_kind": INVALID_DESTINATION_ERROR,
                    "message": (
                        "data_scope must be 'dashboard' or an accessible "
                        "'team:<id>' destination."
                    ),
                    "details": {
                        "destination": data_scope,
                        "team_ids": SESSION_DETAILS.team_ids,
                        "table_name": table_name,
                    },
                },
            )
        return self._resolved_root_context(table_name, data_scope)

    def _tool_error_message(self, exc: ToolErrorException) -> str:
        """Convert structured tool errors into Dashboard result errors."""
        return f"{exc.payload['error_kind']}: {exc.payload['message']}"

    # ──────────────────────────────────────────────────────────────────────────
    # Tiles
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDashboardManager.create_tile, updated=())
    def create_tile(
        self,
        html: str,
        *,
        title: str,
        description: Optional[str] = None,
        data_bindings: Optional[List[DataBinding]] = None,
        on_data: Optional[str] = None,
        actions: Optional[List[TileAction]] = None,
        destination: str | None = None,
        data_scope: str = DASHBOARD_DATA_SCOPE,
    ) -> TileResult:
        try:
            tile_context = self._table_context_for_destination(
                TILES_TABLE,
                destination,
            )
            actions_context = self._table_context_for_destination(
                ACTIONS_TABLE,
                destination,
            )
            dm = self._get_dm()
            token = generate_token()
            bindings = validate_data_bindings(data_bindings)
            tile_actions = validate_tile_actions(actions)

            validate_on_data(on_data, bindings)

            if data_scope != DASHBOARD_DATA_SCOPE and not bindings:
                return TileResult(
                    error=(
                        "data_scope can only be set when fresh "
                        "data_bindings are supplied."
                    ),
                )

            if bindings and on_data is not None:
                bindings = ensure_binding_aliases(bindings)

            if bindings:
                binding_root = self._data_binding_root(
                    TILES_TABLE,
                    destination=destination,
                    data_scope=data_scope,
                )
                bindings = resolve_binding_contexts(
                    bindings,
                    base_context=binding_root,
                )
                verify_data_bindings(bindings, dm)

            row = build_tile_record_row(
                token=token,
                html=html,
                title=title,
                description=description,
                data_bindings=bindings,
                on_data=on_data,
                data_scope=data_scope,
            )

            dm.insert_rows(tile_context, [row.model_dump()])

            register_token(token, "tile", tile_context, _get_active_project())

            if tile_actions is not None:
                replace_tile_actions(
                    actions_context=actions_context,
                    tile_token=token,
                    actions=tile_actions,
                    data_manager=dm,
                    function_manager=self._get_fm(),
                )

            return TileResult(
                url=_build_tile_url(token),
                token=token,
                title=title,
            )
        except ToolErrorException as e:
            return TileResult(error=self._tool_error_message(e))
        except Exception as e:
            logger.exception("create_tile failed")
            return TileResult(error=str(e))

    @functools.wraps(BaseDashboardManager.get_tile, updated=())
    def get_tile(self, token: str) -> Optional[TileRecord]:
        dm = self._get_dm()
        for context in self._read_table_contexts(TILES_TABLE):
            rows = dm.filter(
                context,
                filter=f"token == '{token}'",
                limit=1,
            )
            if rows:
                return TileRecord(**rows[0])
        return None

    @functools.wraps(BaseDashboardManager.update_tile, updated=())
    def update_tile(
        self,
        token: str,
        *,
        html: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        data_bindings: Optional[List[DataBinding]] = None,
        on_data: Optional[str] = None,
        actions: Optional[List[TileAction]] = None,
        destination: str | None = None,
        data_scope: Optional[str] = None,
    ) -> TileResult:
        try:
            if data_scope is not None and not data_bindings:
                return TileResult(
                    error=(
                        "data_scope can only be changed when fresh "
                        "data_bindings are supplied."
                    ),
                )
            tile_context = self._table_context_for_destination(
                TILES_TABLE,
                destination,
            )
            actions_context = self._table_context_for_destination(
                ACTIONS_TABLE,
                destination,
            )
            dm = self._get_dm()
            updates: Dict[str, Any] = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if html is not None:
                updates["html_content"] = html
            if title is not None:
                updates["title"] = title
            if description is not None:
                updates["description"] = description

            if data_bindings is not None:
                bindings = validate_data_bindings(data_bindings)
                if bindings:
                    effective_data_scope = data_scope
                    if effective_data_scope is None:
                        existing_rows = dm.filter(
                            tile_context,
                            filter=f"token == '{token}'",
                            limit=1,
                        )
                        existing_scope = (
                            existing_rows[0].get("data_scope")
                            if existing_rows
                            else None
                        )
                        effective_data_scope = existing_scope or DASHBOARD_DATA_SCOPE
                    binding_root = self._data_binding_root(
                        TILES_TABLE,
                        destination=destination,
                        data_scope=effective_data_scope,
                    )
                    if on_data is not None and on_data != "":
                        bindings = ensure_binding_aliases(bindings)
                    bindings = resolve_binding_contexts(
                        bindings,
                        base_context=binding_root,
                    )
                    verify_data_bindings(bindings, dm)
                    updates["has_data_bindings"] = True
                    updates["data_scope"] = effective_data_scope

                    all_ctxs: list[str] = []
                    for b in bindings:
                        all_ctxs.extend(_contexts_for_binding(b))
                    updates["data_binding_contexts"] = ",".join(
                        dict.fromkeys(all_ctxs),
                    )
                    updates["data_bindings_json"] = serialize_bindings(bindings)
                else:
                    updates["has_data_bindings"] = False
                    updates["data_scope"] = DASHBOARD_DATA_SCOPE
                    updates["data_binding_contexts"] = None
                    updates["data_bindings_json"] = None

            if on_data is not None:
                if on_data == "":
                    updates["on_data_script"] = None
                else:
                    has_bindings = bool(data_bindings)
                    if not has_bindings:
                        existing_rows = dm.filter(
                            tile_context,
                            filter=f"token == '{token}'",
                            limit=1,
                        )
                        has_bindings = bool(
                            existing_rows and existing_rows[0].get("has_data_bindings"),
                        )
                    if not has_bindings:
                        validate_on_data(on_data, data_bindings)
                    updates["on_data_script"] = on_data

            updated_count = dm.update_rows(
                tile_context,
                updates,
                filter=f"token == '{token}'",
            )
            if updated_count == 0:
                return TileResult(error=f"Tile '{token}' not found")

            if actions is not None:
                tile_actions = validate_tile_actions(actions)
                replace_tile_actions(
                    actions_context=actions_context,
                    tile_token=token,
                    actions=tile_actions,
                    data_manager=dm,
                    function_manager=self._get_fm(),
                )

            return TileResult(
                url=_build_tile_url(token),
                token=token,
                title=title,
            )
        except ToolErrorException as e:
            return TileResult(error=self._tool_error_message(e))
        except Exception as e:
            logger.exception("update_tile failed")
            return TileResult(error=str(e))

    @functools.wraps(BaseDashboardManager.delete_tile, updated=())
    def delete_tile(self, token: str, *, destination: str | None = None) -> bool:
        try:
            context = self._table_context_for_destination(TILES_TABLE, destination)
            actions_context = self._table_context_for_destination(
                ACTIONS_TABLE,
                destination,
            )
            dm = self._get_dm()
            delete_tile_actions(
                actions_context=actions_context,
                tile_token=token,
                data_manager=dm,
            )
            deleted = dm.delete_rows(
                context,
                filter=f"token == '{token}'",
            )
            if deleted:
                delete_token(token)
            return deleted > 0
        except ToolErrorException:
            return False

    @functools.wraps(BaseDashboardManager.list_tiles, updated=())
    def list_tiles(
        self,
        *,
        filter: Optional[str] = None,
        limit: int = 50,
    ) -> List[TileRecord]:
        dm = self._get_dm()
        result = []
        for context in self._read_table_contexts(TILES_TABLE):
            remaining = limit - len(result)
            if remaining <= 0:
                break
            rows = dm.filter(
                context,
                filter=filter,
                exclude_columns=["html_content"],
                limit=remaining,
            )
            for row in rows:
                row.setdefault("html_content", "")
                result.append(TileRecord(**row))
                if len(result) >= limit:
                    return result
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Dashboards
    # ──────────────────────────────────────────────────────────────────────────

    @functools.wraps(BaseDashboardManager.create_dashboard, updated=())
    def create_dashboard(
        self,
        title: str,
        *,
        description: Optional[str] = None,
        tiles: Optional[List[TilePosition]] = None,
        destination: str | None = None,
    ) -> DashboardResult:
        try:
            layout_context = self._table_context_for_destination(
                LAYOUTS_TABLE,
                destination,
            )
            token = generate_token()
            tile_list = tiles or []
            row = build_dashboard_record_row(
                token=token,
                title=title,
                tiles=tile_list,
                description=description,
            )

            dm = self._get_dm()
            dm.insert_rows(layout_context, [row.model_dump()])

            register_token(token, "dashboard", layout_context, _get_active_project())

            return DashboardResult(
                url=_build_dashboard_url(token),
                token=token,
                title=title,
                tiles=tile_list,
            )
        except ToolErrorException as e:
            return DashboardResult(error=self._tool_error_message(e))
        except Exception as e:
            logger.exception("create_dashboard failed")
            return DashboardResult(error=str(e))

    @functools.wraps(BaseDashboardManager.get_dashboard, updated=())
    def get_dashboard(self, token: str) -> Optional[DashboardResult]:
        dm = self._get_dm()
        for context in self._read_table_contexts(LAYOUTS_TABLE):
            rows = dm.filter(
                context,
                filter=f"token == '{token}'",
                limit=1,
            )
            if rows:
                record = rows[0]
                tile_positions = deserialize_layout(record.get("layout", "[]"))
                return DashboardResult(
                    url=_build_dashboard_url(token),
                    token=token,
                    title=record.get("title"),
                    tiles=tile_positions,
                )
        return None

    @functools.wraps(BaseDashboardManager.update_dashboard, updated=())
    def update_dashboard(
        self,
        token: str,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        tiles: Optional[List[TilePosition]] = None,
        destination: str | None = None,
    ) -> DashboardResult:
        try:
            layout_context = self._table_context_for_destination(
                LAYOUTS_TABLE,
                destination,
            )
            updates: Dict[str, Any] = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if title is not None:
                updates["title"] = title
            if description is not None:
                updates["description"] = description
            if tiles is not None:
                updates["layout"] = serialize_layout(tiles)
                updates["tile_count"] = len(tiles)

            dm = self._get_dm()
            updated_count = dm.update_rows(
                layout_context,
                updates,
                filter=f"token == '{token}'",
            )
            if updated_count == 0:
                return DashboardResult(error=f"Dashboard '{token}' not found")

            final_tiles = tiles
            if final_tiles is None:
                rows = dm.filter(
                    layout_context,
                    filter=f"token == '{token}'",
                    limit=1,
                )
                final_tiles = (
                    deserialize_layout(rows[0].get("layout", "[]")) if rows else []
                )

            return DashboardResult(
                url=_build_dashboard_url(token),
                token=token,
                title=title,
                tiles=final_tiles,
            )
        except ToolErrorException as e:
            return DashboardResult(error=self._tool_error_message(e))
        except Exception as e:
            logger.exception("update_dashboard failed")
            return DashboardResult(error=str(e))

    @functools.wraps(BaseDashboardManager.delete_dashboard, updated=())
    def delete_dashboard(self, token: str, *, destination: str | None = None) -> bool:
        try:
            context = self._table_context_for_destination(LAYOUTS_TABLE, destination)
            dm = self._get_dm()
            deleted = dm.delete_rows(
                context,
                filter=f"token == '{token}'",
            )
            if deleted:
                delete_token(token)
            return deleted > 0
        except ToolErrorException:
            return False

    @functools.wraps(BaseDashboardManager.list_dashboards, updated=())
    def list_dashboards(
        self,
        *,
        filter: Optional[str] = None,
        limit: int = 50,
    ) -> List[DashboardRecord]:
        dm = self._get_dm()
        result: list[DashboardRecord] = []
        for context in self._read_table_contexts(LAYOUTS_TABLE):
            remaining = limit - len(result)
            if remaining <= 0:
                break
            rows = dm.filter(
                context,
                filter=filter,
                limit=remaining,
            )
            for row in rows:
                result.append(DashboardRecord(**row))
                if len(result) >= limit:
                    return result
        return result

    def _meta_context_for_destination(self, destination: str | None) -> str:
        root_context = ContextRegistry.write_root(
            self,
            DASHBOARDS_META_TABLE,
            destination=destination,
        )
        return f"{root_context.strip('/')}/{DASHBOARDS_META_TABLE}"

    @contextmanager
    def _temporary_meta_context(self, context: str):
        with self._destination_context_lock:
            original = self._meta_ctx
            self._meta_ctx = context
            try:
                yield
            finally:
                self._meta_ctx = original

    def _sync_destination_contexts(
        self,
        destination: str | None,
    ) -> tuple[str, bool]:
        meta_context = self._meta_context_for_destination(destination)
        return meta_context, destination in (None, "personal")

    def _get_stored_custom_dashboards_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_dashboards_hash", "") or ""
        except Exception as exc:
            logger.warning("Failed to read custom dashboards hash: %s", exc)
        return ""

    def _store_custom_dashboards_hash(self, hash_value: str) -> None:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=self._meta_ctx,
                    logs=[logs[0].id],
                    entries={"custom_dashboards_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_dashboards_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning("Failed to store custom dashboards hash: %s", exc)

    def _get_custom_rows(
        self,
        table_name: str,
        destination: str | None,
    ) -> Dict[str, Dict[str, Any]]:
        context = self._table_context_for_destination(table_name, destination)
        dm = self._get_dm()
        rows = dm.filter(
            context,
            filter="custom_hash != None",
            limit=1000,
        )
        indexed: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            custom_key = row.get("custom_key")
            if custom_key:
                indexed[str(custom_key)] = row
        return indexed

    def _resolve_entity_token(
        self,
        *,
        row: Dict[str, Any],
        db_rows: Dict[str, Dict[str, Any]],
    ) -> str:
        explicit = str(row.get("token", "")).strip()
        if explicit:
            return explicit
        custom_key = str(row.get("custom_key", ""))
        if custom_key and custom_key in db_rows:
            return str(db_rows[custom_key]["token"])
        return generate_token()

    def _parse_data_bindings(self, raw_bindings: Any) -> Optional[List[DataBinding]]:
        if raw_bindings is None:
            return None
        adapter = TypeAdapter(List[DataBinding])
        return adapter.validate_python(raw_bindings)

    def _prepare_tile_bindings(
        self,
        *,
        row: Dict[str, Any],
        destination: str | None,
        data_scope: str,
        dm,
    ) -> tuple[Optional[List[DataBinding]], str]:
        bindings = self._parse_data_bindings(row.get("data_bindings"))
        bindings = validate_data_bindings(bindings)
        on_data = row.get("on_data_script")
        validate_on_data(on_data, bindings)
        if bindings:
            bindings = ensure_binding_aliases(bindings)
            binding_root = self._data_binding_root(
                TILES_TABLE,
                destination=destination,
                data_scope=data_scope,
            )
            bindings = resolve_binding_contexts(
                bindings,
                base_context=binding_root,
            )
            verify_data_bindings(bindings, dm)
        return bindings, data_scope

    def _build_tile_row_data(
        self,
        *,
        row: Dict[str, Any],
        token: str,
        bindings: Optional[List[DataBinding]],
        data_scope: str,
    ) -> Dict[str, Any]:
        tile_row = build_tile_record_row(
            token=token,
            html=str(row.get("html_content", "")),
            title=str(row.get("title", "")),
            description=row.get("description"),
            data_bindings=bindings,
            on_data=row.get("on_data_script"),
            data_scope=data_scope,
        )
        payload = tile_row.model_dump()
        payload["custom_key"] = row.get("custom_key")
        payload["custom_hash"] = row.get("custom_hash")
        return payload

    def _resolve_layout_positions(
        self,
        positions: List[Dict[str, Any]],
        tile_tokens_by_id: Dict[str, str],
    ) -> List[TilePosition]:
        resolved: List[TilePosition] = []
        for position in positions:
            if "tile_token" in position:
                resolved.append(TilePosition(**position))
                continue
            tile_id = str(position.get("tile_id", ""))
            token = tile_tokens_by_id.get(tile_id)
            if not token:
                logger.warning(
                    "Skipping layout position for unknown tile_id=%s",
                    tile_id,
                )
                continue
            resolved.append(
                TilePosition(
                    tile_token=token,
                    x=int(position.get("x", 0)),
                    y=int(position.get("y", 0)),
                    w=int(position.get("w", 6)),
                    h=int(position.get("h", 4)),
                ),
            )
        return resolved

    def _tile_tokens_by_id(
        self,
        destination: str | None,
    ) -> Dict[str, str]:
        tokens: Dict[str, str] = {}
        for custom_key, row in self._get_custom_rows(
            TILES_TABLE,
            destination,
        ).items():
            if custom_key.startswith("tile|"):
                tokens[custom_key.split("|", 1)[1]] = str(row["token"])
        return tokens

    def _insert_custom_tile(
        self,
        *,
        destination: str | None,
        row_data: Dict[str, Any],
    ) -> None:
        tile_context = self._table_context_for_destination(TILES_TABLE, destination)
        dm = self._get_dm()
        dm.insert_rows(tile_context, [row_data])
        register_token(
            row_data["token"],
            "tile",
            tile_context,
            _get_active_project(),
        )

    def _update_custom_tile(
        self,
        *,
        destination: str | None,
        tile_id: int,
        row_data: Dict[str, Any],
    ) -> None:
        tile_context = self._table_context_for_destination(TILES_TABLE, destination)
        dm = self._get_dm()
        clean = {k: v for k, v in row_data.items() if k != "tile_id"}
        dm.update_rows(
            tile_context,
            updates=clean,
            filter=f"tile_id == {int(tile_id)}",
        )
        register_token(
            row_data["token"],
            "tile",
            tile_context,
            _get_active_project(),
        )

    def _delete_custom_tile_by_key(
        self,
        *,
        destination: str | None,
        custom_key: str,
    ) -> None:
        tile_context = self._table_context_for_destination(TILES_TABLE, destination)
        dm = self._get_dm()
        rows = dm.filter(
            tile_context,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
            limit=1,
        )
        if not rows:
            return
        token = rows[0].get("token")
        dm.delete_rows(
            tile_context,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
        )
        if token:
            delete_token(str(token))

    def _insert_custom_layout(
        self,
        *,
        destination: str | None,
        row_data: Dict[str, Any],
    ) -> None:
        layout_context = self._table_context_for_destination(
            LAYOUTS_TABLE,
            destination,
        )
        dm = self._get_dm()
        dm.insert_rows(layout_context, [row_data])
        register_token(
            row_data["token"],
            "dashboard",
            layout_context,
            _get_active_project(),
        )

    def _update_custom_layout(
        self,
        *,
        destination: str | None,
        dashboard_id: int,
        row_data: Dict[str, Any],
    ) -> None:
        layout_context = self._table_context_for_destination(
            LAYOUTS_TABLE,
            destination,
        )
        dm = self._get_dm()
        clean = {k: v for k, v in row_data.items() if k != "dashboard_id"}
        dm.update_rows(
            layout_context,
            updates=clean,
            filter=f"dashboard_id == {int(dashboard_id)}",
        )
        register_token(
            row_data["token"],
            "dashboard",
            layout_context,
            _get_active_project(),
        )

    def _delete_custom_layout_by_key(
        self,
        *,
        destination: str | None,
        custom_key: str,
    ) -> None:
        layout_context = self._table_context_for_destination(
            LAYOUTS_TABLE,
            destination,
        )
        dm = self._get_dm()
        rows = dm.filter(
            layout_context,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
            limit=1,
        )
        if not rows:
            return
        token = rows[0].get("token")
        dm.delete_rows(
            layout_context,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
        )
        if token:
            delete_token(str(token))

    def sync_custom_dashboards(
        self,
        *,
        source_entities: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
        destination: str | None = None,
    ) -> bool:
        """Ensure deployment-defined dashboard tiles and layouts match sources."""
        try:
            meta_context, is_personal = self._sync_destination_contexts(destination)
        except ToolErrorException as exc:
            logger.warning(
                "Skipping custom dashboards sync for destination %r: %s",
                destination,
                exc.payload,
            )
            return False
        with self._temporary_meta_context(meta_context):
            if source_entities is None:
                source_entities = {TILES_NAMESPACE: {}, LAYOUTS_NAMESPACE: {}}
            destination_entities = {
                TILES_NAMESPACE: {},
                LAYOUTS_NAMESPACE: {},
            }
            destination_label = destination or "personal"
            for namespace in (TILES_NAMESPACE, LAYOUTS_NAMESPACE):
                for entity_id, spec in source_entities.get(namespace, {}).items():
                    if (spec.get("destination") or "personal") == destination_label:
                        destination_entities[namespace][entity_id] = spec

            expected_hash = compute_custom_dashboards_hash(
                source_entities=destination_entities,
            )
            current_hash = self._get_stored_custom_dashboards_hash()
            already_synced = (
                self._custom_dashboards_synced
                if is_personal
                else meta_context in self._custom_dashboards_synced_contexts
            )
            if already_synced and current_hash == expected_hash:
                return False
            if current_hash == expected_hash:
                logger.debug("Custom dashboards hash matches, skipping sync")
                if is_personal:
                    self._custom_dashboards_synced = True
                else:
                    self._custom_dashboards_synced_contexts.add(meta_context)
                return False

            logger.info(
                "Custom dashboards hash mismatch "
                "(current=%s, expected=%s), syncing...",
                current_hash,
                expected_hash,
            )

            dm = self._get_dm()
            tile_db_rows = self._get_custom_rows(TILES_TABLE, destination)
            processed_tile_keys: Set[str] = set()

            for _entity_id, tile_spec in destination_entities[TILES_NAMESPACE].items():
                data_scope = tile_spec.get("data_scope", DASHBOARD_DATA_SCOPE)
                for row in tile_spec.get("rows", []):
                    custom_key = str(row.get("custom_key", ""))
                    if not custom_key:
                        continue
                    processed_tile_keys.add(custom_key)
                    token = self._resolve_entity_token(
                        row=row,
                        db_rows=tile_db_rows,
                    )
                    try:
                        bindings, resolved_scope = self._prepare_tile_bindings(
                            row=row,
                            destination=destination,
                            data_scope=data_scope,
                            dm=dm,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Skipping custom tile %s: %s",
                            custom_key,
                            exc,
                        )
                        continue
                    row_data = self._build_tile_row_data(
                        row=row,
                        token=token,
                        bindings=bindings,
                        data_scope=resolved_scope,
                    )
                    if custom_key in tile_db_rows:
                        db_entry = tile_db_rows[custom_key]
                        if db_entry.get("custom_hash") != row_data.get("custom_hash"):
                            self._update_custom_tile(
                                destination=destination,
                                tile_id=int(db_entry["tile_id"]),
                                row_data=row_data,
                            )
                    else:
                        self._insert_custom_tile(
                            destination=destination,
                            row_data=row_data,
                        )

            tile_db_rows = self._get_custom_rows(TILES_TABLE, destination)
            for custom_key in tile_db_rows:
                if custom_key not in processed_tile_keys:
                    self._delete_custom_tile_by_key(
                        destination=destination,
                        custom_key=custom_key,
                    )

            tile_tokens_by_id = self._tile_tokens_by_id(destination)
            layout_db_rows = self._get_custom_rows(LAYOUTS_TABLE, destination)
            processed_layout_keys: Set[str] = set()

            for _entity_id, layout_spec in destination_entities[
                LAYOUTS_NAMESPACE
            ].items():
                for row in layout_spec.get("rows", []):
                    custom_key = str(row.get("custom_key", ""))
                    if not custom_key:
                        continue
                    processed_layout_keys.add(custom_key)
                    positions = row.get("positions", [])
                    if not isinstance(positions, list):
                        logger.warning(
                            "Skipping custom layout %s: positions must be a list",
                            custom_key,
                        )
                        continue
                    tile_positions = self._resolve_layout_positions(
                        positions,
                        tile_tokens_by_id,
                    )
                    token = self._resolve_entity_token(
                        row=row,
                        db_rows=layout_db_rows,
                    )
                    layout_row = build_dashboard_record_row(
                        token=token,
                        title=str(row.get("title", "")),
                        tiles=tile_positions,
                        description=row.get("description"),
                    )
                    row_data = layout_row.model_dump()
                    row_data["custom_key"] = row.get("custom_key")
                    row_data["custom_hash"] = row.get("custom_hash")
                    if custom_key in layout_db_rows:
                        db_entry = layout_db_rows[custom_key]
                        if db_entry.get("custom_hash") != row_data.get("custom_hash"):
                            self._update_custom_layout(
                                destination=destination,
                                dashboard_id=int(db_entry["dashboard_id"]),
                                row_data=row_data,
                            )
                    else:
                        self._insert_custom_layout(
                            destination=destination,
                            row_data=row_data,
                        )

            layout_db_rows = self._get_custom_rows(LAYOUTS_TABLE, destination)
            for custom_key in layout_db_rows:
                if custom_key not in processed_layout_keys:
                    self._delete_custom_layout_by_key(
                        destination=destination,
                        custom_key=custom_key,
                    )

            self._store_custom_dashboards_hash(expected_hash)
            if is_personal:
                self._custom_dashboards_synced = True
            else:
                self._custom_dashboards_synced_contexts.add(meta_context)
            return True

    def sync_custom(
        self,
        *,
        source_entities: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    ) -> bool:
        """Sync custom dashboard tiles and layouts across destinations."""
        if source_entities is None:
            source_entities = {TILES_NAMESPACE: {}, LAYOUTS_NAMESPACE: {}}

        destinations: Set[str] = set()
        for namespace in (TILES_NAMESPACE, LAYOUTS_NAMESPACE):
            for spec in source_entities.get(namespace, {}).values():
                destinations.add(spec.get("destination") or "personal")

        changed = False
        for destination in destinations:
            destination_arg = None if destination == "personal" else destination
            changed |= self.sync_custom_dashboards(
                source_entities=source_entities,
                destination=destination_arg,
            )
        return changed
