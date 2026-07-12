from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Optional, Set, Tuple
import functools
import logging
import threading

import unisdk

from ..common.log_utils import log as unity_log, create_logs as unity_create_logs
from ..common.authorship import strip_authoring_assistant_id
from ..common.embed_utils import list_private_fields
from ..common.data_store import DataStore
from ..common.model_to_fields import model_to_fields
from ..common.federated_search import (
    CONTEXT_FIELD,
    SOURCE_FIELD,
    FederatedSearchContext,
    federated_filter,
)
from ..common.filter_utils import normalize_filter_expr
from ..common.tool_outcome import ToolErrorException
from ..blacklist_manager.types.blacklist import BlackList
from ..blacklist_manager.types.meta import BlacklistMeta
from .custom_blacklist import compute_custom_blacklist_hash
from unify.conversation_manager.cm_types import Medium
from .base import BaseBlackListManager
from ..common.context_registry import (
    TEAM_CONTEXT_PREFIX,
    ContextRegistry,
    TableContext,
)

logger = logging.getLogger(__name__)

BLACKLIST_TABLE = "BlackList"
BLACKLIST_META_TABLE = "BlackList/Meta"


class BlackListManager(BaseBlackListManager):
    """
    Manages a minimal catalogue of blacklisted contact details, keyed by ``blacklist_id``.
    """

    class Config:
        required_contexts = [
            TableContext(
                name=BLACKLIST_TABLE,
                description="List of blacklisted contact details (per medium).",
                fields=model_to_fields(BlackList),
                unique_keys={"blacklist_id": "int"},
                auto_counting={"blacklist_id": None},
            ),
            TableContext(
                name=BLACKLIST_META_TABLE,
                description="Metadata for source-defined custom blacklist sync state.",
                fields=model_to_fields(BlacklistMeta),
                unique_keys={"meta_id": "int"},
            ),
        ]

    # ------------------------------------------------------------------ #
    # Construction                                                       #
    # ------------------------------------------------------------------ #
    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, BLACKLIST_TABLE)
        self._meta_ctx = ContextRegistry.get_context(self, BLACKLIST_META_TABLE)
        self._custom_blacklist_synced = False
        self._custom_blacklist_synced_contexts: set[str] = set()
        self._destination_context_lock = threading.RLock()
        self._destination_write_scoped = False

        # Local DataStore mirror (write-through only; never read from it)
        self._data_store = DataStore.for_context(
            self._ctx,
            key_fields=("blacklist_id",),
        )

        # Immutable built-in columns derived directly from the model
        self._BUILTIN_FIELDS: Tuple[str, ...] = tuple(
            field for field in BlackList.model_fields.keys() if field != "destination"
        )

    def _blacklist_context_from_root(self, root_context: str) -> str:
        """Return the concrete BlackList context under one registry root."""

        return f"{root_context.strip('/')}/BlackList"

    def _blacklist_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public write destination into a concrete BlackList context."""

        root_context = ContextRegistry.write_root(
            self,
            "BlackList",
            destination=destination,
        )
        return self._blacklist_context_from_root(root_context)

    def _meta_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public destination into one concrete BlackList/Meta context."""
        root_context = ContextRegistry.write_root(
            self,
            BLACKLIST_META_TABLE,
            destination=destination,
        )
        return f"{root_context.strip('/')}/{BLACKLIST_META_TABLE}"

    @contextmanager
    def _temporary_blacklist_context(self, attr_name: str, context: str):
        """Temporarily bind an existing storage method to a resolved context."""
        with self._destination_context_lock:
            original = getattr(self, attr_name)
            was_write_scoped = self._destination_write_scoped
            setattr(self, attr_name, context)
            self._destination_write_scoped = True
            try:
                yield
            finally:
                setattr(self, attr_name, original)
                self._destination_write_scoped = was_write_scoped

    def _sync_destination_contexts(
        self,
        destination: str | None,
    ) -> tuple[str, str, bool]:
        """Return destination-scoped blacklist context, meta context, and personal flag."""
        data_context = self._blacklist_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)
        return data_context, meta_context, destination in (None, "personal")

    def _read_blacklist_contexts(self) -> list[str]:
        """Return ordered concrete BlackList contexts visible to this assistant."""

        try:
            root_contexts = ContextRegistry.read_roots(self, "BlackList")
            contexts = [
                self._blacklist_context_from_root(root) for root in root_contexts
            ]
        except RuntimeError as exc:
            if "no base context available" not in str(exc):
                raise
            from ..session_details import SESSION_DETAILS

            contexts = [self._ctx]
            contexts.extend(
                f"{TEAM_CONTEXT_PREFIX}{team_id}/BlackList"
                for team_id in sorted(set(SESSION_DETAILS.team_ids))
            )
        return list(dict.fromkeys(contexts))

    def _data_store_for_context(self, context: str) -> DataStore:
        """Return the per-root local cache for a concrete BlackList context."""

        if context == self._ctx:
            return self._data_store
        return DataStore.for_context(context, key_fields=("blacklist_id",))

    def _destination_for_context(self, context: str) -> str:
        """Return the public destination label for a concrete BlackList context."""

        if context.startswith(TEAM_CONTEXT_PREFIX):
            parts = context.split("/")
            if len(parts) >= 2:
                return f"team:{parts[1]}"
        return "personal"

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    @functools.wraps(BaseBlackListManager.clear, updated=())
    def clear(self, *, destination: str | None = None) -> None:
        try:
            context = self._blacklist_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload  # type: ignore[return-value]
        unisdk.delete_context(context)

        # Force re-provisioning by clearing TableStore ensure memo for this context
        ContextRegistry.forget(self, "BlackList")
        try:
            context = self._blacklist_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload  # type: ignore[return-value]

        # Verify visibility before proceeding
        try:
            import time as _time  # local import

            for _ in range(3):
                try:
                    unisdk.get_fields(context=context)
                    break
                except Exception:
                    _time.sleep(0.05)
        except Exception:
            pass

    @functools.wraps(BaseBlackListManager.filter_blacklist, updated=())
    def filter_blacklist(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        annotated_rows = federated_filter(
            [
                FederatedSearchContext(
                    context=context,
                    source=self._destination_for_context(context),
                    allowed_fields=list(self._BUILTIN_FIELDS),
                )
                for context in self._read_blacklist_contexts()
            ],
            filter=normalize_filter_expr(filter),
            offset=offset,
            limit=limit,
        )

        rows: list[dict[str, Any]] = []
        for annotated in annotated_rows:
            row = {
                key: value
                for key, value in annotated.items()
                if not key.startswith("_federated_")
            }
            row["destination"] = annotated[SOURCE_FIELD]
            self._data_store_for_context(annotated[CONTEXT_FIELD]).put(row)
            rows.append(row)

        entries = [BlackList(**r) for r in rows]
        return {
            "blacklist_keys_to_shorthand": BlackList.shorthand_map(),
            "entries": entries,
            "shorthand_to_blacklist_keys": BlackList.shorthand_inverse_map(),
        }

    @functools.wraps(BaseBlackListManager.create_blacklist_entry, updated=())
    def create_blacklist_entry(
        self,
        *,
        medium: Medium,
        contact_detail: str,
        reason: str,
        destination: str | None = None,
    ) -> Dict[str, Any]:
        try:
            context = self._blacklist_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        payload = BlackList(
            medium=medium,
            contact_detail=contact_detail,
            reason=reason,
        ).to_post_json()
        log = unity_log(
            context=context,
            new=True,
            mutable=True,
            stamp_authoring=True,
            **payload,
        )
        self._data_store_for_context(context).put(log.entries)
        return {
            "outcome": "blacklist entry created",
            "details": {"blacklist_id": log.entries["blacklist_id"]},
        }

    @functools.wraps(BaseBlackListManager.update_blacklist_entry, updated=())
    def update_blacklist_entry(
        self,
        *,
        blacklist_id: int,
        medium: Optional[Medium] = None,
        contact_detail: Optional[str] = None,
        reason: Optional[str] = None,
        destination: str | None = None,
    ) -> Dict[str, Any]:
        try:
            context = self._blacklist_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        updates: Dict[str, Any] = {}
        if medium is not None:
            updates["medium"] = medium
        if contact_detail is not None:
            updates["contact_detail"] = contact_detail
        if reason is not None:
            updates["reason"] = reason
        if not updates:
            raise ValueError(
                "At least one field must be provided to update a blacklist entry.",
            )

        # Resolve target log id
        target_ids = unisdk.get_logs(
            context=context,
            filter=f"blacklist_id == {int(blacklist_id)}",
            return_ids_only=True,
        )
        if not target_ids:
            raise ValueError(
                f"No blacklist entry found with blacklist_id {blacklist_id} to update.",
            )
        if len(target_ids) > 1:
            raise ValueError(
                f"Multiple blacklist rows found with blacklist_id {blacklist_id}. Data integrity issue.",
            )
        log_id = target_ids[0]

        unisdk.update_logs(
            logs=[log_id],
            context=context,
            entries=updates,
            overwrite=True,
        )

        # Refresh local cache from backend
        row = unisdk.get_logs(
            context=context,
            filter=f"blacklist_id == {int(blacklist_id)}",
            limit=1,
            from_fields=list(self._BUILTIN_FIELDS),
        )
        if row:
            self._data_store_for_context(context).put(row[0].entries)

        return {
            "outcome": "blacklist entry updated",
            "details": {"blacklist_id": int(blacklist_id)},
        }

    @functools.wraps(BaseBlackListManager.delete_blacklist_entry, updated=())
    def delete_blacklist_entry(
        self,
        *,
        blacklist_id: int,
        destination: str | None = None,
    ) -> Dict[str, Any]:
        try:
            context = self._blacklist_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        # Resolve target log id in the destination context (for the "not found"
        # / "multiple rows" sanity checks; aggregation contexts are queried
        # separately below since they hold independent log ids — see the
        # cascade loop comment).
        target_ids = unisdk.get_logs(
            context=context,
            filter=f"blacklist_id == {int(blacklist_id)}",
            limit=2,
            return_ids_only=True,
        )
        if not target_ids:
            raise ValueError(
                f"No blacklist entry found with blacklist_id {blacklist_id} to delete.",
            )
        if len(target_ids) > 1:
            raise RuntimeError(
                f"Multiple blacklist rows found with blacklist_id {blacklist_id}. Data integrity issue.",
            )
        ids_in_ctx = unisdk.get_logs(
            context=context,
            filter=f"blacklist_id == {int(blacklist_id)}",
            return_ids_only=True,
        )
        for log_id in ids_in_ctx:
            unisdk.delete_logs(context=context, logs=log_id)
        try:
            self._data_store_for_context(context).delete(blacklist_id)
        except KeyError:
            # If cache did not contain the row, proceed without error
            pass
        return {
            "outcome": "blacklist entry deleted",
            "details": {"blacklist_id": int(blacklist_id)},
        }

    # ------------------------------------------------------------------ #
    # Custom blacklist sync                                               #
    # ------------------------------------------------------------------ #

    def _get_stored_custom_blacklist_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_blacklist_hash", "")
        except Exception as exc:
            logger.warning("Failed to retrieve custom blacklist hash: %s", exc)
        return ""

    def _store_custom_blacklist_hash(self, hash_value: str) -> None:
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
                    entries={"custom_blacklist_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_blacklist_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning("Failed to store custom blacklist hash: %s", exc)

    def _get_custom_blacklist_from_db(self) -> Dict[str, Dict[str, Any]]:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter="custom_hash != None",
            exclude_fields=list_private_fields(self._ctx),
        )
        return {
            lg.entries.get("custom_key"): lg.entries
            for lg in logs
            if lg.entries.get("custom_key")
        }

    def _delete_custom_blacklist_by_key(self, custom_key: str) -> bool:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
            limit=1,
        )
        if not logs:
            return False
        unisdk.delete_logs(context=self._ctx, logs=[logs[0].id])
        return True

    def _update_custom_blacklist(
        self,
        blacklist_id: int,
        data: Dict[str, Any],
    ) -> None:
        log_ids = unisdk.get_logs(
            context=self._ctx,
            filter=f"blacklist_id == {int(blacklist_id)}",
            limit=1,
            return_ids_only=True,
        )
        if not log_ids:
            raise ValueError(
                f"No blacklist entry found with blacklist_id {blacklist_id} to update.",
            )
        update_data = strip_authoring_assistant_id(
            {k: v for k, v in data.items() if k != "blacklist_id"},
        )
        unisdk.update_logs(
            context=self._ctx,
            logs=[log_ids[0]],
            entries=update_data,
            overwrite=True,
        )

    def _insert_custom_blacklist(self, data: Dict[str, Any]) -> int:
        insert_data = {k: v for k, v in data.items() if k != "blacklist_id"}
        result = unity_create_logs(
            context=self._ctx,
            entries=[insert_data],
            stamp_authoring=True,
            recompute_derived=True,
        )
        if isinstance(result, list) and len(result) > 0:
            log = result[0]
            if hasattr(log, "entries"):
                return log.entries.get("blacklist_id", -1)
        elif isinstance(result, dict):
            log_ids = result.get("log_event_ids", [])
            if log_ids:
                logs = unisdk.get_logs(
                    context=self._ctx,
                    filter=f"id == {log_ids[0]}",
                    limit=1,
                )
                if logs and hasattr(logs[0], "entries"):
                    return logs[0].entries.get("blacklist_id")
        return -1

    def sync_custom_blacklist(
        self,
        *,
        source_blacklist: Optional[Dict[str, Dict[str, Any]]] = None,
        destination: str | None = None,
    ) -> bool:
        """Ensure custom blacklist rows match source ``blacklist.jsonl`` definitions."""
        try:
            blacklist_context, meta_context, is_personal = (
                self._sync_destination_contexts(destination)
            )
        except ToolErrorException as exc:
            logger.warning(
                "Skipping custom blacklist sync for destination %r: %s",
                destination,
                exc.payload,
            )
            return False

        with (
            self._temporary_blacklist_context("_ctx", blacklist_context),
            self._temporary_blacklist_context("_meta_ctx", meta_context),
        ):
            if source_blacklist is None:
                source_blacklist = {}
            expected_hash = compute_custom_blacklist_hash(
                source_blacklist=source_blacklist,
            )
            current_hash = self._get_stored_custom_blacklist_hash()
            already_synced = (
                self._custom_blacklist_synced
                if is_personal
                else blacklist_context in self._custom_blacklist_synced_contexts
            )

            if already_synced and current_hash == expected_hash:
                return False

            if current_hash == expected_hash:
                logger.debug("Custom blacklist hash matches, skipping sync")
                if is_personal:
                    self._custom_blacklist_synced = True
                else:
                    self._custom_blacklist_synced_contexts.add(blacklist_context)
                return False

            logger.info(
                "Custom blacklist hash mismatch "
                "(current=%s, expected=%s), syncing...",
                current_hash,
                expected_hash,
            )

            db_blacklist = self._get_custom_blacklist_from_db()
            processed_keys: Set[str] = set()

            for custom_key, source_data in source_blacklist.items():
                processed_keys.add(custom_key)
                blacklist_data = {
                    k: v for k, v in source_data.items() if k not in {"destination"}
                }

                if custom_key in db_blacklist:
                    db_entry = db_blacklist[custom_key]
                    if db_entry.get("custom_hash") != blacklist_data["custom_hash"]:
                        logger.info("Updating custom blacklist entry: %s", custom_key)
                        self._update_custom_blacklist(
                            blacklist_id=db_entry["blacklist_id"],
                            data=blacklist_data,
                        )
                else:
                    existing = unisdk.get_logs(
                        context=self._ctx,
                        filter=f"custom_key == '{custom_key}'",
                        limit=1,
                    )
                    if existing:
                        logger.info(
                            "Overwriting user-added blacklist entry with custom: %s",
                            custom_key,
                        )
                        unisdk.delete_logs(
                            context=self._ctx,
                            logs=[existing[0].id],
                        )

                    logger.info("Inserting custom blacklist entry: %s", custom_key)
                    self._insert_custom_blacklist(blacklist_data)

            for custom_key in db_blacklist:
                if custom_key not in processed_keys:
                    logger.info(
                        "Deleting removed custom blacklist entry: %s",
                        custom_key,
                    )
                    self._delete_custom_blacklist_by_key(custom_key)

            self._store_custom_blacklist_hash(expected_hash)
            if is_personal:
                self._custom_blacklist_synced = True
            else:
                self._custom_blacklist_synced_contexts.add(blacklist_context)
            return True

    def sync_custom(
        self,
        *,
        source_blacklist: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> bool:
        """Sync custom blacklist from pre-collected sources across destinations."""
        if source_blacklist is None:
            source_blacklist = {}

        by_destination: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for custom_key, source_data in source_blacklist.items():
            destination = source_data.get("destination") or "personal"
            by_destination.setdefault(destination, {})[custom_key] = source_data

        changed = False
        for destination, group in by_destination.items():
            destination_arg = None if destination == "personal" else destination
            changed |= self.sync_custom_blacklist(
                source_blacklist=group,
                destination=destination_arg,
            )
        return changed
