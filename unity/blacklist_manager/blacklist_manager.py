from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import functools

import unisdk

from ..common.log_utils import log as unity_log
from ..common.data_store import DataStore
from ..common.model_to_fields import model_to_fields
from ..common.federated_search import (
    CONTEXT_FIELD,
    SOURCE_FIELD,
    FederatedSearchContext,
    default_filter_fetcher,
    federated_filter,
)
from ..common.filter_utils import normalize_filter_expr
from ..common.tool_outcome import ToolErrorException
from ..blacklist_manager.types.blacklist import BlackList
from unity.conversation_manager.cm_types import Medium
from .base import BaseBlackListManager
from ..common.context_registry import (
    TEAM_CONTEXT_PREFIX,
    ContextRegistry,
    TableContext,
)


class BlackListManager(BaseBlackListManager):
    """
    Manages a minimal catalogue of blacklisted contact details, keyed by ``blacklist_id``.
    """

    class Config:
        required_contexts = [
            TableContext(
                name="BlackList",
                description="List of blacklisted contact details (per medium).",
                fields=model_to_fields(BlackList),
                unique_keys={"blacklist_id": "int"},
                auto_counting={"blacklist_id": None},
            ),
        ]

    # ------------------------------------------------------------------ #
    # Construction                                                       #
    # ------------------------------------------------------------------ #
    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, "BlackList")

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
        errors: list[Exception] = []

        def fetcher(spec, row_filter, sorting, fetch_limit):
            try:
                return default_filter_fetcher(spec, row_filter, sorting, fetch_limit)
            except Exception as exc:
                errors.append(exc)
                return []

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
            fetcher=fetcher,
        )
        if not annotated_rows and errors:
            raise errors[-1]

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
