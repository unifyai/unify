"""Synchronize deployment-defined integration registry rows."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import unisdk

from unify.common.custom_sync import CustomSyncAdapter, run_custom_sync
from unify.common.log_utils import create_logs as unity_create_logs
from unify.common.sync_lease import exclusive_sync_lease
from unify.integration_registry.custom_integration_registry import (
    compute_custom_integration_registry_hash,
)

logger = logging.getLogger(__name__)

INTEGRATION_MANIFESTS_CONTEXT_LEAF = "Integrations/Manifests"
INTEGRATION_META_CONTEXT_LEAF = "Integrations/Meta"


class IntegrationRegistrySync:
    """Persist deployment integration manifests into ``Integrations/Manifests``."""

    def __init__(self) -> None:
        self._synced = False

    def _active_root(self) -> str:
        active = unisdk.get_active_context()["read"]
        return active.strip("/")

    def _manifests_context(self) -> str:
        ctx = f"{self._active_root()}/{INTEGRATION_MANIFESTS_CONTEXT_LEAF}"
        try:
            unisdk.create_context(ctx)
        except Exception:
            pass
        return ctx

    def _meta_context(self) -> str:
        ctx = f"{self._active_root()}/{INTEGRATION_META_CONTEXT_LEAF}"
        try:
            unisdk.create_context(ctx)
        except Exception:
            pass
        return ctx

    def _get_stored_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_context(),
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_integration_registry_hash", "") or ""
        except Exception as exc:
            logger.warning(
                "Failed to read custom integration registry hash: %s",
                exc,
            )
        return ""

    def _store_hash(self, hash_value: str) -> None:
        ctx = self._meta_context()
        try:
            logs = unisdk.get_logs(
                context=ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=ctx,
                    logs=[logs[0].id],
                    entries={"custom_integration_registry_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=ctx,
                    entries=[
                        {
                            "meta_id": 1,
                            "custom_integration_registry_hash": hash_value,
                        },
                    ],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning(
                "Failed to store custom integration registry hash: %s",
                exc,
            )

    def _get_managed_rows_from_db(self) -> Dict[str, Dict[str, Any]]:
        ctx = self._manifests_context()
        try:
            logs = unisdk.get_logs(
                context=ctx,
                filter="custom_hash != None",
                limit=1000,
            )
        except Exception:
            return {}
        indexed: Dict[str, Dict[str, Any]] = {}
        for log in logs:
            entries = dict(log.entries or {})
            custom_key = entries.get("custom_key") or entries.get("slug")
            if not custom_key:
                continue
            entries["_log_id"] = log.id
            indexed[str(custom_key)] = entries
        return indexed

    def _find_unmanaged_row_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        ctx = self._manifests_context()
        try:
            logs = unisdk.get_logs(
                context=ctx,
                filter=f'slug == "{slug}" and custom_hash == None',
                limit=1,
            )
        except Exception:
            return None
        if not logs:
            return None
        entries = dict(logs[0].entries or {})
        entries["_log_id"] = logs[0].id
        return entries

    def _insert_row(self, row_data: Dict[str, Any]) -> None:
        ctx = self._manifests_context()
        unisdk.log(
            context=ctx,
            **{k: v for k, v in row_data.items() if not k.startswith("_")},
        )

    def _update_row(self, log_id: int, row_data: Dict[str, Any]) -> None:
        ctx = self._manifests_context()
        unisdk.update_logs(
            logs=[log_id],
            context=ctx,
            entries=[{k: v for k, v in row_data.items() if not k.startswith("_")}],
            overwrite=True,
        )

    def _delete_row(self, log_id: int) -> None:
        ctx = self._manifests_context()
        unisdk.delete_logs(context=ctx, logs=log_id)

    def sync_custom(
        self,
        *,
        source_registry: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> bool:
        """Ensure deployment-defined integration registry rows match the source."""
        source_registry = source_registry or {}

        def _mark_synced() -> None:
            self._synced = True

        with exclusive_sync_lease(f"{self._meta_context()}:custom_sync"):
            return run_custom_sync(
                adapter=_IntegrationRegistrySyncAdapter(self),
                source=source_registry,
                expected_hash=compute_custom_integration_registry_hash(
                    source_registry=source_registry,
                ),
                stored_hash=self._get_stored_hash(),
                already_synced=self._synced,
                mark_synced=_mark_synced,
                store_hash=self._store_hash,
            )


class _IntegrationRegistrySyncAdapter(CustomSyncAdapter):
    """Storage mechanics for the integration registry reconcile."""

    kind = "integration registry"

    def __init__(self, syncer: IntegrationRegistrySync) -> None:
        self._syncer = syncer

    def live_rows(self) -> List[Dict[str, Any]]:
        ctx = self._syncer._manifests_context()
        try:
            logs = unisdk.get_logs(
                context=ctx,
                filter="custom_hash != None",
                limit=1000,
            )
        except Exception:
            return []
        rows: List[Dict[str, Any]] = []
        for log in logs:
            entries = dict(log.entries or {})
            entries.setdefault("custom_key", entries.get("slug"))
            entries["_log_id"] = log.id
            rows.append(entries)
        return rows

    def transform(self, key: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        fields.pop("destination", None)
        return fields

    def insert(self, key: str, fields: Dict[str, Any]) -> None:
        self._syncer._insert_row(fields)

    def update(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> None:
        self._syncer._update_row(int(live_row["_log_id"]), fields)

    def delete(self, key: str, live_row: Dict[str, Any]) -> None:
        self._syncer._delete_row(int(live_row["_log_id"]))

    def find_adoptable(
        self,
        key: str,
        fields: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        slug = str(fields.get("slug", key))
        return self._syncer._find_unmanaged_row_by_slug(slug)

    def adopt(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> None:
        self._syncer._update_row(int(live_row["_log_id"]), fields)


_syncer = IntegrationRegistrySync()


def sync_custom_integration_registry(
    *,
    source_registry: Optional[Dict[str, Dict[str, Any]]] = None,
) -> bool:
    """Sync deployment-defined integration registry rows."""
    return _syncer.sync_custom(source_registry=source_registry)
