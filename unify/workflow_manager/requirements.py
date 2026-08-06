"""Resolving whether a workflow's declared integrations are connected.

A bundle names the apps its jobs need by **provider app slug** — the one
id space Console's integrations gallery, the integrations primitives and
these requirements all share. What "connected" means for that app is not
the bundle's business, because it differs per app and can change without
the workflow changing:

- **Third-party, provider-backed** (the gallery's Composio/Pipedream
  catalogue): a connection row exists for the app with status
  ``connected``.
- **Native** (an integration package declared in unify-deploy): the
  package's own manifest names the secrets that make it usable, and the
  assistant's secret keyset either holds them or does not.
- **BYOD OAuth** (Workspace, Microsoft 365): the refresh-token secret is
  the signal, and there is no package to ask, so the bundle names the
  secret itself.

So a requirement carries a slug, and optionally the secrets to look for
when no other authority can answer. Resolution consults each authority in
turn and reports which one settled it, so the Console can say *why* an app
reads as unconnected rather than only that it does.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


def _normalize(slug: str) -> str:
    return str(slug or "").strip().lower().replace("-", "_")


def _json_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


class RequirementResolver:
    """Answers "is this app connected?" for one pass over a bundle.

    Every authority is read at most once per instance and the answers are
    reused, so resolving a bundle's requirements costs one keyset read,
    one connection list and one manifest query regardless of how many
    apps it declares. Each read is best-effort: an authority that cannot
    be reached is treated as silent rather than as a denial, because
    holding a workflow's jobs on a transient read failure is worse than
    arming them a session early.
    """

    def __init__(
        self,
        *,
        keyset: Optional[Iterable[str]] = None,
        connected_apps: Optional[Iterable[str]] = None,
        native_manifests: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self._keyset_override = frozenset(keyset) if keyset is not None else None
        self._connected_override = (
            frozenset(_normalize(s) for s in connected_apps)
            if connected_apps is not None
            else None
        )
        self._manifest_override = native_manifests
        self._keyset_cache: Optional[frozenset[str]] = None
        self._connected_cache: Optional[frozenset[str]] = None
        self._manifest_cache: Optional[Dict[str, Dict[str, Any]]] = None

    # ------------------------------------------------------------------ #
    # Authorities                                                        #
    # ------------------------------------------------------------------ #
    def keyset(self) -> frozenset[str]:
        """Secret names present for this assistant."""
        if self._keyset_override is not None:
            return self._keyset_override
        if self._keyset_cache is None:
            from ..integration_status import _read_local_secret_keyset

            self._keyset_cache = frozenset(_read_local_secret_keyset())
        return self._keyset_cache

    def connected_apps(self) -> frozenset[str]:
        """Provider-backed apps holding a live connection."""
        if self._connected_override is not None:
            return self._connected_override
        if self._connected_cache is None:
            self._connected_cache = frozenset(self._read_connected_apps())
        return self._connected_cache

    @staticmethod
    def _read_connected_apps() -> List[str]:
        try:
            from ..integrations import ops as integration_ops
            from ..integrations.primitives import (
                integration_owner_scope_from_session,
            )

            scope = dict(integration_owner_scope_from_session() or {})
            scope.setdefault("owner_scope", "assistant")
            rows = integration_ops.list_connections(**scope)
        except Exception:
            logger.debug("Could not list integration connections", exc_info=True)
            return []

        if isinstance(rows, dict):
            rows = rows.get("connections") or rows.get("info") or []
        connected: List[str] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            if row.get("status") != "connected":
                continue
            slug = row.get("canonical_app_slug") or row.get("app_slug")
            if isinstance(slug, str) and slug.strip():
                connected.append(_normalize(slug))
        return connected

    def native_manifest(self, slug: str) -> Optional[Dict[str, Any]]:
        """The integration package manifest row for *slug*, if one exists."""
        if self._manifest_override is not None:
            return self._manifest_override.get(_normalize(slug))
        if self._manifest_cache is None:
            self._manifest_cache = self._read_native_manifests()
        return self._manifest_cache.get(_normalize(slug))

    @staticmethod
    def _read_native_manifests() -> Dict[str, Dict[str, Any]]:
        try:
            import unisdk

            active = unisdk.get_active_context() or {}
            root = active.get("read") or active.get("write") or ""
            context = (
                f"{root}/Integrations/Manifests" if root else "Integrations/Manifests"
            )
            rows = unisdk.get_logs(context=context, limit=1000)
        except Exception:
            logger.debug("Could not read integration manifests", exc_info=True)
            return {}

        manifests: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            entries = dict(getattr(row, "entries", None) or {})
            slug = entries.get("slug")
            if isinstance(slug, str) and slug.strip():
                manifests[_normalize(slug)] = entries
        return manifests

    # ------------------------------------------------------------------ #
    # Resolution                                                         #
    # ------------------------------------------------------------------ #
    def resolve(self, requirement: Any) -> Dict[str, Any]:
        """Report one requirement's connection state.

        ``via`` names the authority that settled it, which is what lets a
        caller explain an unmet requirement: a provider-backed app needs
        the gallery's connect flow, a secret-gated one needs a value
        pasted.
        """
        slug = _normalize(requirement.slug)
        report: Dict[str, Any] = {
            "slug": requirement.slug,
            "name": requirement.name or requirement.slug,
        }

        if slug in self.connected_apps():
            report.update({"connected": True, "via": "connection"})
            return report

        keyset = self.keyset()

        # The app's own package is the authority on what connecting means,
        # so a bundle naming a native app need not restate its secrets.
        manifest = self.native_manifest(slug)
        if manifest is not None:
            required = _json_list(manifest.get("required_secrets_json"))
            optional = _json_list(manifest.get("optional_secrets_json"))
            missing = [name for name in required if name not in keyset]
            if required:
                connected = not missing
            elif optional:
                connected = any(name in keyset for name in optional)
                missing = [] if connected else optional
            else:
                # A package gating on nothing is usable once deployed.
                connected = True
                missing = []
            report.update(
                {
                    "connected": connected,
                    "via": "native_package",
                    "missing_secrets": missing,
                },
            )
            return report

        # No package and no connection row: the bundle's own declaration
        # is the only signal left. This is the BYOD OAuth case.
        declared: Sequence[str] = tuple(requirement.required_secrets or ())
        missing = [name for name in declared if name not in keyset]
        report.update(
            {
                "connected": not missing,
                "via": "secret" if declared else "undeclared",
                "missing_secrets": missing,
            },
        )
        return report

    def report(self, requirements: Iterable[Any]) -> List[Dict[str, Any]]:
        return [self.resolve(requirement) for requirement in requirements]

    def unmet(self, requirements: Iterable[Any]) -> List[Dict[str, Any]]:
        return [
            entry for entry in self.report(requirements) if not entry.get("connected")
        ]
