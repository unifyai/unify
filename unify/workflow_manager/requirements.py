"""Resolving whether a workflow's declared requirements are satisfied.

A bundle names what it needs and stops there. What "connected" means differs
per app, can change without the workflow changing, and an app may offer more
than one route at once — so resolution is here, and it consults each authority
in a deliberate order.

**Provider-backed comes first.** That covers third-party gallery apps *and* the
native integration packages authored in unify-deploy, and either may be OAuth,
an API key, or both. All of them connect through the integrations layer, so a
live connection row is the answer, and the absence of one means "press
Connect" — not "paste a token".

**Workspace is separate and distinct.** It is not in the gallery, not a
package, and it is connected in the onboarding and profile flows rather than
the integrations gallery. A requirement declares ``kind: workspace`` for it,
and the refresh-token secret those flows store is its signal.

**A requirement may name several interchangeable apps.** What a workflow
needs is a capability — somewhere to post, a calendar to read — and which app
provides it is the user's choice. Every declared option is resolved and the
first connected one settles the requirement; the rest travel with the answer
so a reader can offer the app the user already has.

**Absence of evidence is "not connected".** A named requirement is by
definition something that needs connecting, so nothing having answered means
unmet. The older default treated it as met, which armed jobs against apps
nobody had connected. A need with nothing to connect — browsing, a filesystem —
is not a requirement at all: it belongs in the bundle's ``capabilities``.

Deliberately no read of the gallery catalogue. Knowing whether a slug is
*offered* would only distinguish a connectable app from a non-connectable
capability, which the ``capabilities`` field already does — and a resolver is
constructed per call, so that read would cost one full catalogue scan per bundle
per listing. Catching a slug the gallery does not offer is an authoring-time
check, and it lives in the authoring rule and the CI gate.

Each authority is read at most once per instance and is best-effort: one that
cannot be reached is silent rather than a denial, because holding a workflow's
jobs on a transient read failure is worse than arming them a session early.
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

        A requirement that offers alternatives is met by any one of them,
        so each is resolved in recommendation order and the first
        connected answer wins. ``options`` carries all of them with their
        own state, which is what lets a reader offer the app the user
        already has rather than the one the bundle happened to name first.
        """
        options = getattr(requirement, "options", None) or ()
        if len(options) > 1:
            reports = [
                self._resolve_app(option.slug, option.display_name, requirement)
                for option in options
            ]
            satisfied = next(
                (entry for entry in reports if entry.get("connected")),
                None,
            )
            answer = dict(satisfied or reports[0])
            answer["options"] = reports
            # The requirement keeps the identity the bundle declared even
            # when a different app satisfied it: that is what a reader
            # matches its own resolution against, and what an uninstall or
            # a re-read looks up.
            answer["slug"] = requirement.slug
            answer["name"] = requirement.name or requirement.slug
            if satisfied is not None:
                answer["satisfied_by"] = satisfied["slug"]
            return answer

        return self._resolve_app(
            requirement.slug,
            requirement.name or requirement.slug,
            requirement,
        )

    def _resolve_app(
        self,
        app_slug: str,
        display_name: str,
        requirement: Any,
    ) -> Dict[str, Any]:
        """Resolve one app, consulting each authority in turn."""
        slug = _normalize(app_slug)
        report: Dict[str, Any] = {
            "slug": app_slug,
            "name": display_name,
        }

        # Workspace first, because it is not an integration at all: not in the
        # gallery, not a package, and connected in the onboarding and profile
        # flows. Its signal here is the refresh-token secret those flows store.
        if requirement.kind == "workspace":
            declared: Sequence[str] = tuple(requirement.required_secrets or ())
            missing = [name for name in declared if name not in self.keyset()]
            report.update(
                {
                    "connected": not missing,
                    "via": "workspace",
                    "missing_secrets": missing,
                },
            )
            return report

        # Then a live provider-backed connection, which covers both third-party
        # gallery apps and the native packages we author — either may be OAuth,
        # an API key, or both, and all of them connect the same way.
        if slug in self.connected_apps():
            report.update({"connected": True, "via": "connection"})
            return report

        # A native package that is genuinely secret-gated rather than
        # connectable answers for itself: its own manifest names the secrets
        # that make it usable, so a bundle never restates them.
        manifest = self.native_manifest(slug)
        if manifest is not None:
            required = _json_list(manifest.get("required_secrets_json"))
            optional = _json_list(manifest.get("optional_secrets_json"))
            keyset = self.keyset()
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

        # Nothing has answered, and a named requirement is by definition an app
        # that needs connecting — so absence of evidence is "not connected", not
        # "met". The old default read as met, which armed jobs against apps
        # nobody had connected. A need with nothing to connect is not a
        # requirement at all; it belongs in the bundle's `capabilities`.
        report.update({"connected": False, "via": "connection"})
        return report

    def report(self, requirements: Iterable[Any]) -> List[Dict[str, Any]]:
        return [self.resolve(requirement) for requirement in requirements]

    def unmet(self, requirements: Iterable[Any]) -> List[Dict[str, Any]]:
        return [
            entry for entry in self.report(requirements) if not entry.get("connected")
        ]
