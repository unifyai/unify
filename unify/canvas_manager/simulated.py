"""In-memory CanvasManager.

Mirrors the full public contract without Orchestra, a node toolchain or a
browser, so tests can exercise the manager's own logic — validation, partial
updates, action resolution, invocation bookkeeping — at speed.

The authoring gates that do not need infrastructure are real here: linting runs
the same function the real implementation uses, and the same binding and action
validation applies. Only compilation and rendering are stubbed, because they are
the parts that genuinely require node and Chromium.
"""

from __future__ import annotations

import functools
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from unify.canvas_manager.base import DEFAULT_VISIBILITY, BaseCanvasManager
from unify.canvas_manager.ops import binding_ops
from unify.canvas_manager.ops.action_ops import coerce_actions, validate_actions
from unify.canvas_manager.ops.build_ops import lint_source
from unify.canvas_manager.types.action import CanvasAction, CanvasInvocationRecord
from unify.canvas_manager.types.binding import PrimitiveBinding
from unify.canvas_manager.types.view import (
    BuildReport,
    CanvasResult,
    CanvasViewRecord,
    ReviewReport,
)

SIMULATED_CONSOLE_URL = "https://simulated-console.example.com"
SIMULATED_KIT_VERSION = "0.1.0-sim"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SimulatedCanvasManager(BaseCanvasManager):
    """In-memory implementation of the CanvasManager contract."""

    def __init__(self) -> None:
        self._views: Dict[str, CanvasViewRecord] = {}
        self._actions: Dict[str, List[CanvasAction]] = {}
        self._invocations: Dict[str, List[CanvasInvocationRecord]] = {}
        self._counter = 0

    # ── helpers ───────────────────────────────────────────────────────────

    def _next_token(self) -> str:
        self._counter += 1
        return f"sim_canvas_{self._counter:04d}"

    def _url(self, token: str) -> str:
        return f"{SIMULATED_CONSOLE_URL}/canvas/view/{token}"

    def _compile(self, tsx: str) -> BuildReport:
        """Run the real lint, then stand in for the node toolchain."""
        problems = lint_source(tsx)
        if problems:
            return BuildReport(ok=False, failed_stage="lint", diagnostics=problems)

        encoded = tsx.encode("utf8")
        return BuildReport(
            ok=True,
            kit_version=SIMULATED_KIT_VERSION,
            bundle_sha=hashlib.sha256(encoded).hexdigest(),
            bytes=len(encoded),
        )

    def _prepare(
        self,
        *,
        bindings: Optional[List[PrimitiveBinding]],
        actions: Optional[List[CanvasAction]],
    ) -> tuple[List[PrimitiveBinding], List[CanvasAction]]:
        coerced = binding_ops.coerce_bindings(bindings)
        binding_ops.check_bindable(coerced)
        resolved = binding_ops.resolve_binding_contexts(
            coerced,
            root_context="Simulated",
        )
        return resolved, validate_actions(coerce_actions(actions))

    # ── authoring ─────────────────────────────────────────────────────────

    @functools.wraps(BaseCanvasManager.create_view, updated=())
    def create_view(
        self,
        tsx: str,
        *,
        title: str,
        description: Optional[str] = None,
        bindings: Optional[List[PrimitiveBinding]] = None,
        props: Optional[Dict[str, Any]] = None,
        actions: Optional[List[CanvasAction]] = None,
        destination: Optional[str] = None,
        visibility: str = DEFAULT_VISIBILITY,
        review: bool = True,
    ) -> CanvasResult:
        build = self._compile(tsx)
        if not build.ok:
            return CanvasResult(title=title, build=build, error="Canvas build failed.")

        try:
            resolved_bindings, resolved_actions = self._prepare(
                bindings=bindings,
                actions=actions,
            )
        except ValueError as error:
            return CanvasResult(title=title, error=str(error))

        token = self._next_token()
        now = _now()
        record = CanvasViewRecord(
            canvas_id=self._counter,
            token=token,
            title=title,
            description=description,
            tsx_source=tsx,
            bundle_sha=build.bundle_sha,
            bundle_uri=f"memory://{build.bundle_sha}",
            kit_version=build.kit_version,
            bindings_json=binding_ops.serialize_bindings(resolved_bindings),
            binding_contexts=binding_ops.binding_contexts(resolved_bindings),
            props_json=json.dumps(props or {}),
            visibility=visibility,
            status="published",
            build_json=build.model_dump_json(),
            created_at=now,
            updated_at=now,
        )

        self._views[token] = record
        self._actions[token] = resolved_actions
        self._invocations.setdefault(token, [])

        return CanvasResult(
            token=token,
            url=self._url(token),
            title=title,
            status="published",
            build=build,
            review=ReviewReport(rendered=True, verdict="ok") if review else None,
        )

    @functools.wraps(BaseCanvasManager.update_view, updated=())
    def update_view(
        self,
        token: str,
        *,
        tsx: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        bindings: Optional[List[PrimitiveBinding]] = None,
        props: Optional[Dict[str, Any]] = None,
        actions: Optional[List[CanvasAction]] = None,
        visibility: Optional[str] = None,
        review: bool = True,
    ) -> CanvasResult:
        record = self._views.get(token)
        if record is None:
            return CanvasResult(token=token, error=f"Canvas {token!r} not found.")

        build: Optional[BuildReport] = None
        if tsx is not None:
            build = self._compile(tsx)
            if not build.ok:
                # The stored canvas is deliberately left as it was: a failed
                # revision must not take down a working view.
                return CanvasResult(
                    token=token,
                    url=self._url(token),
                    title=record.title,
                    status=record.status,
                    build=build,
                    error="Canvas build failed; the published version is unchanged.",
                )

        updates: Dict[str, Any] = {"updated_at": _now()}
        if tsx is not None and build is not None:
            updates.update(
                tsx_source=tsx,
                bundle_sha=build.bundle_sha,
                bundle_uri=f"memory://{build.bundle_sha}",
                kit_version=build.kit_version,
                build_json=build.model_dump_json(),
            )
        if title is not None:
            updates["title"] = title
        if description is not None:
            updates["description"] = description
        if visibility is not None:
            updates["visibility"] = visibility
        if props is not None:
            updates["props_json"] = json.dumps(props)

        if bindings is not None:
            try:
                resolved = binding_ops.resolve_binding_contexts(
                    binding_ops.coerce_bindings(bindings),
                    root_context="Simulated",
                )
            except ValueError as error:
                return CanvasResult(token=token, error=str(error))
            updates["bindings_json"] = binding_ops.serialize_bindings(resolved)
            updates["binding_contexts"] = binding_ops.binding_contexts(resolved)

        if actions is not None:
            try:
                self._actions[token] = validate_actions(coerce_actions(actions))
            except ValueError as error:
                return CanvasResult(token=token, error=str(error))

        self._views[token] = record.model_copy(update=updates)

        return CanvasResult(
            token=token,
            url=self._url(token),
            title=self._views[token].title,
            status=self._views[token].status,
            build=build,
            review=(
                ReviewReport(rendered=True, verdict="ok")
                if (review and tsx is not None)
                else None
            ),
        )

    @functools.wraps(BaseCanvasManager.refresh_props, updated=())
    def refresh_props(self, token: str, *, props: Dict[str, Any]) -> CanvasResult:
        record = self._views.get(token)
        if record is None:
            return CanvasResult(token=token, error=f"Canvas {token!r} not found.")

        self._views[token] = record.model_copy(
            update={"props_json": json.dumps(props), "updated_at": _now()},
        )
        return CanvasResult(
            token=token,
            url=self._url(token),
            title=record.title,
            status=record.status,
        )

    # ── retrieval ─────────────────────────────────────────────────────────

    @functools.wraps(BaseCanvasManager.get_view, updated=())
    def get_view(self, token: str) -> Optional[CanvasViewRecord]:
        return self._views.get(token)

    @functools.wraps(BaseCanvasManager.list_views, updated=())
    def list_views(
        self,
        *,
        filter: Optional[str] = None,
        limit: int = 50,
    ) -> List[CanvasViewRecord]:
        records = list(self._views.values())
        if filter:
            # Substring match is enough to exercise callers; the real
            # implementation pushes the predicate into the backend.
            needle = filter.strip("'\" ").lower()
            records = [
                record
                for record in records
                if needle in (record.title or "").lower()
                or needle in (record.description or "").lower()
            ]
        return [
            record.model_copy(update={"tsx_source": ""}) for record in records[:limit]
        ]

    @functools.wraps(BaseCanvasManager.delete_view, updated=())
    def delete_view(self, token: str, *, destination: Optional[str] = None) -> bool:
        self._views.pop(token, None)
        self._actions.pop(token, None)
        self._invocations.pop(token, None)
        return True

    # ── inspection ────────────────────────────────────────────────────────

    @functools.wraps(BaseCanvasManager.preview, updated=())
    def preview(self, token: str) -> ReviewReport:
        if token not in self._views:
            return ReviewReport(rendered=False, error=f"Canvas {token!r} not found.")
        return ReviewReport(rendered=True, verdict="ok")

    @functools.wraps(BaseCanvasManager.list_invocations, updated=())
    def list_invocations(
        self,
        token: str,
        *,
        limit: int = 20,
    ) -> List[CanvasInvocationRecord]:
        return list(reversed(self._invocations.get(token, [])))[:limit]

    # ── test affordances ──────────────────────────────────────────────────

    def record_invocation(
        self,
        token: str,
        *,
        action_name: str,
        args: Optional[Dict[str, Any]] = None,
        status: str = "pending",
    ) -> CanvasInvocationRecord:
        """Append an invocation. Present so callers can exercise the read path."""
        entries = self._invocations.setdefault(token, [])
        record = CanvasInvocationRecord(
            invocation_id=len(entries) + 1,
            canvas_token=token,
            action_name=action_name,
            args_json=json.dumps(args or {}),
            status=status,
            run_key=hashlib.sha256(
                f"{token}:{action_name}:{json.dumps(args or {}, sort_keys=True)}".encode(
                    "utf8",
                ),
            ).hexdigest()[:16],
            created_at=_now(),
        )
        entries.append(record)
        return record

    def declared_actions(self, token: str) -> List[CanvasAction]:
        """Actions stored against a canvas."""
        return list(self._actions.get(token, []))

    def clear(self) -> None:
        """Drop all state."""
        self._views.clear()
        self._actions.clear()
        self._invocations.clear()
        self._counter = 0
