"""Concrete CanvasManager backed by Unify contexts and Orchestra tokens.

Storage mirrors the other catalogue managers: rows live in contexts this manager
declares, writes resolve to one destination root and reads fan out across every
root the assistant can see. All row I/O goes through DataManager rather than
unisdk directly, so destination routing, type coercion and retry behaviour are
inherited rather than reimplemented.

What is specific to canvases is the authoring pipeline. Nothing is stored until
the source lints, typechecks, compiles, its bindings resolve and dry-run, and its
actions resolve to targets that exist. A canvas that would fail in front of a
viewer fails here instead, with the compiler's own diagnostics.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, List, Optional

from unify.canvas_manager.base import DEFAULT_VISIBILITY, BaseCanvasManager
from unify.canvas_manager.ops import binding_ops, build_ops, token_ops
from unify.canvas_manager.ops.action_ops import (
    coerce_actions,
    resolve_function_id,
    serialize_input_schema,
    validate_actions,
)
from unify.canvas_manager.settings import CanvasSettings
from unify.canvas_manager.types.action import (
    CanvasAction,
    CanvasActionRow,
    CanvasInvocationRecord,
    CanvasInvocationRow,
)
from unify.canvas_manager.types.binding import PrimitiveBinding
from unify.canvas_manager.types.view import (
    BuildReport,
    CanvasResult,
    CanvasViewRecord,
    CanvasViewRow,
    ReviewReport,
)
from unify.common.context_registry import ContextRegistry, TableContext
from unify.common.model_to_fields import model_to_fields

logger = logging.getLogger(__name__)

VIEWS_TABLE = "Canvas/Views"
ACTIONS_TABLE = "Canvas/Actions"
INVOCATIONS_TABLE = "Canvas/Invocations"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_canvas_context(context: Optional[str], suffix: str) -> str:
    """Guard against a canvas table resolving outside its own namespace.

    Cheap to check and worth checking: a misresolved root would silently write
    canvas rows into another manager's namespace, where nothing would look for
    them and a later read would appear to lose data.
    """
    if not context:
        raise RuntimeError(f"Canvas context {suffix!r} could not be resolved")
    expected = f"Canvas/{suffix}"
    if context == expected or "/" not in context:
        raise RuntimeError(
            f"Canvas context {suffix!r} is not fully qualified: {context}",
        )
    if not context.endswith(f"/{expected}"):
        raise RuntimeError(
            f"Canvas context {suffix!r} resolved outside the Canvas namespace: {context}",
        )
    return context


class CanvasManager(BaseCanvasManager):
    """CanvasManager over Unify contexts, DataManager and the canvas toolchain."""

    class Config:
        """Context registration for the Canvas namespace."""

        required_contexts = [
            TableContext(
                name=VIEWS_TABLE,
                description=(
                    "Registry of canvases. Each row holds the authored TSX, the "
                    "content address of its compiled bundle, its resolved data "
                    "bindings and its lifecycle state."
                ),
                fields=model_to_fields(CanvasViewRow),
                unique_keys={"canvas_id": "int"},
                auto_counting={"canvas_id": None},
            ),
            TableContext(
                name=ACTIONS_TABLE,
                description=(
                    "Actions a viewer may trigger from a canvas. Each row wires a "
                    "Functions-catalogue id or task to a control, with the input "
                    "schema its arguments are validated against."
                ),
                fields=model_to_fields(CanvasActionRow),
                unique_keys={"action_id": "int"},
                auto_counting={"action_id": None},
            ),
            TableContext(
                name=INVOCATIONS_TABLE,
                description=(
                    "One row per action run: the arguments supplied, status, "
                    "result and timing. Carries the arguments that neither "
                    "dispatch lane can take directly, and doubles as the audit "
                    "trail of who triggered what."
                ),
                fields=model_to_fields(CanvasInvocationRow),
                unique_keys={"invocation_id": "int"},
                auto_counting={"invocation_id": None},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._settings = CanvasSettings()
        self._destination_lock = RLock()
        logger.debug("CanvasManager initialized")

    # ── plumbing ──────────────────────────────────────────────────────────

    def _get_dm(self):
        # Resolved per call rather than held, to avoid an import cycle at
        # construction time. Matches the other catalogue managers.
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_data_manager()

    def _get_fm(self):
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_function_manager()

    def _table_for_root(self, root_context: str, table: str) -> str:
        suffix = table.rsplit("/", 1)[-1]
        return _require_canvas_context(f"{root_context.strip('/')}/{table}", suffix)

    def _write_table(self, table: str, destination: str | None) -> str:
        """Concrete context a write goes to."""
        root = ContextRegistry.write_root(self, table, destination=destination)
        return self._table_for_root(root, table)

    def _read_tables(self, table: str) -> List[str]:
        """Every context a read should consider, personal first then each team."""
        return [
            self._table_for_root(root, table)
            for root in ContextRegistry.read_roots(self, table)
        ]

    def _find_row(self, table: str, token: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Locate a canvas row by token, returning it and the context holding it."""
        dm = self._get_dm()
        for context in self._read_tables(table):
            rows = dm.filter(context, filter=f"token == '{token}'", limit=1)
            if rows:
                return rows[0], context
        return None, ""

    @staticmethod
    def _root_of(context: str, table: str) -> str:
        """Root a table context hangs off.

        A revision keeps whichever root the canvas was written to, and that root
        is recoverable from the context the row was found in, so nothing has to
        be re-resolved from a destination that may no longer be supplied.
        """
        suffix = f"/{table}"
        if not context.endswith(suffix):
            raise RuntimeError(f"Context {context!r} is not a {table!r} context")
        return context[: -len(suffix)]

    # ── authoring ─────────────────────────────────────────────────────────

    def _build(self, tsx: str) -> tuple[BuildReport, str]:
        return build_ops.build_canvas(tsx, kit_version=self._settings.KIT_VERSION)

    def _review(
        self,
        *,
        token: str,
        bundle: str,
        props: Dict[str, Any],
    ) -> ReviewReport:
        """Render the compiled canvas and look at it.

        Skipped when disabled or when no browser is available. A render failure
        is a publication gate, because a canvas that compiles and then throws on
        mount is the one class of fault the compiler cannot see.
        """
        if not self._settings.REVIEW_ENABLED:
            return ReviewReport(rendered=True, verdict="skipped")

        from unify.canvas_manager.ops import review_ops

        return review_ops.render_and_review(token=token, bundle=bundle, props=props)

    def _prepare_bindings(
        self,
        bindings: Optional[List[PrimitiveBinding]],
        *,
        root_context: str,
    ) -> List[PrimitiveBinding]:
        coerced = binding_ops.coerce_bindings(bindings)
        if not coerced:
            return []

        binding_ops.check_bindable(coerced)
        # Bindings resolve against the root the canvas itself lives under, so a
        # team canvas reads that team's data rather than the author's.
        resolved = binding_ops.resolve_binding_contexts(
            coerced,
            root_context=root_context,
        )
        binding_ops.verify_bindings(resolved, data_manager=self._get_dm())
        return resolved

    def _store_actions(
        self,
        token: str,
        actions: List[CanvasAction],
        *,
        context: str,
    ) -> None:
        """Replace the action set for one canvas.

        Delete-then-insert rather than a diff: the action set is small, and a
        partial update risks leaving a control wired to a target the current
        canvas no longer declares.
        """
        dm = self._get_dm()
        dm.delete_rows(context, filter=f"canvas_token == '{token}'")
        if not actions:
            return

        fm = self._get_fm()
        rows = []
        for action in actions:
            rows.append(
                CanvasActionRow(
                    canvas_token=token,
                    action_name=action.name,
                    label=action.label,
                    icon=action.icon,
                    kind=action.kind,
                    function_id=resolve_function_id(action, function_manager=fm),
                    task_id=action.task_id,
                    request=action.request,
                    input_schema_json=serialize_input_schema(action),
                    confirm=action.confirm,
                    destructive=action.destructive,
                    result_mode=action.result_mode,
                    max_invocations_per_hour=action.max_invocations_per_hour,
                    created_at=_now(),
                    updated_at=_now(),
                ).model_dump(),
            )
        dm.insert_rows(context, rows)

    # The public methods below inherit their documentation from
    # BaseCanvasManager; see that module for the contract.

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
        build, bundle = self._build(tsx)
        if not build.ok:
            return CanvasResult(title=title, build=build, error="Canvas build failed.")

        views_context = self._write_table(VIEWS_TABLE, destination)
        root_context = self._root_of(views_context, VIEWS_TABLE)

        try:
            resolved_bindings = self._prepare_bindings(
                bindings,
                root_context=root_context,
            )
            resolved_actions = validate_actions(coerce_actions(actions))
        except ValueError as error:
            return CanvasResult(title=title, build=build, error=str(error))

        token = token_ops.generate_token()
        bundle_uri = token_ops.upload_bundle(token, bundle, sha256=build.bundle_sha)

        review_report = (
            self._review(token=token, bundle=bundle, props=props or {})
            if review
            else None
        )
        if review_report is not None and not review_report.rendered:
            # Compiles but does not mount. Publishing this would put a blank
            # frame in front of the user.
            return CanvasResult(
                title=title,
                build=build.model_copy(update={"ok": False, "failed_stage": "render"}),
                review=review_report,
                error=f"Canvas failed to render: {review_report.error}",
            )

        now = _now()
        row = CanvasViewRow(
            token=token,
            title=title,
            description=description,
            tsx_source=tsx,
            bundle_sha=build.bundle_sha,
            bundle_uri=bundle_uri,
            kit_version=build.kit_version,
            bindings_json=binding_ops.serialize_bindings(resolved_bindings),
            binding_contexts=binding_ops.binding_contexts(resolved_bindings),
            props_json=json.dumps(props or {}),
            visibility=visibility,
            status="published",
            preview_image_path=(
                review_report.screenshots[0]
                if review_report and review_report.screenshots
                else None
            ),
            build_json=build.model_dump_json(),
            created_at=now,
            updated_at=now,
        )

        dm = self._get_dm()
        dm.insert_rows(views_context, [row.model_dump()])
        self._store_actions(
            token,
            resolved_actions,
            context=self._write_table(ACTIONS_TABLE, destination),
        )

        token_ops.register_token(
            token,
            context_name=views_context,
            project_name=token_ops.active_project(),
            visibility=visibility,
        )

        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=title,
            status="published",
            build=build,
            review=review_report,
        )

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
        existing, context = self._find_row(VIEWS_TABLE, token)
        if existing is None:
            return CanvasResult(token=token, error=f"Canvas {token!r} not found.")

        build: Optional[BuildReport] = None
        bundle = ""
        review_report: Optional[ReviewReport] = None

        if tsx is not None:
            build, bundle = self._build(tsx)
            if not build.ok:
                # The published version stays exactly as it is. A failed revision
                # must not take down a working canvas.
                return CanvasResult(
                    token=token,
                    url=token_ops.build_canvas_url(token),
                    title=existing.get("title", ""),
                    status=existing.get("status", "published"),
                    build=build,
                    error="Canvas build failed; the published version is unchanged.",
                )

            if review:
                review_report = self._review(
                    token=token,
                    bundle=bundle,
                    props=(
                        props
                        if props is not None
                        else json.loads(existing.get("props_json") or "{}")
                    ),
                )
                if not review_report.rendered:
                    return CanvasResult(
                        token=token,
                        url=token_ops.build_canvas_url(token),
                        title=existing.get("title", ""),
                        status=existing.get("status", "published"),
                        build=build.model_copy(
                            update={"ok": False, "failed_stage": "render"},
                        ),
                        review=review_report,
                        error=f"Canvas failed to render; the published version is unchanged: "
                        f"{review_report.error}",
                    )

        updates: Dict[str, Any] = {"updated_at": _now()}

        if tsx is not None and build is not None:
            updates.update(
                tsx_source=tsx,
                bundle_sha=build.bundle_sha,
                bundle_uri=token_ops.upload_bundle(
                    token,
                    bundle,
                    sha256=build.bundle_sha,
                ),
                kit_version=build.kit_version,
                build_json=build.model_dump_json(),
            )
            if review_report and review_report.screenshots:
                updates["preview_image_path"] = review_report.screenshots[0]

        if title is not None:
            updates["title"] = title
        if description is not None:
            updates["description"] = description
        if visibility is not None:
            updates["visibility"] = visibility
        if props is not None:
            updates["props_json"] = json.dumps(props)

        root_context = self._root_of(context, VIEWS_TABLE)

        if bindings is not None:
            try:
                resolved = self._prepare_bindings(bindings, root_context=root_context)
            except ValueError as error:
                return CanvasResult(token=token, error=str(error))
            updates["bindings_json"] = binding_ops.serialize_bindings(resolved)
            updates["binding_contexts"] = binding_ops.binding_contexts(resolved)

        if actions is not None:
            try:
                self._store_actions(
                    token,
                    validate_actions(coerce_actions(actions)),
                    context=self._table_for_root(root_context, ACTIONS_TABLE),
                )
            except ValueError as error:
                return CanvasResult(token=token, error=str(error))

        self._get_dm().update_rows(context, updates, filter=f"token == '{token}'")

        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=updates.get("title", existing.get("title", "")),
            status=existing.get("status", "published"),
            build=build,
            review=review_report,
        )

    def refresh_props(self, token: str, *, props: Dict[str, Any]) -> CanvasResult:
        existing, context = self._find_row(VIEWS_TABLE, token)
        if existing is None:
            return CanvasResult(token=token, error=f"Canvas {token!r} not found.")

        self._get_dm().update_rows(
            context,
            {"props_json": json.dumps(props), "updated_at": _now()},
            filter=f"token == '{token}'",
        )
        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=existing.get("title", ""),
            status=existing.get("status", "published"),
        )

    # ── retrieval ─────────────────────────────────────────────────────────

    def get_view(self, token: str) -> Optional[CanvasViewRecord]:
        row, _ = self._find_row(VIEWS_TABLE, token)
        return CanvasViewRecord.model_validate(row) if row else None

    def list_views(
        self,
        *,
        filter: Optional[str] = None,
        limit: int = 50,
    ) -> List[CanvasViewRecord]:
        dm = self._get_dm()
        records: List[CanvasViewRecord] = []

        for context in self._read_tables(VIEWS_TABLE):
            if len(records) >= limit:
                break
            rows = dm.filter(
                context,
                filter=filter,
                # Source is the largest column by far and a listing never needs
                # it; excluding it is what keeps discovery cheap.
                exclude_columns=["tsx_source"],
                limit=limit - len(records),
            )
            records.extend(CanvasViewRecord.model_validate(row) for row in rows)

        return records[:limit]

    def delete_view(self, token: str, *, destination: Optional[str] = None) -> bool:
        row, context = self._find_row(VIEWS_TABLE, token)
        if row is None:
            return True

        dm = self._get_dm()
        dm.delete_rows(context, filter=f"token == '{token}'")

        for table in (ACTIONS_TABLE, INVOCATIONS_TABLE):
            for related in self._read_tables(table):
                dm.delete_rows(related, filter=f"canvas_token == '{token}'")

        token_ops.delete_token(token)
        return True

    # ── inspection ────────────────────────────────────────────────────────

    def preview(self, token: str) -> ReviewReport:
        record = self.get_view(token)
        if record is None:
            return ReviewReport(rendered=False, error=f"Canvas {token!r} not found.")

        bundle = token_ops.fetch_bundle(record.bundle_uri)
        if not bundle:
            return ReviewReport(
                rendered=False,
                error=f"Compiled bundle for {token!r} is unavailable at {record.bundle_uri}.",
            )

        return self._review(
            token=token,
            bundle=bundle,
            props=json.loads(record.props_json or "{}"),
        )

    def list_invocations(
        self,
        token: str,
        *,
        limit: int = 20,
    ) -> List[CanvasInvocationRecord]:
        dm = self._get_dm()
        records: List[CanvasInvocationRecord] = []

        for context in self._read_tables(INVOCATIONS_TABLE):
            rows = dm.filter(
                context,
                filter=f"canvas_token == '{token}'",
                order_by="created_at",
                descending=True,
                limit=limit,
            )
            records.extend(CanvasInvocationRecord.model_validate(row) for row in rows)

        records.sort(key=lambda record: record.created_at or "", reverse=True)
        return records[:limit]


# Attach the base contract's documentation to the concrete methods, so the
# caller reads one contract regardless of which implementation is active.
for _name in (
    "create_view",
    "update_view",
    "refresh_props",
    "get_view",
    "list_views",
    "delete_view",
    "preview",
    "list_invocations",
):
    _base_method = getattr(BaseCanvasManager, _name)
    getattr(CanvasManager, _name).__doc__ = _base_method.__doc__
