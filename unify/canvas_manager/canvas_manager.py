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
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, List, Optional

from unify.canvas_manager.base import DEFAULT_VISIBILITY, BaseCanvasManager
from unify.canvas_manager.ops import binding_ops, build_ops, token_ops
from unify.canvas_manager.ops.binding_ops import BindingError
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

# Age past which a `running` invocation's claim is presumed dead and may be
# taken over. Long enough that a slow bulk action is not stolen mid-send;
# short enough that a crashed runner does not wedge the run for a day.
STALE_CLAIM_SECONDS = 900


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
        rows: Optional[Dict[str, Any]] = None,
    ) -> ReviewReport:
        """Render the compiled canvas and look at it.

        Skipped when disabled or when no browser is available. A render failure
        is a publication gate, because a canvas that compiles and then throws on
        mount is the one class of fault the compiler cannot see.

        ``rows`` are the dry-run samples from binding verification, replayed in
        place of the parent so the review exercises the shape the canvas will
        actually receive.
        """
        if not self._settings.REVIEW_ENABLED:
            return ReviewReport(rendered=True, verdict="skipped")

        from unify.canvas_manager.ops import review_ops

        return review_ops.render_and_review(
            token=token,
            bundle=bundle,
            props=props,
            rows=rows,
        )

    def _prepare_bindings(
        self,
        bindings: Optional[List[PrimitiveBinding]],
        *,
        root_context: str,
    ) -> tuple[List[PrimitiveBinding], Dict[str, Any]]:
        """Resolve, authorise and dry-run the bindings.

        Returns the resolved bindings and the sample rows the dry-run produced,
        which the render gate replays so the canvas is reviewed against its own
        real column names rather than against nothing.
        """
        coerced = binding_ops.coerce_bindings(bindings)
        if not coerced:
            return [], {}

        binding_ops.check_bindable(coerced)
        # Bindings resolve against the root the canvas itself lives under, so a
        # team canvas reads that team's data rather than the author's.
        resolved = binding_ops.resolve_binding_contexts(
            coerced,
            root_context=root_context,
        )
        samples = binding_ops.verify_bindings(resolved, data_manager=self._get_dm())
        return resolved, samples

    def _sample_stored_bindings(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Re-run the bindings already stored on a canvas, for their samples.

        Used when revising the source without touching the bindings. A binding
        that has since broken does not fail the revision: the gate is judging the
        new source, and refusing to publish it because an unrelated table went
        missing would leave the author unable to fix anything.
        """
        stored = binding_ops.deserialize_bindings(row.get("bindings_json"))
        if not stored:
            return {}

        try:
            return binding_ops.verify_bindings(stored, data_manager=self._get_dm())
        except BindingError as error:
            logger.warning("canvas preview data unavailable: %s", error)
            return {}

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
            resolved_bindings, samples = self._prepare_bindings(
                bindings,
                root_context=root_context,
            )
            resolved_actions = validate_actions(coerce_actions(actions))
        except ValueError as error:
            return CanvasResult(title=title, build=build, error=str(error))

        token = token_ops.generate_token()

        review_report = (
            self._review(
                token=token,
                bundle=bundle,
                props=props or {},
                rows=samples,
            )
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

        # Registered before the row exists, so a token that turns out to belong
        # to another canvas can be replaced with a fresh one instead of leaving
        # a stored canvas behind a URL that resolves to somebody else's view.
        registration = "collision"
        for _ in range(3):
            registration = token_ops.register_token(
                token,
                context_name=views_context,
                project_name=token_ops.active_project(),
                visibility=visibility,
            )
            if registration != "collision":
                break
            token = token_ops.generate_token()
        if registration == "collision":
            return CanvasResult(
                title=title,
                build=build,
                review=review_report,
                error=(
                    "A canvas URL could not be allocated after several "
                    "attempts; nothing was stored. Try again."
                ),
            )
        warning = (
            "The canvas was stored, but its URL could not be registered with "
            "the backend, so the link will not resolve yet. Publishing it "
            "again once the backend is reachable will register it."
            if registration == "unreachable"
            else None
        )

        now = _now()
        row = CanvasViewRow(
            token=token,
            title=title,
            description=description,
            tsx_source=tsx,
            bundle_sha=build.bundle_sha,
            bundle_code=bundle,
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

        self._announce(token, title, "published")

        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=title,
            status="published",
            build=build,
            review=review_report,
            warning=warning,
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
        root_context = self._root_of(context, VIEWS_TABLE)

        # Bindings are resolved before the review, because the render gate
        # replays their dry-run samples. When only the source is being revised,
        # the stored bindings are re-run for the same reason.
        resolved: Optional[List[PrimitiveBinding]] = None
        samples: Dict[str, Any] = {}
        if bindings is not None:
            try:
                resolved, samples = self._prepare_bindings(
                    bindings,
                    root_context=root_context,
                )
            except ValueError as error:
                return CanvasResult(token=token, error=str(error))
        elif tsx is not None and review:
            samples = self._sample_stored_bindings(existing)

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
                    rows=samples,
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
                bundle_code=bundle,
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

        if resolved is not None:
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

        if visibility is not None:
            # The routing row carries visibility too, and it is the copy console
            # actually reads when deciding whether to serve. Updating only the
            # canvas row would leave a canvas that looks shared here and stays
            # private to every viewer.
            token_ops.set_token_state(token, visibility=visibility)

        title = updates.get("title", existing.get("title", ""))
        status = existing.get("status", "published")
        self._announce(token, title, status)

        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=title,
            status=status,
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

        title = existing.get("title", "")
        status = existing.get("status", "published")
        # Materialised props are the values a canvas over LLM-shaped or expensive
        # reads displays, so a refresh changes what a viewer sees just as much as a
        # code revision does.
        self._announce(token, title, status)

        return CanvasResult(
            token=token,
            url=token_ops.build_canvas_url(token),
            title=title,
            status=status,
        )

    def _announce(self, token: str, title: str, status: str) -> None:
        """Tell open frames a canvas changed.

        A canvas is typically left open in a tab, so a revision that only landed on
        reload would leave a viewer reading a superseded view with nothing to
        indicate it. Imported here rather than at module scope because the publisher
        lives in the conversation manager, which imports managers in turn.

        Carries no bundle. Surfaces re-read the record, so the integrity check stays
        on the one path that performs it.
        """
        from unify.conversation_manager.domains.comms_utils import (
            publish_canvas_updated,
        )

        publish_canvas_updated(token=token, title=title, status=status)

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

        # Announced as `deleted` so a surface can drop the canvas instead of
        # re-reading a token that will now never resolve.
        self._announce(token, row.get("title", ""), "deleted")
        return True

    # ── inspection ────────────────────────────────────────────────────────

    def preview(self, token: str) -> ReviewReport:
        record = self.get_view(token)
        if record is None:
            return ReviewReport(rendered=False, error=f"Canvas {token!r} not found.")

        if not record.bundle_code:
            return ReviewReport(
                rendered=False,
                error=f"Canvas {token!r} has no compiled bundle to render.",
            )

        row, _ = self._find_row(VIEWS_TABLE, token)
        return self._review(
            token=token,
            bundle=record.bundle_code,
            props=json.loads(record.props_json or "{}"),
            rows=self._sample_stored_bindings(row or {}),
        )

    def run_invocation(
        self,
        invocation_id: int,
        *,
        token: str,
    ) -> CanvasInvocationRecord:
        row, context = self._find_invocation(token, invocation_id)
        if row is None:
            raise ValueError(
                f"Canvas {token!r} has no invocation {invocation_id}.",
            )

        record = CanvasInvocationRecord.model_validate(row)

        # Already done. A redelivered event or a manual retry of a completed
        # send must not send again; returning what happened is the honest answer.
        if record.status == "succeeded":
            return record

        if record.status == "running" and not self._claim_is_stale(record):
            # Someone is executing it right now; their result will land on the
            # row. Reporting the current state is all a second delivery may do.
            return record

        claimed = self._claim_invocation(context, record)
        if claimed is None:
            # Lost the race to another delivery -- the winner runs it.
            fresh, _ = self._find_invocation(token, invocation_id)
            return CanvasInvocationRecord.model_validate(fresh or row)

        action = self._find_action(token, record.action_name)
        if action is None:
            # The canvas was revised and no longer declares this action. Recording
            # that is better than executing a target the current canvas disowns.
            return self._settle_invocation(
                context,
                invocation_id,
                status="failed",
                error=(
                    f"Canvas {token!r} no longer declares an action named "
                    f"{record.action_name!r}."
                ),
            )

        args = json.loads(record.args_json or "{}")
        try:
            result = self._dispatch_action(action, args)
        except Exception as error:  # noqa: BLE001 - reported to the viewer verbatim
            logger.exception("Canvas invocation %s failed", invocation_id)
            return self._settle_invocation(
                context,
                invocation_id,
                status="failed",
                error=str(error)[:2000],
            )

        return self._settle_invocation(
            context,
            invocation_id,
            status="succeeded",
            result=result,
        )

    def _claim_is_stale(self, record: CanvasInvocationRecord) -> bool:
        """Whether a `running` claim's holder can be presumed dead.

        A run whose claim is younger than the window is someone else's live
        work. Past the window with no terminal state written, the holder died
        mid-run -- the one condition under which taking the run over does not
        risk executing it twice, because the original can no longer finish.
        """
        if not record.claimed_at:
            # A running row with no claim predates claims; age is unknowable,
            # so treat it as stale rather than stuck forever.
            return True
        claimed = datetime.fromisoformat(record.claimed_at)
        age = (datetime.now(timezone.utc) - claimed).total_seconds()
        return age >= STALE_CLAIM_SECONDS

    def _claim_invocation(
        self,
        context: str,
        record: CanvasInvocationRecord,
    ) -> Optional[str]:
        """Take exclusive execution of one run, or None when another won.

        An atomic compare-and-set on the state this caller just observed, so
        two deliveries racing produce exactly one winner. This is what keeps
        at-least-once event delivery from sending an email twice. Expecting
        the observed ``claim_key`` on a stale takeover fences out the dead
        holder: if it somehow wrote again in between, the expectation no
        longer matches and nobody double-runs.
        """
        import uuid

        nonce = uuid.uuid4().hex
        expect: Dict[str, Any] = {
            "canvas_token": record.canvas_token,
            "invocation_id": record.invocation_id,
            "status": record.status,
        }
        if record.claim_key:
            expect["claim_key"] = record.claim_key

        claimed = self._get_dm().claim(
            context,
            expect=expect,
            updates={"status": "running", "claim_key": nonce, "claimed_at": _now()},
            limit=1,
        )
        return nonce if claimed else None

    def _find_invocation(
        self,
        token: str,
        invocation_id: int,
    ) -> tuple[Optional[Dict[str, Any]], str]:
        """Locate one invocation row, scoped to its canvas.

        Scoped because invocation ids are sequential per context: without the
        token in the filter, an id from one canvas would address another's run.
        """
        dm = self._get_dm()
        for context in self._read_tables(INVOCATIONS_TABLE):
            rows = dm.filter(
                context,
                filter=(
                    f"canvas_token == '{token}' and invocation_id == {invocation_id}"
                ),
                limit=1,
            )
            if rows:
                return rows[0], context
        return None, ""

    def _find_action(self, token: str, action_name: str) -> Optional[Dict[str, Any]]:
        """The declared action for one name, or None if the canvas dropped it."""
        dm = self._get_dm()
        for context in self._read_tables(ACTIONS_TABLE):
            rows = dm.filter(
                context,
                filter=(
                    f"canvas_token == '{token}' and action_name == '{action_name}'"
                ),
                limit=1,
            )
            if rows:
                return rows[0]
        return None

    def _settle_invocation(
        self,
        context: str,
        invocation_id: int,
        *,
        status: str,
        result: Optional[Any] = None,
        error: Optional[str] = None,
    ) -> CanvasInvocationRecord:
        """Record a terminal outcome and read the row back."""
        updates: Dict[str, Any] = {"status": status, "finished_at": _now()}
        if result is not None:
            updates["result_json"] = json.dumps(result, default=str)[:20000]
        if error is not None:
            updates["error"] = error

        dm = self._get_dm()
        dm.update_rows(context, updates, filter=f"invocation_id == {invocation_id}")

        rows = dm.filter(context, filter=f"invocation_id == {invocation_id}", limit=1)
        return CanvasInvocationRecord.model_validate(rows[0])

    def _dispatch_action(self, action: Dict[str, Any], args: Dict[str, Any]) -> Any:
        """Run the declared target in whichever lane the action names.

        All three lanes live here rather than in Orchestra because all three are
        things only the assistant can do: it owns the function catalogue, the task
        scheduler and the actor. Orchestra's job ended when it recorded the run.
        """
        kind = str(action.get("kind") or "function")

        if kind == "function":
            return self._run_function(action, args)

        if kind == "task":
            task_id = action.get("task_id")
            if task_id is None:
                raise ValueError("This action declares no task to trigger.")
            from unify.task_scheduler.typed_tasks_client import trigger_task

            # Never `update` to start work, per the TaskScheduler contract. The
            # trigger route takes no payload, which is why the arguments live on
            # the invocation row for the task itself to read.
            return trigger_task(task_id=int(task_id))

        if kind == "assistant":
            request = action.get("request")
            if not request:
                raise ValueError("This action declares no request to hand over.")
            return self._ask_assistant(str(request), args)

        raise ValueError(f"Unknown action kind {kind!r}.")

    def _run_function(self, action: Dict[str, Any], args: Dict[str, Any]) -> Any:
        """Execute the stored function this action names."""
        import asyncio

        fm = self._get_fm()
        function_id = action.get("function_id")
        if function_id is None:
            raise ValueError("This action declares no function to run.")

        rows = fm.filter_functions(filter=f"function_id == {int(function_id)}", limit=1)
        if not rows:
            raise ValueError(f"Function {function_id} no longer exists.")
        name = rows[0].get("name") or rows[0].get("function_name")
        if not name:
            raise ValueError(f"Function {function_id} has no name to call.")

        # Runs on its own thread for the same reason the render gate does: this is
        # reached from both plain sync code and `asyncio.to_thread`, and
        # `asyncio.run` refuses a thread that already has a loop.
        with ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="canvas-action",
        ) as pool:
            return pool.submit(
                lambda: asyncio.run(
                    fm.execute_function(function_name=str(name), call_kwargs=args),
                ),
            ).result()

    def _ask_assistant(self, request: str, args: Dict[str, Any]) -> Any:
        """Hand a request to the actor, with the viewer's arguments attached.

        The escape hatch for when no stored function fits. The request template is
        authored, the arguments are the viewer's, and the actor decides how to
        satisfy it.
        """
        import asyncio

        from unify.manager_registry import ManagerRegistry

        actor = ManagerRegistry.get_actor()
        prompt = (
            f"{request}\n\n"
            f"A viewer triggered this from a canvas with these arguments:\n"
            f"{json.dumps(args, indent=2, default=str)}"
        )

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="canvas-ask") as pool:
            return pool.submit(lambda: asyncio.run(actor.act(prompt))).result()

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
    "run_invocation",
    "list_invocations",
):
    _base_method = getattr(BaseCanvasManager, _name)
    getattr(CanvasManager, _name).__doc__ = _base_method.__doc__
