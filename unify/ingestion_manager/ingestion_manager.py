"""IngestionManager over Unify contexts and the existing ingestion engines.

Storage mirrors the other catalogue managers: runs and their events are rows in
contexts this manager declares, and all row I/O goes through DataManager so
destination routing and retry behaviour are inherited rather than reimplemented.

What is specific here is that a run is recorded **before** any work starts. A
failure is then always something with an id that can be inspected and retried,
rather than an exception that went past. It is also why ``submit`` can return
immediately: the record is the handle, and the work follows it.

Neither ingestion engine is reimplemented. Rows go through the existing chunked
parallel row path; files go through the existing parse pipeline with its per-format
backends. This manager decides *which* engine, *where* the output lands, and records
what happened -- it does not parse or insert anything itself.
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from unify.common.context_registry import ContextRegistry, TableContext
from unify.common.model_to_fields import model_to_fields
from unify.ingestion_manager.base import BaseIngestionManager
from unify.ingestion_manager.policy import choose_tier, next_step, stages_from_events
from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types.request import (
    EmbedSpec,
    IngestionMode,
    IngestionRequest,
    IngestionSource,
    IngestionTarget,
)
from unify.ingestion_manager.types.run import (
    TERMINAL_STATES,
    IngestionEventRow,
    IngestionRun,
    IngestionRunRecord,
    IngestionSummary,
    LogEntry,
    RetryResult,
    RetryScope,
    RunState,
    RunStatus,
)
from unify.data_manager.types.ingest import PostIngestConfig

logger = logging.getLogger(__name__)

RUNS_TABLE = "Ingestion/Runs"
EVENTS_TABLE = "Ingestion/Events"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_key() -> str:
    """Opaque, unguessable handle minted before the row exists.

    Separate from the auto-counted row id so the worker thread has something to
    write events against immediately, without first reading back what id the
    backend assigned.
    """
    return secrets.token_urlsafe(9)


class IngestionManager(BaseIngestionManager):
    """Ingestion over Unify contexts, DataManager and the file parse pipeline."""

    class Config:
        """Context registration for the Ingestion namespace."""

        required_contexts = [
            TableContext(
                name=RUNS_TABLE,
                description=(
                    "One row per ingestion. Records what was asked for, which tier "
                    "ran it, the contexts it wrote and how it ended, so a failure "
                    "is recoverable rather than merely reported."
                ),
                fields=model_to_fields(IngestionRunRecord),
                unique_keys={"run_id": "int"},
                auto_counting={"run_id": None},
            ),
            TableContext(
                name=EVENTS_TABLE,
                description=(
                    "Append-only log of what happened during each run. Stage "
                    "progress is folded from these rows rather than counted "
                    "separately, so there is one source of truth."
                ),
                fields=model_to_fields(IngestionEventRow),
                unique_keys={"event_id": "int"},
                auto_counting={"event_id": None},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._settings = IngestionSettings()
        self._pool = ThreadPoolExecutor(
            max_workers=self._settings.INLINE_WORKERS,
            thread_name_prefix="ingestion",
        )
        self._lock = threading.RLock()
        logger.debug("IngestionManager initialized")

    # ── plumbing ──────────────────────────────────────────────────────────

    def _get_dm(self):
        # Resolved per call rather than held, to avoid an import cycle at
        # construction time. Matches the other catalogue managers.
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_data_manager()

    def _get_fm(self):
        from unify.manager_registry import ManagerRegistry

        return ManagerRegistry.get_file_manager()

    def _write_table(self, table: str, destination: Optional[str]) -> str:
        root = ContextRegistry.write_root(self, table, destination=destination)
        return f"{root.strip('/')}/{table}"

    def _read_tables(self, table: str) -> List[str]:
        return [
            f"{root.strip('/')}/{table}"
            for root in ContextRegistry.read_roots(self, table)
        ]

    def _find_run(self, run_id: str) -> tuple[Optional[Dict[str, Any]], str]:
        """Locate a run by its id or its key, and the context holding it."""
        dm = self._get_dm()
        for context in self._read_tables(RUNS_TABLE):
            # Accepts either identifier: the actor holds whichever `submit` handed
            # back, and making it guess which one this expects would be a trap.
            for expression in (f"run_key == '{run_id}'", f"run_id == {run_id}"):
                if expression.startswith("run_id ==") and not str(run_id).isdigit():
                    continue
                rows = dm.filter(context, filter=expression, limit=1)
                if rows:
                    return rows[0], context
        return None, ""

    def _record_event(
        self,
        run_key: str,
        *,
        destination: Optional[str],
        stage: Optional[str] = None,
        level: str = "info",
        message: str = "",
        state: Optional[str] = None,
        done: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        row = IngestionEventRow(
            run_key=run_key,
            at=_now(),
            stage=stage,
            level=level,  # type: ignore[arg-type]
            message=message,
            state=state,
            done=done,
            total=total,
        )
        self._get_dm().insert_rows(
            self._write_table(EVENTS_TABLE, destination),
            [row.model_dump(exclude_none=True)],
        )

    def _update_run(
        self,
        run_key: str,
        context: str,
        updates: Dict[str, Any],
    ) -> None:
        self._get_dm().update_rows(context, updates, filter=f"run_key == '{run_key}'")

    # ── submitting ────────────────────────────────────────────────────────

    def submit(
        self,
        source: IngestionSource,
        target: IngestionTarget,
        *,
        embed: Optional[EmbedSpec] = None,
        post_ingest: Optional[PostIngestConfig] = None,
        destination: Optional[str] = None,
        mode: IngestionMode = "auto",
    ) -> IngestionRun:
        # Validation happens by constructing the request: the impossible
        # source/target pairing and every unsafe name are rejected here, before a
        # row exists, so a refused request leaves nothing behind.
        request = IngestionRequest(
            source=source,
            target=target,
            embed=embed,
            post_ingest=post_ingest,
            destination=destination,
            mode=mode,
        )

        tier = choose_tier(request, self._settings)
        if tier == "dispatched" and not self._settings.PIPELINE_URL:
            # Failing here rather than queueing: a dispatched run with nothing
            # configured to collect it would sit at `queued` forever, which reads
            # as slow rather than broken.
            raise RuntimeError(
                "This request needs dispatched execution and no pipeline control "
                "plane is configured (UNITY_INGESTION_PIPELINE_URL is unset). "
                "Submit a smaller request, or pass mode='inline' if the work is "
                "known to fit in process.",
            )

        key = _run_key()
        runs_context = self._write_table(RUNS_TABLE, destination)
        record = IngestionRunRecord(
            run_key=key,
            state="queued",
            mode=mode,
            executed_as=tier,  # type: ignore[arg-type]
            source_kind=source.kind,
            target_kind=target.kind,
            request_json=request.model_dump_json(),
            created_at=_now(),
        )
        self._get_dm().insert_rows(runs_context, [record.model_dump(exclude_none=True)])

        if tier == "inline":
            self._pool.submit(self._run_inline, key, runs_context, request)
        else:
            self._dispatch(key, runs_context, request)

        return IngestionRun(run_id=key, state="queued", executed_as=tier)  # type: ignore[arg-type]

    # ── inline execution ──────────────────────────────────────────────────

    def _run_inline(
        self,
        run_key: str,
        runs_context: str,
        request: IngestionRequest,
    ) -> None:
        """Execute a request in process, recording progress as it goes.

        Runs on a worker thread. Every exit path writes a terminal state, because a
        run left at ``running`` after the thread died is indistinguishable from one
        still working -- and that is the one failure mode that makes the whole
        ledger untrustworthy.
        """
        destination = request.destination
        self._update_run(
            run_key,
            runs_context,
            {"state": "running", "started_at": _now()},
        )
        self._record_event(
            run_key,
            destination=destination,
            stage="ingest",
            state="running",
            message=f"Storing from {request.source.kind} into {request.target.kind}.",
        )

        try:
            contexts, rows_written, files_processed = self._execute(run_key, request)
        except Exception as error:  # noqa: BLE001 -- see docstring
            # Deliberately broad. Whatever failed, the run must not be left looking
            # live: the actor is polling it, and an unfinished row is a run nobody
            # can act on.
            logger.exception("Ingestion run %s failed", run_key)
            self._record_event(
                run_key,
                destination=destination,
                stage="ingest",
                level="error",
                state="failed",
                message=str(error),
            )
            self._update_run(
                run_key,
                runs_context,
                {"state": "failed", "error": str(error), "finished_at": _now()},
            )
            return

        self._record_event(
            run_key,
            destination=destination,
            stage="ingest",
            state="succeeded",
            done=rows_written,
            total=rows_written,
            message=f"Wrote {rows_written} row(s) to {', '.join(contexts) or 'no context'}.",
        )
        self._update_run(
            run_key,
            runs_context,
            {
                "state": "succeeded",
                "contexts": contexts,
                "rows_written": rows_written,
                "files_processed": files_processed,
                "finished_at": _now(),
            },
        )

    def _execute(
        self,
        run_key: str,
        request: IngestionRequest,
    ) -> tuple[List[str], int, int]:
        """Route one request to the engine that handles it.

        Returns the contexts written, rows written and files processed. The contexts
        are what make a view over the result possible without predicting the storage
        layout, so they are harvested from what the engine actually did rather than
        assumed from the request.
        """
        source = request.source
        target = request.target

        if source.kind in {"rows", "table"} and target.kind == "table":
            rows = (
                source.rows
                if source.kind == "rows"
                else self._read_source_table(source)
            )
            context = self._ingest_rows(rows, request)
            return [context], len(rows), 0

        if source.kind in {"files", "folder"} and target.kind == "collection":
            paths = self._resolve_paths(source)
            contexts = self._ingest_files(paths, request)
            return contexts, 0, len(paths)

        if source.kind in {"files", "folder"} and target.kind == "table":
            # Files into one table: parse through the file pipeline into a staging
            # collection, then fold the extracted tables into the requested target.
            # Reusing the parse pipeline is the point -- it is where the per-format
            # backends and their tests live.
            paths = self._resolve_paths(source)
            staged = self._ingest_files(paths, request, staging=True)
            rows = self._collect_rows(staged)
            context = self._ingest_rows(rows, request)
            return [context], len(rows), len(paths)

        raise RuntimeError(
            f"No engine handles a {source.kind} source with a {target.kind} target.",
        )

    def _resolve_paths(self, source: Any) -> List[str]:
        if source.kind == "files":
            return list(source.paths)
        from pathlib import Path

        root = Path(source.path)
        walker = root.rglob if source.recursive else root.glob
        return [str(p) for p in sorted(walker(source.pattern)) if p.is_file()]

    def _read_source_table(self, source: Any) -> List[Dict[str, Any]]:
        return self._get_dm().filter(
            source.context,
            filter=source.filter,
            columns=source.columns,
        )

    def _ingest_rows(
        self,
        rows: List[Dict[str, Any]],
        request: IngestionRequest,
    ) -> str:
        target = request.target
        embed = request.embed
        result = self._get_dm().ingest(
            target.context,
            rows=rows,
            description=target.description,
            fields=target.fields,
            unique_keys=target.unique_keys,
            infer_untyped_fields=target.infer_untyped_fields,
            embed_columns=embed.columns if embed else None,
            embed_strategy=embed.strategy if embed else "along",
            post_ingest=request.post_ingest,
            destination=request.destination,
        )
        return getattr(result, "context", target.context) or target.context

    def _ingest_files(
        self,
        paths: List[str],
        request: IngestionRequest,
        *,
        staging: bool = False,
    ) -> List[str]:
        """Store files through the parse pipeline, returning the contexts written."""
        from unify.file_manager.types.config import FilePipelineConfig, IngestConfig

        target = request.target
        # A staging namespace is named after the run so two concurrent runs cannot
        # read each other's parsed output while folding it into a table.
        storage_id = (
            f"staging-{request.target.context.replace('/', '-')}"
            if staging
            else getattr(target, "name", None)
        )
        config = FilePipelineConfig(
            ingest=IngestConfig(
                storage_id=storage_id,
                table_ingest=getattr(target, "extract_tables", True),
            ),
        )
        fm = self._get_fm()
        result = fm.ingest_files(paths, config=config, destination=request.destination)
        return self._contexts_from_result(fm, result, storage_id)

    def _contexts_from_result(
        self,
        fm: Any,
        result: Any,
        storage_id: Optional[str],
    ) -> List[str]:
        """Derive the contexts a file run wrote.

        Asked of the file manager rather than string-built here, so the layout stays
        owned by the component that defines it.
        """
        if storage_id:
            ids = [storage_id]
        else:
            ids = [
                str(getattr(entry, "file_id", "") or "")
                for entry in getattr(result, "files", {}).values()
            ]
        contexts: List[str] = []
        for identifier in [i for i in ids if i]:
            contexts.append(fm._ctx_for_file_content(identifier))
        return contexts

    def _collect_rows(self, contexts: List[str]) -> List[Dict[str, Any]]:
        dm = self._get_dm()
        rows: List[Dict[str, Any]] = []
        for context in contexts:
            rows.extend(dm.filter(context, limit=self._settings.MAX_INLINE_ROWS))
        return rows

    def _dispatch(
        self,
        run_key: str,
        runs_context: str,
        request: IngestionRequest,
    ) -> None:
        """Hand a request to the hosted control plane. Wired separately."""
        raise RuntimeError(
            "Dispatched execution is not wired in this build. Pass mode='inline' "
            "for work that fits in process.",
        )

    # ── observing ─────────────────────────────────────────────────────────

    def get_status(self, run_id: str) -> RunStatus:
        row, _ = self._find_run(run_id)
        if row is None:
            raise ValueError(f"No ingestion run {run_id!r}.")

        events = self._events_for(row["run_key"])
        contexts = row.get("contexts") or []
        parked = int(row.get("parked") or 0)
        state = row.get("state") or "queued"

        return RunStatus(
            run_id=str(row.get("run_id", row["run_key"])),
            state=state,  # type: ignore[arg-type]
            executed_as=row.get("executed_as"),
            stages=stages_from_events(events),
            contexts=contexts,
            rows_written=int(row.get("rows_written") or 0),
            files_processed=int(row.get("files_processed") or 0),
            parked=parked,
            error=row.get("error"),
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            finished_at=row.get("finished_at"),
            next_step=next_step(
                state=state,
                parked=parked,
                error=row.get("error"),
                executed_as=row.get("executed_as"),
                contexts=contexts,
            ),
        )

    def _events_for(self, run_key: str) -> List[Dict[str, Any]]:
        dm = self._get_dm()
        events: List[Dict[str, Any]] = []
        for context in self._read_tables(EVENTS_TABLE):
            events.extend(
                dm.filter(
                    context,
                    filter=f"run_key == '{run_key}'",
                    limit=self._settings.MAX_EVENTS_PER_READ,
                ),
            )
        return sorted(events, key=lambda event: event.get("at") or "")

    def get_logs(
        self,
        run_id: str,
        *,
        stage: Optional[str] = None,
        limit: int = 200,
    ) -> List[LogEntry]:
        row, _ = self._find_run(run_id)
        if row is None:
            raise ValueError(f"No ingestion run {run_id!r}.")
        events = self._events_for(row["run_key"])
        if stage:
            events = [event for event in events if event.get("stage") == stage]
        return [
            LogEntry(
                at=event.get("at") or "",
                stage=event.get("stage"),
                level=event.get("level") or "info",
                message=event.get("message") or "",
            )
            for event in events[-limit:]
        ]

    def wait(self, run_id: str, *, timeout_s: Optional[float] = None) -> RunStatus:
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        # Backs off to a second so a long run does not spend the wait hammering the
        # backend, while a short one still returns promptly.
        interval = 0.2
        while True:
            status = self.get_status(run_id)
            if status.is_terminal:
                return status
            if deadline is not None and time.monotonic() >= deadline:
                return status
            time.sleep(interval)
            interval = min(interval * 1.5, 1.0)

    def list_runs(
        self,
        *,
        state: Optional[RunState] = None,
        context: Optional[str] = None,
        limit: int = 50,
    ) -> List[IngestionSummary]:
        dm = self._get_dm()
        rows: List[Dict[str, Any]] = []
        for runs_context in self._read_tables(RUNS_TABLE):
            rows.extend(
                dm.filter(
                    runs_context,
                    filter=f"state == '{state}'" if state else None,
                    limit=limit,
                ),
            )
        if context:
            rows = [row for row in rows if context in (row.get("contexts") or [])]
        rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
        return [
            IngestionSummary(
                run_id=str(row.get("run_id", row.get("run_key", ""))),
                state=row.get("state") or "queued",  # type: ignore[arg-type]
                source_kind=row.get("source_kind") or "",
                target_kind=row.get("target_kind") or "",
                contexts=row.get("contexts") or [],
                rows_written=int(row.get("rows_written") or 0),
                parked=int(row.get("parked") or 0),
                created_at=row.get("created_at"),
                finished_at=row.get("finished_at"),
            )
            for row in rows[:limit]
        ]

    # ── recovering ────────────────────────────────────────────────────────

    def retry(self, run_id: str, *, only: RetryScope = "dlq") -> RetryResult:
        row, runs_context = self._find_run(run_id)
        if row is None:
            raise ValueError(f"No ingestion run {run_id!r}.")

        parked = int(row.get("parked") or 0)
        if only == "dlq" and parked == 0:
            # Zero is an answer, not a failure. Saying so plainly stops a caller
            # concluding the retry itself broke.
            return RetryResult(
                run_id=run_id,
                scope=only,
                requeued=0,
                state=row.get("state") or "queued",  # type: ignore[arg-type]
                detail="Nothing is parked on this run, so there was nothing to retry.",
            )

        request = IngestionRequest.model_validate_json(row["request_json"])
        self._update_run(
            row["run_key"],
            runs_context,
            {"state": "queued", "error": None, "parked": 0},
        )
        self._record_event(
            row["run_key"],
            destination=request.destination,
            stage="ingest",
            message=f"Retrying ({only}).",
            state="queued",
        )
        self._pool.submit(self._run_inline, row["run_key"], runs_context, request)
        return RetryResult(
            run_id=run_id,
            scope=only,
            requeued=max(parked, 1),
            state="queued",
        )

    def cancel(self, run_id: str) -> bool:
        return self._set_state(run_id, "cancelled", "Cancelled by request.")

    def pause(self, run_id: str) -> bool:
        return self._set_state(run_id, "paused", "Paused by request.")

    def resume(self, run_id: str) -> bool:
        row, runs_context = self._find_run(run_id)
        if row is None or row.get("state") != "paused":
            # Only a paused run can resume. Refusing to "resume" a failed one keeps
            # the two recovery paths distinct: that case wants retry.
            return False
        request = IngestionRequest.model_validate_json(row["request_json"])
        self._update_run(row["run_key"], runs_context, {"state": "queued"})
        self._pool.submit(self._run_inline, row["run_key"], runs_context, request)
        return True

    def _set_state(self, run_id: str, state: str, message: str) -> bool:
        row, runs_context = self._find_run(run_id)
        if row is None or row.get("state") in TERMINAL_STATES:
            return False
        request = IngestionRequest.model_validate_json(row["request_json"])
        self._update_run(
            row["run_key"],
            runs_context,
            {"state": state, "finished_at": _now() if state == "cancelled" else None},
        )
        self._record_event(
            row["run_key"],
            destination=request.destination,
            message=message,
            state=state,
        )
        return True
