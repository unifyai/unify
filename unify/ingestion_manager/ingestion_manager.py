"""IngestionManager over Unify contexts, the shared ingest core, and the fleet.

Storage mirrors the other catalogue managers: runs and their events are rows in
contexts this manager declares, and all row I/O goes through DataManager so
destination routing and retry behaviour are inherited rather than reimplemented.

Two things here are load-bearing.

**A run is recorded before any work starts.** A failure is then always something
with an id that can be inspected and resumed, rather than an exception that went
past. It is also why ``submit`` returns immediately: the record is the handle, and
the work follows it.

**Both tiers run the same code.** Rows are written by
:class:`~unify.common.pipeline.checkpointed_ingest.CheckpointedIngest` whether the
work runs in this process or on the fleet, so leases, checkpoints and the
completion check are not reimplemented per tier and cannot drift between them.
The difference is only which artifact store and queue are bound -- and because
both write the same layout, a run interrupted in process is adoptable by the
fleet exactly as one interrupted on a worker is.

Neither parser nor inserter is reimplemented. Files go through the existing parse
pipeline with its per-format backends; rows go through DataManager's chunked
insert. This manager decides *where* work runs, stages what it needs, and records
what happened.
"""

from __future__ import annotations

import json
import logging
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from unify.common.context_registry import ContextRegistry, TableContext
from unify.common.model_to_fields import model_to_fields
from unify.common.pipeline import (
    CheckpointedIngest,
    DuplicateLiveAttempt,
    IncompleteIngest,
    InlineRowsHandle,
    LocalArtifactStore,
    ObjectStoreArtifactHandle,
    TableWork,
)
from unify.data_manager.types.ingest import PostIngestConfig
from unify.ingestion_manager.base import BaseIngestionManager
from unify.ingestion_manager.policy import choose_tier, next_step, stages_from_events
from unify.ingestion_manager.settings import IngestionSettings
from unify.ingestion_manager.types.request import (
    EmbedSpec,
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

logger = logging.getLogger(__name__)

RUNS_TABLE = "Ingestion/Runs"
EVENTS_TABLE = "Ingestion/Events"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_key() -> str:
    """Opaque, unguessable handle minted before the row exists.

    Separate from the auto-counted row id so work can be recorded against it
    immediately, without first reading back what id the backend assigned.
    """
    return secrets.token_urlsafe(9)


class IngestionManager(BaseIngestionManager):
    """Ingestion over Unify contexts, the shared ingest core and the fleet."""

    class Config:
        """Context registration for the Ingestion namespace."""

        required_contexts = [
            TableContext(
                name=RUNS_TABLE,
                description=(
                    "One row per ingestion. Records what was asked for, where it "
                    "ran, the contexts it wrote and how it ended, so a failure is "
                    "recoverable rather than merely reported."
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
        self._store = self._build_artifact_store()
        logger.debug("IngestionManager initialized")

    # ── plumbing ──────────────────────────────────────────────────────────

    def _build_artifact_store(self) -> Any:
        """Bind the artifact store this deployment ingests through.

        The hosted deployment overrides this with its object-store adapter. The
        local one is a full implementation of the same port, not a stub: it
        fences and checkpoints, so a self-host run is as resumable as a hosted
        one and the two are not different code paths.
        """
        from unify.session_details import SESSION_DETAILS

        root = Path(getattr(SESSION_DETAILS, "local_dir", None) or ".") / "Ingestion"
        return LocalArtifactStore(root_dir=root)

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
        )

        # Counted before the tier is chosen, so the decision rests on a
        # measurement. A stored table is counted by one server-side aggregate
        # rather than by reading it, which is what keeps the count cheap enough
        # to take before committing to anything.
        declared = self._count_source(request)
        tier = choose_tier(request, self._settings, row_count=declared)

        key = _run_key()
        runs_context = self._write_table(RUNS_TABLE, destination)
        # Staged before the run row so the row can point at it. The request may
        # carry a large rows payload, and a row holding that payload is both a
        # write the backend can reject and a read that costs more than it answers.
        request_key = self._stage_request(key, request)

        record = IngestionRunRecord(
            run_key=key,
            state="queued",
            executed_as=tier,
            source_kind=source.kind,
            target_kind=target.kind,
            request_key=request_key,
            declared_rows=declared,
            created_at=_now(),
        )
        self._get_dm().insert_rows(runs_context, [record.model_dump(exclude_none=True)])

        self._start(key, runs_context, request, tier=tier, declared=declared)
        return IngestionRun(run_id=key, state="queued", executed_as=tier)

    def _count_source(self, request: IngestionRequest) -> Optional[int]:
        """Measure the source exactly, or return ``None`` when it cannot be.

        Rows in hand are counted directly. A stored table is counted server-side,
        which is one cheap query even on a large context -- unlike reading it,
        which would cost the whole table just to decide where to run.

        Files return ``None``: what a file holds is unknowable before parsing it,
        and guessing from bytes or count is the error this design removes rather
        than refines.
        """
        source = request.source
        if source.kind == "rows":
            return len(source.rows)
        if source.kind == "table":
            return int(
                self._get_dm().reduce(
                    source.context,
                    metric="count",
                    filter=source.filter,
                )
                or 0,
            )
        return None

    def _stage_request(self, run_key: str, request: IngestionRequest) -> str:
        """Persist the request as an artifact and return its key.

        Staged rather than embedded so a retry or resume can rebuild the work
        without the caller reconstructing it, and without a bulk payload ever
        landing in a log row. This is also what a worker reads when the run is
        dispatched, so one representation serves both tiers.
        """
        key = f"jobs/{run_key}/request.json"
        self._store.put_json(key, request.model_dump(mode="json"))
        return key

    def _load_request(self, row: Dict[str, Any]) -> IngestionRequest:
        return IngestionRequest.model_validate(
            self._store.get_json(row["request_key"]),
        )

    def _start(
        self,
        run_key: str,
        runs_context: str,
        request: IngestionRequest,
        *,
        tier: str,
        declared: Optional[int],
    ) -> None:
        if tier == "inline":
            self._pool.submit(
                self._execute,
                run_key,
                runs_context,
                request,
                declared,
            )
            return
        self._dispatch(run_key, runs_context, request)

    # ── execution ─────────────────────────────────────────────────────────

    def _execute(
        self,
        run_key: str,
        runs_context: str,
        request: IngestionRequest,
        declared: Optional[int],
    ) -> None:
        """Run a request in this process, recording progress as it commits.

        Every exit path writes a terminal state. A run left at ``running`` after
        its thread died is indistinguishable from one still working, and that is
        the single failure that makes the whole ledger untrustworthy.
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
            total=declared,
            message=f"Storing from {request.source.kind} into {request.target.kind}.",
        )

        try:
            outcome = self._ingest_rows(run_key, request, declared=declared)
        except IncompleteIngest as shortfall:
            # Distinct from an ordinary failure: rows did land, and the run is
            # resumable from its checkpoint. Saying so is what turns a silent
            # under-ingest into something recoverable.
            logger.error("Ingestion run %s incomplete: %s", run_key, shortfall.detail)
            self._record_event(
                run_key,
                destination=destination,
                stage="ingest",
                level="error",
                state="failed",
                message=(
                    f"Committed less than the source declared ({shortfall.detail}). "
                    "Resume to continue from the last checkpoint."
                ),
            )
            self._update_run(
                run_key,
                runs_context,
                {
                    "state": "failed",
                    "error": str(shortfall),
                    "parked": len(shortfall.shortfalls),
                    "finished_at": _now(),
                },
            )
            return
        except DuplicateLiveAttempt:
            # Another attempt owns the work. Leaving the run alone is correct --
            # the holder will finish it and write the terminal state.
            logger.info(
                "Ingestion run %s is already being executed; standing down",
                run_key,
            )
            return
        except Exception as error:  # noqa: BLE001 -- see docstring
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

        rows = outcome.rows_committed
        contexts = outcome.contexts
        self._record_event(
            run_key,
            destination=destination,
            stage="ingest",
            state="succeeded",
            done=rows,
            total=declared or rows,
            message=f"Committed {rows} row(s) to {', '.join(contexts) or 'no context'}.",
        )
        self._update_run(
            run_key,
            runs_context,
            {
                "state": "succeeded",
                "contexts": contexts,
                "rows_written": rows,
                "finished_at": _now(),
            },
        )

    def _ingest_rows(
        self,
        run_key: str,
        request: IngestionRequest,
        *,
        declared: Optional[int],
    ) -> Any:
        """Ingest a rows or table source through the shared checkpointed core.

        Files never reach here: they are dispatched, because parsing loads the
        file and its model into whatever process runs it and a thread shares this
        one's memory limit.
        """
        source = request.source
        target = request.target
        if source.kind not in {"rows", "table"}:
            raise RuntimeError(
                f"A {source.kind} source is dispatched work and is not executed in "
                "process; this run should not have been started here.",
            )

        handle = (
            InlineRowsHandle(
                rows=list(source.rows),
                columns=list(source.rows[0].keys()) if source.rows else [],
                row_count=len(source.rows),
            )
            if source.kind == "rows"
            else self._handle_for_table(run_key, source, declared=int(declared or 0))
        )

        embed = request.embed
        work = TableWork(
            table_id=f"run-{run_key}",
            label=target.context,
            context=target.context,
            handle=handle,
            declared_rows=int(declared or handle.row_count or 0),
            columns=list(handle.columns or []),
            chunk_size=500,
            description=target.description,
            embed_columns=embed.columns if embed else None,
            embed_strategy=embed.strategy if embed else "off",
            post_ingest=request.post_ingest,
            infer_untyped_fields=target.infer_untyped_fields,
        )

        engine = CheckpointedIngest(
            artifact_store=self._store,
            job_id=run_key,
            lease_ttl_seconds=self._settings.LEASE_TTL_SECONDS,
            lease_steal_after_seconds=self._settings.LEASE_STEAL_AFTER_SECONDS,
        )
        return engine.run(
            [work],
            dm=self._get_dm(),
            destination=request.destination,
            source_path=run_key,
            on_progress=lambda table_id, done, total: self._record_event(
                run_key,
                destination=request.destination,
                stage="ingest",
                state="running",
                done=done,
                total=total or None,
                message=f"Committed {done} row(s).",
            ),
        )

    def _handle_for_table(self, run_key: str, source: Any, *, declared: int) -> Any:
        """Stage a stored table as a durable artifact, page by page.

        Read by offset because the backend caps a single read, and written to the
        artifact store as each page arrives rather than accumulated: a page is the
        most this process holds at once, whatever the table's size.

        Staging rather than reading the source directly at ingest time is also
        what makes a resume correct. The artifact outlives this process and does
        not change, so a resumed attempt re-reads exactly the rows the first
        attempt saw -- where re-querying the source could return a table that has
        since been written to, and skip_rows would then point at different rows.
        """
        dm = self._get_dm()
        page = self._settings.EVENTS_PAGE_SIZE
        key = f"jobs/{run_key}/source-table.jsonl"
        path = Path(self._store.put_json(f"{key}.pending", {"staging": key}))
        target = path.with_name("source-table.jsonl")

        columns: List[str] = list(source.columns or [])
        written = 0
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="\n") as handle:
            offset = 0
            while written < declared:
                batch = dm.filter(
                    source.context,
                    filter=source.filter,
                    columns=source.columns,
                    limit=min(page, declared - written),
                    offset=offset,
                )
                if not batch:
                    break
                for row in batch:
                    if not columns:
                        columns = [str(column) for column in row.keys()]
                    handle.write(json.dumps(row, ensure_ascii=False, default=str))
                    handle.write("\n")
                written += len(batch)
                offset += len(batch)
                if len(batch) < page:
                    break

        self._store.delete(f"{key}.pending")
        return ObjectStoreArtifactHandle(
            storage_uri=target.resolve().as_uri(),
            logical_path=source.context,
            source_local_path=str(target),
            artifact_format="jsonl",
            columns=columns,
            row_count=written,
        )

    def _dispatch(
        self,
        run_key: str,
        runs_context: str,
        request: IngestionRequest,
    ) -> None:
        """Hand a run to the pipeline control plane.

        The staged request is what the fleet reads, so nothing about the work is
        re-described here. If no control plane is configured the tier decision
        would not have chosen dispatch, so reaching this without one is a
        misconfiguration rather than a size problem, and it says so.
        """
        from unify.ingestion_manager.dispatch import dispatch_run

        if not self._settings.PIPELINE_URL:
            raise RuntimeError(
                "This run needs the worker fleet and no pipeline control plane is "
                "configured (UNITY_INGESTION_PIPELINE_URL is unset). Files are "
                "always parsed off the assistant's process, so configure a control "
                "plane or run the self-host worker services.",
            )

        dispatch_id = dispatch_run(
            base_url=self._settings.PIPELINE_URL,
            run_key=run_key,
            request=request,
            request_key=f"jobs/{run_key}/request.json",
            paths=self._resolve_paths(request.source),
        )
        self._update_run(run_key, runs_context, {"dispatch_id": dispatch_id})
        self._record_event(
            run_key,
            destination=request.destination,
            stage="parse",
            state="queued",
            message=f"Dispatched to the worker fleet as {dispatch_id}.",
        )

    def _resolve_paths(self, source: Any) -> List[str]:
        """List the files a source names, walking a folder when it is one.

        A walk needs no parsing, so this is measurement rather than prediction --
        it fixes the membership of the set the fleet will process, so a file added
        mid-run is not silently half-included.
        """
        if source.kind == "files":
            return list(source.paths)
        if source.kind != "folder":
            return []
        root = Path(source.path)
        walker = root.rglob if source.recursive else root.glob
        return [str(path) for path in sorted(walker(source.pattern)) if path.is_file()]

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

    def _events_for(
        self,
        run_key: str,
        *,
        max_rows: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read a run's events, paging until they are exhausted.

        Paged rather than fetched with a large limit because the backend serves at
        most a page at a time: asking for more returns a prefix, which would read
        as the whole history and quietly lose the end of a long run -- the part
        that says how it finished.
        """
        dm = self._get_dm()
        page = self._settings.EVENTS_PAGE_SIZE
        events: List[Dict[str, Any]] = []
        for context in self._read_tables(EVENTS_TABLE):
            offset = 0
            while True:
                batch = dm.filter(
                    context,
                    filter=f"run_key == '{run_key}'",
                    limit=page,
                    offset=offset,
                )
                if not batch:
                    break
                events.extend(batch)
                offset += len(batch)
                if len(batch) < page:
                    break
                if max_rows is not None and len(events) >= max_rows:
                    break
        return sorted(events, key=lambda event: event.get("at") or "")

    def get_logs(
        self,
        run_id: str,
        *,
        stage: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[LogEntry]:
        row, _ = self._find_run(run_id)
        if row is None:
            raise ValueError(f"No ingestion run {run_id!r}.")
        events = self._events_for(row["run_key"])
        if stage:
            events = [event for event in events if event.get("stage") == stage]
        window = events[offset : offset + limit]
        return [
            LogEntry(
                at=event.get("at") or "",
                stage=event.get("stage"),
                level=event.get("level") or "info",
                message=event.get("message") or "",
            )
            for event in window
        ]

    def wait(self, run_id: str, *, timeout_s: Optional[float] = None) -> RunStatus:
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        # Backs off to a second so a long run does not spend the wait hammering
        # the backend, while a short one still returns promptly.
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
                    limit=min(limit, self._settings.EVENTS_PAGE_SIZE),
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

        request = self._load_request(row)
        if only == "all":
            # The checkpoints are what make a resume skip committed work, so
            # re-attempting everything means discarding them. Done explicitly
            # here rather than left implicit, because it is the one scope that
            # rewrites rows that were already correct.
            self._clear_checkpoints(row["run_key"])

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

        dispatch_id = row.get("dispatch_id")
        if dispatch_id:
            # Asked of the control plane rather than re-published from here: it
            # owns the transition, so a retry cannot race a stale-recovery into
            # two live attempts on one table.
            from unify.ingestion_manager.dispatch import request_retry

            request_retry(
                base_url=self._settings.PIPELINE_URL,
                dispatch_id=dispatch_id,
                scope=only,
            )
        else:
            self._start(
                row["run_key"],
                runs_context,
                request,
                tier="inline",
                declared=row.get("declared_rows"),
            )

        return RetryResult(
            run_id=run_id,
            scope=only,
            requeued=max(parked, 1),
            state="queued",
        )

    def _clear_checkpoints(self, run_key: str) -> None:
        self._store.delete(f"jobs/{run_key}/checkpoints/run-{run_key}")

    def cancel(self, run_id: str) -> bool:
        return self._transition(run_id, "cancelled", "Cancelled by request.")

    def pause(self, run_id: str) -> bool:
        return self._transition(run_id, "paused", "Paused by request.")

    def resume(self, run_id: str) -> bool:
        row, runs_context = self._find_run(run_id)
        if row is None or row.get("state") != "paused":
            # Only a paused run can resume. Refusing to "resume" a failed one
            # keeps the two recovery paths distinct: that case wants retry.
            return False
        request = self._load_request(row)
        self._update_run(row["run_key"], runs_context, {"state": "queued"})
        self._record_event(
            row["run_key"],
            destination=request.destination,
            message="Resuming from the last checkpoint.",
            state="queued",
        )

        dispatch_id = row.get("dispatch_id")
        if dispatch_id:
            from unify.ingestion_manager.dispatch import request_resume

            request_resume(
                base_url=self._settings.PIPELINE_URL,
                dispatch_id=dispatch_id,
            )
        else:
            self._start(
                row["run_key"],
                runs_context,
                request,
                tier="inline",
                declared=row.get("declared_rows"),
            )
        return True

    def _transition(self, run_id: str, state: str, message: str) -> bool:
        """Move a live run to *state*, telling the fleet when it owns the work.

        The run row is the record either way, but a dispatched run also has
        messages in flight -- so recording the state without telling the control
        plane would leave workers writing to a run the ledger calls cancelled.
        """
        row, runs_context = self._find_run(run_id)
        if row is None or row.get("state") in TERMINAL_STATES:
            return False
        request = self._load_request(row)

        dispatch_id = row.get("dispatch_id")
        if dispatch_id:
            from unify.ingestion_manager.dispatch import (
                request_cancel,
                request_pause,
            )

            ask = request_cancel if state == "cancelled" else request_pause
            ask(base_url=self._settings.PIPELINE_URL, dispatch_id=dispatch_id)

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
