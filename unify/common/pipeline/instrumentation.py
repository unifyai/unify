"""Reusable pipeline instrumentation context manager.

Encapsulates observability wiring -- the run ledger and its stage, file and run
manifests -- behind a single object that is safe to call unconditionally. When no
ledger is attached every method is a no-op, so callers never need conditionals.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from .run_ledger import (
    JsonlRunLedger,
    PipelineFileManifest,
    PipelineRunManifest,
    PipelineStageManifest,
    RunLedger,
    generate_run_ledger_path,
)

logger = logging.getLogger(__name__)


@dataclass
class _Counters:
    """How much this run has recorded, for progress reporting."""

    progress_events: int = 0
    run_manifests: int = 0
    file_manifests: int = 0
    stage_manifests: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def increment(
        self,
        *,
        progress: int = 0,
        run: int = 0,
        filec: int = 0,
        stage: int = 0,
    ) -> None:
        with self.lock:
            self.progress_events += progress
            self.run_manifests += run
            self.file_manifests += filec
            self.stage_manifests += stage


class PipelineInstrumentation:
    """Context manager that wires up the run ledger and manifest recording.

    Usage::

        instr = PipelineInstrumentation.from_config(config, run_id="abc123")
        with instr:
            instr.record_stage(...)
            instr.record_file(...)
        # On exit: the run-completed manifest is written and the ledger flushed

    All public methods are safe to call with no ledger attached (no-op).
    """

    def __init__(
        self,
        *,
        run_id: str | None = None,
        run_ledger: RunLedger | None = None,
        parallel_files: bool = False,
        file_count: int = 0,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.run_id: str = run_id or uuid4().hex
        self._run_ledger = run_ledger
        self._parallel_files = parallel_files
        self._file_count = file_count
        self._meta = dict(meta or {})
        self._counters = _Counters()
        self._started_at: float | None = None

    @property
    def enabled(self) -> bool:
        return self._run_ledger is not None

    @property
    def has_run_ledger(self) -> bool:
        return self._run_ledger is not None

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config,
        *,
        run_id: str | None = None,
        parallel_files: bool = False,
        file_count: int = 0,
        meta: dict[str, Any] | None = None,
    ) -> "PipelineInstrumentation":
        """Build from a ``FilePipelineConfig`` (or any object with ``.diagnostics``)."""
        run_id = run_id or uuid4().hex
        run_ledger: RunLedger | None = None

        diagnostics = getattr(config, "diagnostics", None)
        if diagnostics and getattr(diagnostics, "enable_run_ledger", False):
            ledger_path = getattr(diagnostics, "run_ledger_file", None)
            run_ledger = JsonlRunLedger(path=ledger_path or generate_run_ledger_path())

        return cls(
            run_id=run_id,
            run_ledger=run_ledger,
            parallel_files=parallel_files,
            file_count=file_count,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipelineInstrumentation":
        self._started_at = time.perf_counter()
        if self._run_ledger is not None:
            try:
                self._run_ledger.write(
                    PipelineRunManifest(
                        run_id=self.run_id,
                        status="started",
                        file_count=self._file_count,
                        parallel_files=self._parallel_files,
                        meta=self._meta,
                    ),
                )
                self._counters.increment(run=1)
            except Exception as exc:
                logger.debug("Run-started manifest write failed: %s", exc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        total_ms = (
            (time.perf_counter() - self._started_at) * 1000
            if self._started_at is not None
            else 0.0
        )
        self._finalize_run_ledger(total_ms)
        return None

    # ------------------------------------------------------------------
    # Stage recording
    # ------------------------------------------------------------------

    def record_stage(
        self,
        *,
        file_path: str,
        stage_name: str,
        status: str,
        duration_ms: float = 0.0,
        retries_used: int = 0,
        error: str | None = None,
        stage_id: str | None = None,
        file_id: int | None = None,
        storage_id: str | None = None,
        table_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if self._run_ledger is None:
            return
        try:
            self._run_ledger.write(
                PipelineStageManifest(
                    run_id=self.run_id,
                    stage_id=stage_id,
                    file_path=file_path,
                    file_id=file_id,
                    storage_id=storage_id,
                    table_id=table_id,
                    stage_name=stage_name,
                    status="success" if status == "success" else "error",
                    duration_ms=duration_ms,
                    retries_used=retries_used,
                    error=error,
                    meta=dict(meta or {}),
                ),
            )
            self._counters.increment(stage=1)
        except Exception as exc:
            logger.debug("Stage ledger write failed: %s", exc)

    def record_file(
        self,
        *,
        file_path: str,
        status: str,
        total_duration_ms: float = 0.0,
        retries_used: int = 0,
        file_id: int | None = None,
        storage_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        if self._run_ledger is None:
            return
        try:
            self._run_ledger.write(
                PipelineFileManifest(
                    run_id=self.run_id,
                    file_path=file_path,
                    file_id=file_id,
                    storage_id=storage_id,
                    status="success" if status == "success" else "error",
                    total_duration_ms=total_duration_ms,
                    retries_used=retries_used,
                    meta=dict(meta or {}),
                ),
            )
            self._counters.increment(filec=1)
        except Exception as exc:
            logger.debug("File ledger write failed: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def make_stage_id(
        self,
        *,
        file_path: str,
        stage_name: str,
        discriminator: str | None = None,
    ) -> str | None:
        if not self.run_id:
            return None
        raw = "::".join(
            part
            for part in (self.run_id, file_path, stage_name, discriminator or "")
            if part is not None
        )
        return uuid5(NAMESPACE_URL, raw).hex

    def increment_progress_events(self, count: int = 1) -> None:
        self._counters.increment(progress=count)

    # ------------------------------------------------------------------
    # Finalization (private)
    # ------------------------------------------------------------------

    def _finalize_run_ledger(self, total_ms: float) -> None:
        if self._run_ledger is None:
            return
        try:
            self._run_ledger.write(
                PipelineRunManifest(
                    run_id=self.run_id,
                    status="completed",
                    file_count=self._file_count,
                    parallel_files=self._parallel_files,
                    total_duration_ms=total_ms,
                    meta=self._meta,
                ),
            )
            self._counters.increment(run=1)
        except Exception as exc:
            logger.debug("Run-completed manifest write failed: %s", exc)
        finally:
            try:
                self._run_ledger.flush()
                self._run_ledger.close()
            except Exception:
                pass
