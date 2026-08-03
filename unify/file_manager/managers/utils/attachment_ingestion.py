"""Attachment ingestion: hand the file to IngestionManager, track its status.

An attachment used to take one of two entirely separate routes. With cloud
dispatch configured it published to the worker fleet; without, it parsed on a
thread **inside the assistant's own process** -- which is the case this module no
longer contains. Parsing loads the file and its model into whatever process does
it, and a thread shares that process's memory limit, so an oversized attachment
could take the assistant down with it. Neither route checkpointed, so an
interrupted attachment was simply lost.

Both are now one call to ``IngestionManager.submit``, which decides where the work
runs from a single rule and checkpoints it either way. What remains here is the
part that is genuinely FileManager's: keeping ``FileRecords.ingestion_status``
current, because that is what the UI reads to show an attachment as still landing.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Sequence

from unify.file_manager.types.config import FilePipelineConfig

from .ops import add_or_replace_file_row

if TYPE_CHECKING:
    from unify.file_manager.managers.file_manager import FileManager

logger = logging.getLogger(__name__)

_UNSET = object()

# How long a status watcher follows one attachment before giving up on it. The
# run itself is unaffected -- it is recorded, checkpointed and resumable, so this
# bounds only how long a thread sits here waiting to relay the outcome.
_WATCH_TIMEOUT_S = 3600.0


class AttachmentIngestionPool:
    """Submits attachments for ingestion and relays their status.

    Holds no parsing and no queue of its own. The thread pool exists only to
    watch runs and write their outcome back to ``FileRecords``, so its size
    bounds concurrent *watchers*, not concurrent work -- the work is placed
    wherever the tier decision puts it.

    Deduplication is kept: the same path submitted twice while still in flight
    would produce two runs writing the same namespace.
    """

    def __init__(self, *, max_workers: int = 2) -> None:
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="attachment-watch",
        )
        self._active: dict[str, str] = {}

    def enqueue(
        self,
        file_manager: "FileManager",
        file_paths: str | Sequence[str],
        *,
        config: FilePipelineConfig | None = None,
    ) -> list[str]:
        """Submit each path for ingestion and return the paths now in flight.

        ``config`` is accepted for callers that pin parse behaviour, and its
        table-extraction choice is carried onto the target. Everything else it
        used to control -- where the work runs, how many files at a time -- is
        decided by the ingestion tier rule now, which is why it no longer needs
        to be threaded through per caller.
        """
        queued: list[str] = []

        for file_path in _normalize_paths(file_paths):
            with self._lock:
                if file_path in self._active:
                    # Already in flight. A second run would write the same
                    # namespace as the first and race it.
                    queued.append(file_path)
                    continue
                self._active[file_path] = ""

            try:
                run_id = _submit_attachment(
                    file_manager,
                    file_path=file_path,
                    config=config,
                )
            except Exception as error:
                logger.exception(
                    "Failed to submit attachment for ingestion",
                    extra={"file_path": file_path},
                )
                with self._lock:
                    self._active.pop(file_path, None)
                _upsert_attachment_status(
                    file_manager,
                    file_path=file_path,
                    ingestion_status="error",
                    error=str(error) or "could not submit for ingestion",
                    parse_status="error",
                )
                continue

            with self._lock:
                self._active[file_path] = run_id
            self._executor.submit(
                self._watch,
                file_manager,
                file_path=file_path,
                run_id=run_id,
            )
            queued.append(file_path)

        return queued

    def _watch(
        self,
        file_manager: "FileManager",
        *,
        file_path: str,
        run_id: str,
    ) -> None:
        """Follow one run and write its outcome to ``FileRecords``.

        Watching rather than doing. If this thread dies the attachment still
        completes -- the run owns the work -- and only the status relay is lost,
        which the next read of the run repairs.
        """
        from unify.manager_registry import ManagerRegistry

        try:
            ingestion = ManagerRegistry.get_ingestion_manager()
            status = ingestion.wait(run_id, timeout_s=_WATCH_TIMEOUT_S)
            if status.state == "succeeded":
                _upsert_attachment_status(
                    file_manager,
                    file_path=file_path,
                    ingestion_status="success",
                )
            elif status.is_terminal:
                _upsert_attachment_status(
                    file_manager,
                    file_path=file_path,
                    ingestion_status="error",
                    # next_step names the recovery, so the record carries what
                    # to do rather than only that something went wrong.
                    error=status.error or status.next_step,
                    parse_status="error",
                )
            else:
                # Still running past the watch window. Left as-is deliberately:
                # marking it failed would be untrue, and the run is still live.
                logger.info(
                    "Attachment %s still ingesting after the watch window (run %s)",
                    file_path,
                    run_id,
                )
        except Exception:
            logger.exception(
                "Failed to relay attachment ingestion status",
                extra={"file_path": file_path, "run_id": run_id},
            )
        finally:
            with self._lock:
                self._active.pop(file_path, None)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


_POOL: AttachmentIngestionPool | None = None
_POOL_LOCK = threading.Lock()


def _get_pool() -> AttachmentIngestionPool:
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            from unify.settings import SETTINGS

            _POOL = AttachmentIngestionPool(
                max_workers=SETTINGS.file.ATTACHMENT_INGESTION_MAX_WORKERS,
            )
        return _POOL


def enqueue_attachment_ingestion(
    file_manager: "FileManager",
    file_paths: str | Sequence[str],
    *,
    config: FilePipelineConfig | None = None,
) -> list[str]:
    """Public entry point -- delegates to the module-level pool singleton."""
    return _get_pool().enqueue(file_manager, file_paths, config=config)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_paths(file_paths: str | Sequence[str]) -> list[str]:
    if isinstance(file_paths, str):
        return [file_paths]
    return [path for path in file_paths if path]


def _submit_attachment(
    file_manager: "FileManager",
    *,
    file_path: str,
    config: FilePipelineConfig | None,
) -> str:
    """Submit one attachment and mark the record as queued.

    Targets an unnamed collection, which gives the file its own namespace --
    attachments arrive unrelated to each other, so grouping them under a shared
    name would make one file's re-ingest touch another's rows.
    """
    from unify.ingestion_manager.types import CollectionTarget, FilesSource
    from unify.manager_registry import ManagerRegistry

    _upsert_attachment_status(
        file_manager,
        file_path=file_path,
        ingestion_status="queued",
    )
    extract_tables = True
    if config is not None:
        extract_tables = bool(getattr(config.ingest, "table_ingest", True))

    run = ManagerRegistry.get_ingestion_manager().submit(
        FilesSource(paths=[file_path]),
        CollectionTarget(extract_tables=extract_tables),
    )
    logger.info(
        "Submitted attachment %s for ingestion as run %s (%s)",
        file_path,
        run.run_id,
        run.executed_as,
    )
    return run.run_id


def _upsert_attachment_status(
    file_manager: "FileManager",
    *,
    file_path: str,
    ingestion_status: str,
    error: str | None = None,
    parse_status: str | None | object = _UNSET,
) -> None:
    try:
        entry: dict = {
            "file_path": file_path,
            "source_uri": file_manager.resolve_source_uri(file_path),
            "source_provider": file_manager.source_provider_name,
            "ingestion_status": ingestion_status,
            "error": error,
            "storage_id": "",
        }
        if parse_status is not _UNSET:
            entry["status"] = parse_status
        add_or_replace_file_row(file_manager, entry=entry)
    except Exception:
        logger.exception(
            "Failed to persist attachment ingestion status",
            extra={"file_path": file_path, "ingestion_status": ingestion_status},
        )


# ---------------------------------------------------------------------------
# Completion callback (invoked by CommsManager when a worker reports back)
# ---------------------------------------------------------------------------


def apply_attachment_completion(
    file_manager: "FileManager",
    *,
    display_name: str,
    status: str,
    error: str | None = None,
) -> None:
    """Update ``FileRecords`` for a completed worker-dispatched attachment.

    Called from ``CommsManager`` when a ``thread="attachment_ingestion_complete"``
    message arrives on the per-assistant topic. Kept alongside the watcher rather
    than replaced by it: the callback arrives as soon as the fleet finishes, where
    the watcher only notices on its next poll, so the two together make the status
    prompt without either being the sole path.
    """
    if status == "success":
        _upsert_attachment_status(
            file_manager,
            file_path=display_name,
            ingestion_status="success",
        )
    else:
        _upsert_attachment_status(
            file_manager,
            file_path=display_name,
            ingestion_status="error",
            error=error or "attachment ingestion failed",
            parse_status="error",
        )
