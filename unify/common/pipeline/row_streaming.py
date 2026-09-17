"""Row iteration for ``TableInputHandle`` variants.

This module is a thin dispatcher: each handle type delegates to the
native backend that knows how to stream its format efficiently
(Polars for CSV, openpyxl read-only for XLSX).  No file I/O logic
lives here -- it all lives in the backends.

All handle types are defined in ``unify.common.pipeline.types``, so
this module has **no dependency on FileManager** -- backend imports
are lazy so consumers that only use ``InlineRowsHandle`` or
``ObjectStoreArtifactHandle`` never trigger parser imports.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Dict
from urllib.parse import unquote, urlparse

from .types import (
    CsvFileHandle,
    InlineRowsHandle,
    ObjectStoreArtifactHandle,
    TableInputHandle,
    XlsxSheetHandle,
)

logger = logging.getLogger(__name__)

JsonObject = Dict[str, object]


def iter_table_input_rows(
    handle: TableInputHandle,
    *,
    skip_rows: int = 0,
) -> Iterator[JsonObject]:
    """Yield table rows from any supported transport handle.

    Parameters
    ----------
    skip_rows:
        Number of leading data rows to consume and discard before
        yielding.  Used by crash-recovery to resume after a checkpoint.
    """

    if isinstance(handle, InlineRowsHandle):
        yield from _apply_skip(
            (dict(row) for row in handle.rows),
            skip_rows,
        )
        return

    if isinstance(handle, CsvFileHandle):
        from unify.file_manager.file_parsers.implementations.native.backends.csv_backend import (
            NativeCsvBackend,
        )

        path = _resolve_local_path(
            source_local_path=handle.source_local_path,
            storage_uri=handle.storage_uri,
        )
        yield from _apply_skip(
            NativeCsvBackend.iter_rows(
                path,
                delimiter=handle.delimiter,
                quotechar=handle.quotechar,
                has_header=handle.has_header,
                encoding=handle.encoding or "utf8-lossy",
                columns=handle.columns or None,
            ),
            skip_rows,
        )
        return

    if isinstance(handle, XlsxSheetHandle):
        from unify.file_manager.file_parsers.implementations.native.backends.excel_backend import (
            NativeExcelBackend,
        )

        path = _resolve_local_path(
            source_local_path=handle.source_local_path,
            storage_uri=handle.storage_uri,
        )
        yield from _apply_skip(
            NativeExcelBackend.iter_rows(
                path,
                sheet_name=handle.sheet_name,
                has_header=handle.has_header,
                columns=handle.columns or None,
            ),
            skip_rows,
        )
        return

    if isinstance(handle, ObjectStoreArtifactHandle):
        yield from _iter_object_store_rows(handle, skip_rows=skip_rows)
        return

    raise TypeError(f"Unsupported table input handle: {type(handle)!r}")


def iter_table_input_row_batches(
    handle: TableInputHandle,
    batch_size: int,
    *,
    skip_rows: int = 0,
) -> Iterator[list[JsonObject]]:
    """Yield bounded row batches from a table input handle."""

    size = max(int(batch_size or 0), 1)
    batch: list[JsonObject] = []
    for row in iter_table_input_rows(handle, skip_rows=skip_rows):
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# Skip helper
# ---------------------------------------------------------------------------


def _apply_skip(
    rows: Iterator[JsonObject],
    skip: int,
) -> Iterator[JsonObject]:
    """Consume and discard the first *skip* rows, then yield the rest."""
    if skip <= 0:
        yield from rows
        return
    skipped = 0
    for row in rows:
        if skipped < skip:
            skipped += 1
            continue
        yield row
    if skipped and skipped >= skip:
        logger.info("[row_streaming] Skipped %d rows (checkpoint resume)", skipped)


# ---------------------------------------------------------------------------
# JSONL artifact handle
# ---------------------------------------------------------------------------


def _iter_object_store_rows(
    handle: ObjectStoreArtifactHandle,
    *,
    skip_rows: int = 0,
) -> Iterator[JsonObject]:
    if handle.artifact_format != "jsonl":
        raise NotImplementedError(
            f"Artifact streaming is not implemented for {handle.artifact_format!r}",
        )

    fh = _open_jsonl_handle(handle)
    try:
        skipped = 0
        emitted = 0
        for line in fh:
            text = line.strip()
            if not text:
                continue
            if skipped < skip_rows:
                skipped += 1
                continue
            loaded = json.loads(text)
            if isinstance(loaded, dict):
                emitted += 1
                yield {str(key): value for key, value in loaded.items()}
        if skipped:
            logger.info(
                "[row_streaming] Skipped %d JSONL rows (checkpoint resume)",
                skipped,
            )
        if handle.row_count is not None and skipped + emitted != handle.row_count:
            raise ValueError(
                f"JSONL artifact row count mismatch for {handle.storage_uri}: "
                f"read={skipped + emitted} expected={handle.row_count}",
            )
    finally:
        if hasattr(fh, "close"):
            fh.close()


def _open_jsonl_handle(handle: ObjectStoreArtifactHandle):
    """Return a line-iterable file handle for the JSONL artifact.

    A staged ``source_local_path`` wins; otherwise the ``file://`` storage
    URI is opened directly.
    """
    path = _resolve_local_path(
        source_local_path=handle.source_local_path,
        storage_uri=handle.storage_uri,
    )
    return path.open("r", encoding="utf-8")


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def _resolve_local_path(*, source_local_path: str, storage_uri: str) -> Path:
    if source_local_path:
        return Path(source_local_path).expanduser().resolve()
    parsed = urlparse(storage_uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser().resolve()
    raise ValueError(f"Cannot resolve local path from storage URI: {storage_uri}")
