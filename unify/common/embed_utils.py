"""
Utility functions for embedding-based vector search through the logs.
"""

import hashlib
import logging
import os
import sys
import tempfile
import unisdk
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Cross-platform file locking
if sys.platform == "win32":
    import msvcrt

    def _lock_file(file_obj):
        """Acquire an exclusive lock on the file (Windows)."""
        msvcrt.locking(file_obj.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock_file(file_obj):
        """Release the lock on the file (Windows)."""
        file_obj.seek(0)
        msvcrt.locking(file_obj.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_file(file_obj):
        """Acquire an exclusive lock on the file (Unix)."""
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_file(file_obj):
        """Release the lock on the file (Unix)."""
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)


# Model to use for text embeddings
EMBED_MODEL = "text-embedding-3-small"


# In‑process locks keyed by (context, column_key) to avoid race conditions when
# multiple concurrent tool calls attempt to create the same derived/embedding
# columns at the same time.
_COLUMN_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_COLUMN_LOCKS_LOCK = threading.Lock()


def _get_column_lock(context: str, key: str) -> threading.Lock:
    """Return a process-local lock for a specific (context, key)."""
    lk_key = (context, key)
    with _COLUMN_LOCKS_LOCK:
        lock = _COLUMN_LOCKS.get(lk_key)
        if lock is None:
            lock = threading.Lock()
            _COLUMN_LOCKS[lk_key] = lock
        return lock


@contextmanager
def _cross_process_column_lock(context: str, key: str, timeout: float = 600.0):
    """
    Cross-process file lock for column creation.

    Provides coordination across parallel test processes that share the same
    Unify backend. Uses a file lock in the temp directory keyed by a hash of
    (context, key) to avoid filename length issues.

    The overhead is ~12μs per lock acquisition - negligible compared to the
    ~500ms+ embedding operations being protected.

    Parameters
    ----------
    context : str
        The Unify context name.
    key : str
        The column key being created.
    timeout : float, default 600.0
        Maximum seconds to wait for the lock (10 minutes). If exceeded, raises TimeoutError.
        This prevents indefinite hangs if another process holds the lock and is
        stuck (e.g., hung API call).
    """
    import time

    # Hash the key to avoid filesystem issues with long/special characters
    lock_id = hashlib.sha256(f"{context}:{key}".encode()).hexdigest()[:16]
    lock_path = os.path.join(tempfile.gettempdir(), f"unity_col_{lock_id}.lock")

    lock_file = open(lock_path, "w")
    try:
        # Try non-blocking acquisition with timeout to avoid indefinite hangs
        start = time.monotonic()
        while True:
            try:
                _lock_file(lock_file)
                break  # Successfully acquired lock
            except (BlockingIOError, OSError):
                elapsed = time.monotonic() - start
                if elapsed >= timeout:
                    lock_file.close()
                    raise TimeoutError(
                        f"Timeout after {timeout}s waiting for column lock: "
                        f"context={context}, key={key}. Another process may be hung.",
                    )
                time.sleep(0.1)  # Brief sleep before retry
        yield
    finally:
        _unlock_file(lock_file)
        lock_file.close()


def list_private_fields(context: str, *, project: str | None = None) -> list[str]:
    """
    Return a list of private field names for a context.

    Private fields are defined as columns whose names start with "_". These
    typically include derived/debug columns and embedding vectors which can be
    very large, so they should be excluded from payloads returned to clients.
    """
    try:
        fields = unisdk.get_fields(context=context, project=project)
        return [name for name in fields.keys() if name.startswith("_")]
    except Exception:
        # If field introspection fails (e.g. offline tests), fall back to none
        return []


def escape_single_quotes(text: str) -> str:
    """Return text with single quotes escaped for Unify expressions."""
    return text.replace("'", "\\'")


# Page size for coverage scans / backfills. Matches ingest embedding batches.
_LOG_ID_PAGE_SIZE = 1000


def _iter_log_ids(
    context: str,
    *,
    filter: str | None = None,
    project: str | None = None,
    page_size: int = _LOG_ID_PAGE_SIZE,
) -> list[int]:
    """Return all log IDs in ``context`` matching ``filter`` (paginated)."""
    ids: list[int] = []
    offset = 0
    while True:
        batch = unisdk.get_logs(
            context=context,
            filter=filter,
            return_ids_only=True,
            limit=page_size,
            offset=offset,
            project=project,
        )
        if not batch:
            break
        ids.extend(int(x) for x in batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return ids


def _chunked(items: list[int], size: int = _LOG_ID_PAGE_SIZE) -> list[list[int]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _backfill_derived_for_ids(
    context: str,
    key: str,
    equation: str,
    *,
    from_ids: list[int],
    project: str | None = None,
) -> None:
    """Create/refill derived values for ``from_ids`` in batches."""
    for chunk in _chunked(from_ids):
        ensure_derived_column(
            context=context,
            key=key,
            equation=equation,
            derived=True,
            from_ids=chunk,
            project=project,
        )


def ensure_derived_column(
    context: str,
    key: str,
    equation: str,
    *,
    referenced_logs_context: str | None = None,
    derived: bool | None = None,
    from_ids: list[int] | None = None,
    project: str | None = None,
) -> None:
    """
    Ensure a derived column exists with the given equation.

    - Creates the column if missing, guarded by a process-local lock to avoid
      duplicate creations under concurrency.
    - Tolerates backend uniqueness races.
    - By default, scopes placeholders to a local alias `lg` referencing the
      provided `context` when `referenced_logs_context` is not specified.
    """
    # Fast path: if field already exists and we are not doing targeted
    # derived logs creation using from_ids, return without locking or logging
    try:
        fields = unisdk.get_fields(context=context, project=project)
        if key in fields and not from_ids:
            return
    except Exception:
        # If introspection fails, fall through to locked creation
        pass

    # Attempt creation under both a cross-process file lock (for parallel test
    # isolation) and a process-local thread lock (for in-process concurrency).
    # The file lock coordinates across parallel pytest sessions; the thread lock
    # handles concurrent async tasks within a single process.
    thread_lock = _get_column_lock(context, key)
    with _cross_process_column_lock(context, key):
        with thread_lock:
            # Early return if the column already exists and
            # we are not doing targeted derived logs creation using from_ids
            # We intentionally do this to avoid redundant calls to create
            # duplicate embeddings that will get rejected by the backend
            # due to duplication constraint
            existing = unisdk.get_fields(context=context, project=project)
            if key in existing and not from_ids:
                return

            try:
                referenced_logs = {}
                if from_ids:
                    # Instruct backend to scope the operation to a subset of log entries
                    referenced_logs = {
                        "lg": list(from_ids),
                    }
                else:
                    referenced_logs = {
                        "lg": {"context": referenced_logs_context or context},
                    }

                response = unisdk.create_derived_logs(
                    context=context,
                    key=key,
                    equation=equation,
                    referenced_logs=referenced_logs,
                    derived=derived,
                    project=project,
                )
                id_count = len(from_ids) if from_ids else None
                logger.debug(
                    "create_derived_logs response context=%s key=%s "
                    "from_ids_count=%s referenced_logs=%s => %s",
                    context,
                    key,
                    id_count,
                    referenced_logs,
                    response,
                )
            except unisdk.RequestError as e:
                body = getattr(e.response, "text", "") or ""
                logger.debug(
                    "create_derived_logs FAILED context=%s key=%s "
                    "from_ids_count=%s status=%s body=%s",
                    context,
                    key,
                    len(from_ids) if from_ids else None,
                    getattr(e.response, "status_code", "?"),
                    body,
                )
                if (
                    "already exists" in body
                    or "duplicate key value violates unique constraint" in body
                ):
                    return
                # For other errors, re-raise
                raise e


def ensure_vector_column(
    context: str,
    embed_column: str,
    source_column: str,
    derived_expr: str | None = None,
    *,
    async_embeddings: bool = False,
    from_ids: list[int] | None = None,
    project: str | None = None,
) -> bool:
    """
    Ensure that a vector column exists and is populated for the given context.

    If the embedding field is registered but values are missing (e.g. after a
    context rollback that preserved schema while dropping derived rows), missing
    rows are backfilled. Field presence alone is not treated as coverage.

    Args:
        context (str): The Unify context (e.g., "Knowledge/table_name" or "ContextName").
        embed_column (str): The name of the vector column to ensure. (eg: "content_emb")
        source_column (str): The name of the source column to embed. (eg: "content_plus_desc")
        derived_expr Optional(str): An optional expression to dynamically derive the source column
            (in case it's not already present) (eg: "str({name}) + ' || ' + str({description})")
        async_embeddings (bool): Whether to generate embeddings asynchronously.

    Returns:
        True if the column was created or any rows were backfilled; False if
        the column was already fully covered (or a targeted ``from_ids`` call
        completed without needing a full-context create).
    """
    created_or_backfilled = False
    scoped_derived_expr = derived_expr
    if scoped_derived_expr is not None:
        # Scope placeholder references to the local logs alias
        scoped_derived_expr = scoped_derived_expr.replace("{", "{lg:")
        ensure_derived_column(
            context=context,
            key=source_column,
            equation=scoped_derived_expr,
            derived=True,
            from_ids=from_ids,
            project=project,
        )
        # Schema can survive rollback while derived source values do not.
        # When not doing a targeted from_ids write, refill orphaned sources.
        if from_ids is None:
            existing_src = unisdk.get_fields(context=context, project=project)
            if source_column in existing_src:
                missing_src = _iter_log_ids(
                    context,
                    filter=f"{source_column} == None",
                    project=project,
                )
                if missing_src:
                    logger.info(
                        "Backfilling orphaned derived source context=%s key=%s "
                        "missing=%s",
                        context,
                        source_column,
                        len(missing_src),
                    )
                    _backfill_derived_for_ids(
                        context,
                        source_column,
                        scoped_derived_expr,
                        from_ids=missing_src,
                        project=project,
                    )
                    created_or_backfilled = True

    embed_expr = (
        f"embed({{lg:{source_column}}}, model='{EMBED_MODEL}', "
        f"async_embeddings={async_embeddings})"
    )

    # Targeted vectorize path (caller supplies row ids).
    if from_ids is not None:
        ensure_derived_column(
            context=context,
            key=embed_column,
            equation=embed_expr,
            derived=True,
            from_ids=from_ids,
            project=project,
        )
        return True

    existing = unisdk.get_fields(context=context, project=project)
    if embed_column not in existing:
        ensure_derived_column(
            context=context,
            key=embed_column,
            equation=embed_expr,
            derived=True,
            from_ids=None,
            project=project,
        )
        return True

    # Field exists: only backfill rows that have source text but no embedding.
    # This self-heals schema-only leftovers after context rollback.
    missing_emb = _iter_log_ids(
        context,
        filter=f"({embed_column} == None) and ({source_column} != None)",
        project=project,
    )
    if not missing_emb:
        return created_or_backfilled

    logger.info(
        "Backfilling orphaned embeddings context=%s key=%s missing=%s",
        context,
        embed_column,
        len(missing_emb),
    )
    _backfill_derived_for_ids(
        context,
        embed_column,
        embed_expr,
        from_ids=missing_emb,
        project=project,
    )
    return True
