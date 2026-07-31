"""Shared reconcile engine for git-tracked custom source definitions.

Every state manager that syncs deployment sources (tasks, functions,
venvs, guidance, knowledge, contacts, secrets, blacklist, data seeds,
dashboards, integration registry) reconciles through this module. The
contract lives in ``docs/writeups/custom-source-sync.md``; the short
version:

- A deployment-owned row carries ``custom_key`` (stable identity of the
  authored source entry) and ``custom_hash`` (content fingerprint), set
  together, atomically with the row itself.
- The diff loop is implemented once, here. Managers supply a
  :class:`CustomSyncAdapter` with their storage mechanics and declared
  policy knobs, never a bespoke loop.
- Two live managed rows sharing one ``custom_key`` raise
  :class:`CustomSyncDuplicateKeyError` instead of silently picking a
  survivor.
- Per-entry failures are isolated: the pass completes, then raises
  :class:`CustomSyncPartialFailure` so the caller skips storing the
  aggregate hash and the next reconcile retries only the failed keys.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Literal, Mapping, Optional

logger = logging.getLogger(__name__)


class CustomSyncDuplicateKeyError(RuntimeError):
    """Two live managed rows share one ``custom_key``.

    The engine refuses to guess which row is authoritative. Delete the
    stale duplicate (or clear its ``custom_key``/``custom_hash``) and
    re-run the sync.
    """

    def __init__(self, kind: str, custom_key: str) -> None:
        self.kind = kind
        self.custom_key = custom_key
        super().__init__(
            f"Custom {kind} sync found two live rows with "
            f"custom_key={custom_key!r}; refusing to pick a survivor.",
        )


class CustomSyncFieldDrift(RuntimeError):
    """A transforming writer received fields it does not know how to persist.

    Raised by :func:`require_consumed` when a collector emits (and hashes)
    a field that the writer never consumed. Without this check the row is
    stamped with the new hash while the field silently never lands, and
    the entry pins itself as "up to date" forever.
    """

    def __init__(self, kind: str, custom_key: str, leftover: Iterable[str]) -> None:
        names = ", ".join(sorted(leftover))
        super().__init__(
            f"Custom {kind} writer for key={custom_key!r} left fields "
            f"unconsumed: {names}. Teach the writer about the field or "
            "remove it from the collector.",
        )


class CustomSyncPartialFailure(Exception):
    """One or more source entries failed to reconcile.

    Successful entries are still written. Callers must NOT store the
    aggregate source hash when this is raised, so the next reconcile
    retries the failed entries (unchanged ones short-circuit on their
    per-row hash).
    """

    def __init__(self, kind: str, failures: Mapping[str, BaseException]) -> None:
        self.kind = kind
        self.failures = dict(failures)
        names = ", ".join(sorted(self.failures))
        super().__init__(f"Custom {kind} sync partially failed for: {names}")


def require_consumed(
    payload: Mapping[str, Any],
    *,
    kind: str,
    custom_key: str,
) -> None:
    """Assert a transforming writer consumed every collected field."""

    if payload:
        raise CustomSyncFieldDrift(kind, custom_key, payload.keys())


@dataclass
class CustomSyncResult:
    """Counts from one reconcile pass."""

    inserted: int = 0
    updated: int = 0
    adopted: int = 0
    deleted: int = 0
    unchanged: int = 0
    skipped: int = 0
    yielded: int = 0
    failures: Dict[str, BaseException] = field(default_factory=dict)

    @property
    def changed(self) -> bool:
        return bool(self.inserted or self.updated or self.adopted or self.deleted)


class CustomSyncAdapter:
    """Manager-specific half of the reconcile contract.

    Subclasses implement the storage mechanics (``live_rows``,
    ``insert``, ``update``, ``delete``) and declare policy deviations as
    the named knobs below — never by forking the diff loop.

    Policy knobs:

    - ``prune``: delete managed rows whose key left the source
      (secrets set ``False``: credentials are never auto-deleted).
    - ``collision``: what to do when the source key is new but an
      unmanaged row occupies its natural slot. ``"replace"`` deletes the
      user-added row and inserts the source row; ``"yield"`` leaves the
      user's row in place and skips the source entry (secrets).
    - ``max_workers``: parallel per-key upserts. Updates run
      concurrently; adoption/collision/insert handling is serialized
      under one lock so key probes cannot race.
    """

    kind: str = "rows"
    prune: bool = True
    collision: Literal["replace", "yield"] = "replace"
    max_workers: int = 1

    def live_rows(self) -> Iterable[Dict[str, Any]]:
        """Yield every managed row (``custom_hash`` set), including its
        ``custom_key`` and any fields ``update``/``delete`` need back."""
        raise NotImplementedError

    def insert(self, key: str, fields: Dict[str, Any]) -> None:
        """Create the row, writing ``custom_key``/``custom_hash`` in the
        same write as the rest of the fields."""
        raise NotImplementedError

    def update(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> None:
        raise NotImplementedError

    def delete(self, key: str, live_row: Dict[str, Any]) -> None:
        raise NotImplementedError

    def transform(self, key: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve source-shaped fields into row-shaped fields
        (e.g. function names to ids). Runs inside per-key isolation."""
        return fields

    def should_update(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> bool:
        """Veto an update this pass (e.g. a task run is in flight). The
        aggregate hash still isn't stored if anything was skipped, so the
        next reconcile retries."""
        return True

    def find_adoptable(
        self,
        key: str,
        fields: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Return a pre-existing row this entry should claim (stamping
        identity in place) instead of inserting a duplicate."""
        return None

    def adopt(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> None:
        self.update(key, live_row, fields)

    def find_collision(
        self,
        key: str,
        fields: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Return an unmanaged row occupying this entry's natural slot."""
        return None

    def remove_collision(self, key: str, live_row: Dict[str, Any]) -> None:
        raise NotImplementedError


def _index_live_rows(adapter: CustomSyncAdapter) -> Dict[str, Dict[str, Any]]:
    live: Dict[str, Dict[str, Any]] = {}
    for row in adapter.live_rows():
        key = row.get("custom_key")
        if not key:
            continue
        key = str(key)
        if key in live:
            raise CustomSyncDuplicateKeyError(adapter.kind, key)
        live[key] = row
    return live


def _upsert_one(
    *,
    adapter: CustomSyncAdapter,
    key: str,
    fields: Dict[str, Any],
    live: Dict[str, Dict[str, Any]],
    insert_lock: threading.Lock,
) -> str:
    fields = adapter.transform(key, dict(fields))
    if key in live:
        live_row = live[key]
        if live_row.get("custom_hash") == fields.get("custom_hash"):
            logger.debug("Custom %s unchanged: %s", adapter.kind, key)
            return "unchanged"
        if not adapter.should_update(key, live_row, fields):
            logger.warning(
                "Skipping update for custom %s key=%s this pass",
                adapter.kind,
                key,
            )
            return "skipped"
        logger.info("Updating custom %s: %s", adapter.kind, key)
        adapter.update(key, live_row, fields)
        return "updated"

    with insert_lock:
        adoptable = adapter.find_adoptable(key, fields)
        if adoptable is not None:
            logger.info("Adopting unmanaged %s row: %s", adapter.kind, key)
            adapter.adopt(key, adoptable, fields)
            return "adopted"
        collision = adapter.find_collision(key, fields)
        if collision is not None:
            if adapter.collision == "yield":
                logger.info(
                    "Skipping custom %s %s: user-owned row exists",
                    adapter.kind,
                    key,
                )
                return "yielded"
            logger.info(
                "Overwriting user-added %s with custom definition: %s",
                adapter.kind,
                key,
            )
            adapter.remove_collision(key, collision)
        logger.info("Inserting custom %s: %s", adapter.kind, key)
        adapter.insert(key, fields)
        return "inserted"


def reconcile_custom_rows(
    *,
    source: Mapping[str, Dict[str, Any]],
    adapter: CustomSyncAdapter,
) -> CustomSyncResult:
    """Run one full diff of source entries against live managed rows.

    Raises :class:`CustomSyncDuplicateKeyError` before touching anything
    if the live rows are ambiguous, and :class:`CustomSyncPartialFailure`
    after the pass if any entry failed. Skipped updates
    (``should_update`` veto) count as failures for hash-storage purposes
    but carry no exception.
    """

    live = _index_live_rows(adapter)
    result = CustomSyncResult()

    workers = max(1, min(adapter.max_workers, len(source) or 1))
    insert_lock = threading.Lock()

    def run(key: str, fields: Dict[str, Any]) -> str:
        try:
            return _upsert_one(
                adapter=adapter,
                key=key,
                fields=fields,
                live=live,
                insert_lock=insert_lock,
            )
        except Exception as exc:
            result.failures[key] = exc
            logger.exception(
                "Failed to sync custom %s %r; continuing with remaining entries",
                adapter.kind,
                key,
            )
            return "failed"

    if workers == 1:
        outcomes = [run(key, fields) for key, fields in source.items()]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(run, key, fields) for key, fields in source.items()
            ]
            outcomes = [future.result() for future in as_completed(futures)]

    for outcome in outcomes:
        if outcome in (
            "inserted",
            "updated",
            "adopted",
            "unchanged",
            "skipped",
            "yielded",
        ):
            setattr(result, outcome, getattr(result, outcome) + 1)

    if adapter.prune:
        for key, live_row in live.items():
            if key in source:
                continue
            try:
                logger.info("Deleting removed custom %s: %s", adapter.kind, key)
                adapter.delete(key, live_row)
                result.deleted += 1
            except Exception as exc:
                result.failures[key] = exc
                logger.exception(
                    "Failed to delete removed custom %s %r; continuing",
                    adapter.kind,
                    key,
                )

    if result.failures:
        raise CustomSyncPartialFailure(adapter.kind, result.failures)
    return result


def run_custom_sync(
    *,
    adapter: CustomSyncAdapter,
    source: Mapping[str, Dict[str, Any]],
    expected_hash: str,
    stored_hash: str,
    already_synced: bool,
    mark_synced: Callable[[], None],
    store_hash: Callable[[str], None],
) -> bool:
    """Aggregate-hash short-circuit around :func:`reconcile_custom_rows`.

    Returns True when a reconcile pass ran to completion. Skipped
    updates leave the stored hash untouched so the next boot retries
    them.
    """

    if already_synced and stored_hash == expected_hash:
        return False
    if stored_hash == expected_hash:
        logger.debug("Custom %s hash matches, skipping sync", adapter.kind)
        mark_synced()
        return False

    logger.info(
        "Custom %s hash mismatch (current=%s, expected=%s), syncing...",
        adapter.kind,
        stored_hash,
        expected_hash,
    )
    result = reconcile_custom_rows(source=source, adapter=adapter)
    if result.skipped:
        # A vetoed update (e.g. a task run in flight) must not be lost:
        # storing the hash would pin the stale row as "up to date" until
        # the source changes again. Leave both the hash and the synced
        # flag alone so the next reconcile retries.
        logger.info(
            "Custom %s sync deferred %d entries; leaving hash unstored for retry",
            adapter.kind,
            result.skipped,
        )
        return True
    store_hash(expected_hash)
    mark_synced()
    return True
