"""Shared reconcile engine for git-tracked custom source definitions.

Every state manager that syncs deployment sources (tasks, functions,
venvs, guidance, knowledge, contacts, secrets, blacklist, data seeds,
integration registry) reconciles through this module. The
contract lives in ``docs/writeups/custom-source-sync.md``; the short
version:

- A managed row carries ``custom_key`` (stable identity of the authored
  source entry), ``custom_hash`` (content fingerprint), and ``managed_by``
  (which source reconciles it), set together, atomically with the row
  itself.
- The diff loop is implemented once, here. Managers supply a
  :class:`CustomSyncAdapter` with their storage mechanics and declared
  policy knobs, never a bespoke loop.
- Every pass is scoped to one ``managed_by``. Two sources syncing into the
  same context see disjoint row sets, so neither prunes the other's rows.
- Two live managed rows of one source sharing one ``custom_key`` raise
  :class:`CustomSyncDuplicateKeyError` instead of silently picking a
  survivor.
- Per-entry failures are isolated: the pass completes, then raises
  :class:`CustomSyncPartialFailure` so the caller skips storing the
  aggregate hash and the next reconcile retries only the failed keys.
- A surface may hand one row's ownership to the user by clearing
  ``managed_by`` and keeping ``custom_key``. The pass then reports the key
  as ``released`` and touches nothing — see :func:`released_rows_filter`.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Literal, Mapping, Optional

logger = logging.getLogger(__name__)

MANAGED_BY_DEPLOYMENT = "deployment"
"""The ``managed_by`` value for rows the deployment's own sources reconcile.

Rows written before ``managed_by`` existed carry no value for it — including
the short-lived ``source_id`` spelling, whose only writer was the deployment.
Such a row is the deployment's, so the deployment's reconcile admits it via
the null branch below and stamps it on the next content change; other
sources never match it.
"""


def managed_rows_filter(managed_by: str) -> str:
    """Filter selecting the managed rows one source owns.

    Every adapter's ``live_rows`` must use this rather than a bare
    ``custom_hash != None``: an unscoped query hands a source its
    siblings' rows, and prune then deletes them.
    """

    if managed_by == MANAGED_BY_DEPLOYMENT:
        return (
            "custom_hash != None and "
            f"(managed_by == '{managed_by}' or managed_by == None)"
        )
    return f"custom_hash != None and managed_by == '{managed_by}'"


CUSTOM_RELEASED_FIELD = "custom_released"
"""Marks a planted row whose ownership has passed to the user.

Releasing clears ``managed_by`` so no source reconciles the row, and keeps
``custom_key`` so it stays traceable to the entry it grew from. The absence
of provenance alone cannot say *why* it is absent — a row written before
``managed_by`` existed also has none, and that one the deployment still
owns. This flag is the positive answer, so releasing is never inferred
from a null.
"""


def released_rows_filter(key: str) -> str:
    """Filter selecting a row this source planted that the user now owns.

    A released row is invisible to :func:`managed_rows_filter`, so without
    this probe the next pass reads its key as missing and plants a second
    copy beside the user's edited one.
    """

    return f"custom_key == '{key}' and {CUSTOM_RELEASED_FIELD} == True"


def stored_hash_field(base_field: str, managed_by: str) -> str:
    """Meta field holding one source's aggregate hash.

    The deployment keeps the original unsuffixed field so existing
    installations do not re-sync on upgrade; every other source gets its
    own slot instead of fighting over that one.
    """

    if managed_by == MANAGED_BY_DEPLOYMENT:
        return base_field
    return f"{base_field}__{managed_by}"


class CustomSyncDuplicateKeyError(RuntimeError):
    """Two live managed rows of one source share one ``custom_key``.

    The engine refuses to guess which row is authoritative. Delete the
    stale duplicate (or clear its ``custom_key``/``custom_hash``) and
    re-run the sync. Rows of *different* sources may share a key freely;
    they are distinct rows and never collide.
    """

    def __init__(self, kind: str, custom_key: str, managed_by: str) -> None:
        self.kind = kind
        self.custom_key = custom_key
        self.managed_by = managed_by
        super().__init__(
            f"Custom {kind} sync found two live rows for source "
            f"{managed_by!r} with custom_key={custom_key!r}; refusing to "
            "pick a survivor.",
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
    released: int = 0
    failures: Dict[str, BaseException] = field(default_factory=dict)

    @property
    def changed(self) -> bool:
        return bool(self.inserted or self.updated or self.adopted or self.deleted)


class CustomSyncAdapter:
    """Manager-specific half of the reconcile contract.

    Subclasses implement the storage mechanics (``live_rows``,
    ``insert``, ``update``, ``delete``) and declare policy deviations as
    the named knobs below — never by forking the diff loop.

    Scoping by ``managed_by`` is not optional. ``live_rows`` and
    ``find_collision`` must both restrict to :attr:`managed_by` — use
    :func:`managed_rows_filter` — because the loop prunes every managed
    row whose key left the source.
    An unscoped query therefore deletes whatever a sibling source planted
    in the same context. ``insert`` need not write ``managed_by`` itself:
    the loop stamps it into the fields after ``transform``, so a writer
    that persists its field dict wholesale gets it for free.

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
    managed_by: str = MANAGED_BY_DEPLOYMENT
    prune: bool = True
    collision: Literal["replace", "yield"] = "replace"
    max_workers: int = 1

    def live_rows(self) -> Iterable[Dict[str, Any]]:
        """Yield this source's managed rows, including their ``custom_key``
        and any fields ``update``/``delete`` need back.

        Scope the query with ``managed_rows_filter(self.managed_by)``.
        """
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
        """Overwrite the row, reaching it by the storage handle carried on
        *live_row*.

        Do not re-query by ``managed_by``: a legacy row adopted by the
        deployment has not been stamped yet, and this write is what stamps
        it.
        """
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

    def derived_stale(
        self,
        key: str,
        live_row: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> bool:
        """Return True when a field the writer derives from another store
        (e.g. a function name resolved to a numeric id) no longer matches
        its referent, forcing an update despite an unchanged content hash.

        The content hash fingerprints the authored source only, so it is
        blind to the referenced store being rewritten underneath the row.
        """
        return False

    def find_released(
        self,
        key: str,
        fields: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Return this entry's row if the user has taken ownership of it.

        Surfaces whose rows a user may edit (a planted task) hand ownership
        over by clearing ``managed_by`` and keeping ``custom_key``. That
        removes the row from :meth:`live_rows`, so without this probe the
        key reads as missing and the pass plants a second copy beside the
        edited one.

        Implement with :func:`released_rows_filter`; leave it unimplemented
        on a library surface whose rows refuse direct edits, so nothing can
        drift out from under the source.
        """
        return None

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
        """Return an unmanaged row occupying this entry's natural slot.

        Scope the probe to :attr:`managed_by`. A sibling source's row is
        not a collision — it is another source's property, and replacing
        it silently uninstalls part of that source.
        """
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
            raise CustomSyncDuplicateKeyError(adapter.kind, key, adapter.managed_by)
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
    # Stamped after transform so transforms stay source-agnostic and the
    # collected content hash is identical whoever installs the bundle.
    fields["managed_by"] = adapter.managed_by
    if key in live:
        live_row = live[key]
        if live_row.get("custom_hash") == fields.get("custom_hash"):
            if not adapter.derived_stale(key, live_row, fields):
                logger.debug("Custom %s unchanged: %s", adapter.kind, key)
                return "unchanged"
            logger.info(
                "Custom %s source unchanged but derived fields stale; " "updating: %s",
                adapter.kind,
                key,
            )
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
        released = adapter.find_released(key, fields)
        if released is not None:
            logger.info(
                "Custom %s %s is owned by the user; leaving it alone",
                adapter.kind,
                key,
            )
            return "released"
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

    Scoped to ``adapter.managed_by`` throughout: the live index, the
    collision probes, and the prune all see only that source's rows.

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
            "released",
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
