"""Unit tests for the shared custom source sync engine.

These pin the reconcile contract itself (``docs/writeups/custom-source-sync.md``)
independently of any manager: diff outcomes, duplicate-key loudness,
per-key failure isolation, collision policies, adoption, update vetoes,
and the hash-storage rules in ``run_custom_sync``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from unify.common.custom_sync import (
    CustomSyncAdapter,
    CustomSyncDuplicateKeyError,
    CustomSyncFieldDrift,
    CustomSyncPartialFailure,
    reconcile_custom_rows,
    require_consumed,
    run_custom_sync,
)


class _RecordingAdapter(CustomSyncAdapter):
    kind = "probe"

    def __init__(
        self,
        live: Optional[List[Dict[str, Any]]] = None,
        collisions: Optional[Dict[str, Dict[str, Any]]] = None,
        adoptable: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self._live = live or []
        self._collisions = collisions or {}
        self._adoptable = adoptable or {}
        self.calls: List[tuple] = []

    def live_rows(self) -> List[Dict[str, Any]]:
        return self._live

    def insert(self, key, fields):
        self.calls.append(("insert", key))

    def update(self, key, live_row, fields):
        self.calls.append(("update", key))

    def delete(self, key, live_row):
        self.calls.append(("delete", key))

    def find_collision(self, key, fields):
        return self._collisions.get(key)

    def remove_collision(self, key, live_row):
        self.calls.append(("remove_collision", key))

    def find_adoptable(self, key, fields):
        return self._adoptable.get(key)

    def adopt(self, key, live_row, fields):
        self.calls.append(("adopt", key))


def _row(key: str, hash_: str, **extra: Any) -> Dict[str, Any]:
    return {"custom_key": key, "custom_hash": hash_, **extra}


def test_insert_update_unchanged_and_prune():
    adapter = _RecordingAdapter(
        live=[_row("stale", "x"), _row("same", "s"), _row("changed", "old")],
    )
    result = reconcile_custom_rows(
        source={
            "new": _row("new", "n"),
            "same": _row("same", "s"),
            "changed": _row("changed", "new"),
        },
        adapter=adapter,
    )
    assert ("insert", "new") in adapter.calls
    assert ("update", "changed") in adapter.calls
    assert ("delete", "stale") in adapter.calls
    assert ("update", "same") not in adapter.calls
    assert result.inserted == 1
    assert result.updated == 1
    assert result.deleted == 1
    assert result.unchanged == 1
    assert result.changed


def test_duplicate_live_keys_raise_before_any_write():
    adapter = _RecordingAdapter(live=[_row("k", "a"), _row("k", "b")])
    with pytest.raises(CustomSyncDuplicateKeyError):
        reconcile_custom_rows(source={"k": _row("k", "c")}, adapter=adapter)
    assert adapter.calls == []


def test_per_key_failures_are_isolated_and_reraised():
    adapter = _RecordingAdapter()
    original_insert = adapter.insert

    def flaky_insert(key, fields):
        if key == "bad":
            raise RuntimeError("boom")
        original_insert(key, fields)

    adapter.insert = flaky_insert
    with pytest.raises(CustomSyncPartialFailure) as exc_info:
        reconcile_custom_rows(
            source={"bad": _row("bad", "b"), "good": _row("good", "g")},
            adapter=adapter,
        )
    assert set(exc_info.value.failures) == {"bad"}
    assert ("insert", "good") in adapter.calls


def test_collision_replace_and_yield():
    replace = _RecordingAdapter(collisions={"k": {"name": "k"}})
    reconcile_custom_rows(source={"k": _row("k", "h")}, adapter=replace)
    assert ("remove_collision", "k") in replace.calls
    assert ("insert", "k") in replace.calls

    yielding = _RecordingAdapter(collisions={"k": {"name": "k"}})
    yielding.collision = "yield"
    result = reconcile_custom_rows(source={"k": _row("k", "h")}, adapter=yielding)
    assert ("insert", "k") not in yielding.calls
    assert result.yielded == 1


def test_adoption_wins_over_collision_and_insert():
    adapter = _RecordingAdapter(
        adoptable={"k": {"name": "k"}},
        collisions={"k": {"name": "k"}},
    )
    result = reconcile_custom_rows(source={"k": _row("k", "h")}, adapter=adapter)
    assert ("adopt", "k") in adapter.calls
    assert ("insert", "k") not in adapter.calls
    assert result.adopted == 1


def test_no_prune_keeps_removed_rows():
    adapter = _RecordingAdapter(live=[_row("stale", "x")])
    adapter.prune = False
    reconcile_custom_rows(source={}, adapter=adapter)
    assert adapter.calls == []


def test_should_update_veto_counts_as_skip():
    adapter = _RecordingAdapter(live=[_row("k", "old")])
    adapter.should_update = lambda key, live_row, fields: False
    result = reconcile_custom_rows(source={"k": _row("k", "new")}, adapter=adapter)
    assert adapter.calls == []
    assert result.skipped == 1


def test_run_custom_sync_hash_short_circuits():
    adapter = _RecordingAdapter()
    stored: List[str] = []
    marked: List[bool] = []

    # Fresh process, hash already current: mark synced, no reconcile.
    changed = run_custom_sync(
        adapter=adapter,
        source={},
        expected_hash="h",
        stored_hash="h",
        already_synced=False,
        mark_synced=lambda: marked.append(True),
        store_hash=stored.append,
    )
    assert changed is False
    assert marked and not stored

    # Hash mismatch: reconcile runs and the new hash is stored.
    changed = run_custom_sync(
        adapter=adapter,
        source={"k": _row("k", "h2")},
        expected_hash="h2",
        stored_hash="h",
        already_synced=False,
        mark_synced=lambda: marked.append(True),
        store_hash=stored.append,
    )
    assert changed is True
    assert stored == ["h2"]


def test_run_custom_sync_defers_hash_on_skipped_updates():
    adapter = _RecordingAdapter(live=[_row("k", "old")])
    adapter.should_update = lambda key, live_row, fields: False
    stored: List[str] = []
    marked: List[bool] = []

    changed = run_custom_sync(
        adapter=adapter,
        source={"k": _row("k", "new")},
        expected_hash="new-agg",
        stored_hash="old-agg",
        already_synced=False,
        mark_synced=lambda: marked.append(True),
        store_hash=stored.append,
    )
    assert changed is True
    assert stored == []
    assert marked == []


def test_run_custom_sync_does_not_store_hash_on_partial_failure():
    adapter = _RecordingAdapter()
    adapter.insert = lambda key, fields: (_ for _ in ()).throw(RuntimeError("boom"))
    stored: List[str] = []

    with pytest.raises(CustomSyncPartialFailure):
        run_custom_sync(
            adapter=adapter,
            source={"k": _row("k", "h")},
            expected_hash="agg",
            stored_hash="",
            already_synced=False,
            mark_synced=lambda: None,
            store_hash=stored.append,
        )
    assert stored == []


def test_require_consumed_raises_on_leftovers():
    require_consumed({}, kind="probe", custom_key="k")
    with pytest.raises(CustomSyncFieldDrift, match="tags"):
        require_consumed({"tags": ["gtm"]}, kind="probe", custom_key="k")
