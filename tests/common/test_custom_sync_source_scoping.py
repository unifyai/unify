"""Source scoping in the custom-sync reconcile engine.

Symbolic tests over an in-memory store shaped like a manager context. No
backend and no LLM: these pin the invariant that lets more than one source
(the deployment, plus any number of installed bundles) write into a single
context without pruning each other.
"""

from typing import Any, Dict, List

import pytest

from unify.common.custom_sync import (
    DEPLOYMENT_SOURCE_ID,
    CustomSyncAdapter,
    managed_rows_filter,
    reconcile_custom_rows,
    stored_hash_field,
)

WORKFLOW = "draft_email_replies"


class FakeStore:
    """Rows in one context, shared by every source that syncs into it."""

    def __init__(self, rows: List[Dict[str, Any]] | None = None) -> None:
        self.rows: List[Dict[str, Any]] = list(rows or [])

    def owned_by(self, source_id: str) -> set:
        return {r["custom_key"] for r in self.rows if r.get("source_id") == source_id}


class FakeAdapter(CustomSyncAdapter):
    kind = "guidance"

    def __init__(self, store: FakeStore, source_id: str) -> None:
        self._store = store
        self.source_id = source_id

    def _owns(self, row: Dict[str, Any]) -> bool:
        """Mirrors ``managed_rows_filter``: the deployment also claims rows
        written before ``source_id`` existed."""
        if row.get("custom_hash") is None:
            return False
        owner = row.get("source_id")
        return owner == self.source_id or (
            owner is None and self.source_id == DEPLOYMENT_SOURCE_ID
        )

    def live_rows(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._store.rows if self._owns(r)]

    def insert(self, key: str, fields: Dict[str, Any]) -> None:
        self._store.rows.append(dict(fields))

    def update(self, key, live_row, fields) -> None:
        for row in self._store.rows:
            if row.get("custom_key") == key and self._owns(row):
                row.update(fields)
                return
        raise AssertionError(f"no row to update for {key}")

    def delete(self, key, live_row) -> None:
        self._store.rows = [
            r
            for r in self._store.rows
            if not (r.get("custom_key") == key and self._owns(r))
        ]


def entry(key: str, body: str) -> Dict[str, Any]:
    return {"custom_key": key, "custom_hash": f"h({body})", "body": body}


def sync(store: FakeStore, source_id: str, source: Dict[str, Dict[str, Any]]):
    return reconcile_custom_rows(source=source, adapter=FakeAdapter(store, source_id))


@pytest.fixture
def store() -> FakeStore:
    s = FakeStore()
    sync(s, DEPLOYMENT_SOURCE_ID, {"triage": entry("triage", "deploy")})
    sync(s, WORKFLOW, {"draft": entry("draft", "wf")})
    return s


def test_two_sources_coexist_in_one_context(store):
    assert store.owned_by(DEPLOYMENT_SOURCE_ID) == {"triage"}
    assert store.owned_by(WORKFLOW) == {"draft"}


def test_engine_stamps_source_id_on_insert(store):
    assert all(r.get("source_id") for r in store.rows)


def test_resyncing_one_source_does_not_prune_the_other(store):
    sync(store, DEPLOYMENT_SOURCE_ID, {"triage": entry("triage", "deploy")})
    assert store.owned_by(WORKFLOW) == {"draft"}

    sync(store, WORKFLOW, {"draft": entry("draft", "wf")})
    assert store.owned_by(DEPLOYMENT_SOURCE_ID) == {"triage"}


def test_same_custom_key_in_two_sources_stays_two_rows(store):
    sync(
        store,
        DEPLOYMENT_SOURCE_ID,
        {"triage": entry("triage", "deploy"), "daily": entry("daily", "a")},
    )
    sync(store, WORKFLOW, {"draft": entry("draft", "wf"), "daily": entry("daily", "b")})

    daily = [r for r in store.rows if r["custom_key"] == "daily"]
    assert len(daily) == 2
    assert sorted(r["body"] for r in daily) == ["a", "b"]


def test_uninstall_prunes_only_the_leaving_source(store):
    sync(store, WORKFLOW, {})

    assert store.owned_by(WORKFLOW) == set()
    assert store.owned_by(DEPLOYMENT_SOURCE_ID) == {"triage"}


def test_legacy_rows_without_source_id_belong_to_the_deployment():
    legacy = FakeStore(
        [{"custom_key": "old", "custom_hash": "h(old)", "source_id": None}],
    )

    assert FakeAdapter(legacy, DEPLOYMENT_SOURCE_ID).live_rows()
    assert FakeAdapter(legacy, WORKFLOW).live_rows() == []


def test_legacy_rows_are_stamped_on_next_deployment_sync():
    legacy = FakeStore(
        [{"custom_key": "old", "custom_hash": "h(old)", "source_id": None}],
    )

    sync(legacy, DEPLOYMENT_SOURCE_ID, {"old": entry("old", "restamped")})

    assert legacy.rows[0]["source_id"] == DEPLOYMENT_SOURCE_ID


@pytest.mark.parametrize(
    ("source_id", "expected"),
    [
        (DEPLOYMENT_SOURCE_ID, "custom_guidance_hash"),
        (WORKFLOW, f"custom_guidance_hash__{WORKFLOW}"),
    ],
)
def test_hash_slots_do_not_collide(source_id, expected):
    assert stored_hash_field("custom_guidance_hash", source_id) == expected


def test_only_the_deployment_filter_admits_unstamped_rows():
    assert "source_id == None" in managed_rows_filter(DEPLOYMENT_SOURCE_ID)
    assert "source_id == None" not in managed_rows_filter(WORKFLOW)
