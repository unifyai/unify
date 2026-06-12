"""Integration tests for the global builtins guidance catalogue.

Each test seeds a dedicated public-read builtins project (unique name per
test, settings-overridden) so parallel runs never contend on the shared
platform catalogue, then exercises the GuidanceManager read federation,
multi-term read-only search, delta seeding, and mutation refusal.
"""

from __future__ import annotations

import uuid

import pytest
import unify

from tests.helpers import _handle_project
from unity.guidance_manager.builtins_catalog import (
    BUILTINS_GUIDANCE_CONTEXT,
    CONTENT_EMBED_HEAD_CHARS,
    load_snapshot,
    seed_builtin_guidance,
    stable_guidance_id,
)
from unity.guidance_manager.guidance_manager import GuidanceManager
from unity.settings import SETTINGS

_ENTRIES = {
    "test/ffmpeg-frames": {
        "title": "[test] ffmpeg-frames",
        "content": (
            "Extract frames from a video file. Use ffmpeg with an output "
            "pattern to grab still images from any video."
        ),
    },
    "test/arxiv-search": {
        "title": "[test] arxiv-search",
        "content": (
            "Search arXiv for academic papers. Query the arXiv API with "
            "keywords and parse the Atom feed of results."
        ),
    },
}


@pytest.fixture
def builtins_test_project(monkeypatch):
    """Point the builtins project at a unique per-test name and clean it up."""
    name = f"BuiltinsTest-{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(SETTINGS, "UNITY_BUILTINS_PROJECT", name)
    yield name
    try:
        unify.delete_project(name)
    except Exception:
        pass


def _builtin_rows(project: str) -> dict[str, dict]:
    logs = unify.get_logs(
        project=project,
        context=BUILTINS_GUIDANCE_CONTEXT,
        from_fields=["guidance_id", "title", "content", "is_builtin"],
    )
    return {log.entries["title"]: log.entries for log in logs}


# --------------------------------------------------------------------------- #
# Seeding                                                                      #
# --------------------------------------------------------------------------- #


@_handle_project
def test_seed_builtin_guidance_delta_and_idempotent(builtins_test_project):
    project = builtins_test_project

    assert seed_builtin_guidance(entries=_ENTRIES) is True
    rows = _builtin_rows(project)
    assert set(rows) == {entry["title"] for entry in _ENTRIES.values()}
    for entry in _ENTRIES.values():
        row = rows[entry["title"]]
        assert row["is_builtin"] is True
        assert row["guidance_id"] == stable_guidance_id(entry["title"])
        assert row["content"] == entry["content"]

    # Converged: re-seeding writes nothing.
    assert seed_builtin_guidance(entries=_ENTRIES) is False

    # Delta: only the changed skill is rewritten; the other row is untouched.
    changed = {key: dict(entry) for key, entry in _ENTRIES.items()}
    changed["test/arxiv-search"]["content"] = "Updated arXiv instructions."
    assert seed_builtin_guidance(entries=changed) is True
    rows = _builtin_rows(project)
    assert rows["[test] arxiv-search"]["content"] == "Updated arXiv instructions."
    assert rows["[test] ffmpeg-frames"]["content"] == (
        _ENTRIES["test/ffmpeg-frames"]["content"]
    )

    # Removal: skills dropped from the snapshot disappear from the catalogue.
    only_ffmpeg = {"test/ffmpeg-frames": _ENTRIES["test/ffmpeg-frames"]}
    assert seed_builtin_guidance(entries=only_ffmpeg) is True
    assert set(_builtin_rows(project)) == {"[test] ffmpeg-frames"}


@_handle_project
def test_seed_builtin_guidance_empty_snapshot_is_noop(builtins_test_project):
    assert seed_builtin_guidance(entries={}) is False
    assert seed_builtin_guidance(entries={}) is False
    assert _builtin_rows(builtins_test_project) == {}


def test_default_catalogue_is_converged():
    """The session-start seeding already converged the shared catalogue."""
    assert seed_builtin_guidance() is False


# --------------------------------------------------------------------------- #
# Default library (the committed snapshot of 14 Anthropic skills)              #
# --------------------------------------------------------------------------- #


@_handle_project
def test_default_library_seeds_and_surfaces_through_guidance_manager(
    builtins_test_project,
):
    snapshot = load_snapshot()
    assert len(snapshot) == 14

    # Several skills exceed the backend's per-input embedding limit; seeding
    # must still succeed because ranking embeds a truncated content head.
    assert any(
        len(entry["content"]) > CONTENT_EMBED_HEAD_CHARS for entry in snapshot.values()
    )
    assert seed_builtin_guidance() is True

    gm = GuidanceManager()
    builtin_rows = gm.filter(filter="is_builtin == True", limit=100)
    assert {row.title for row in builtin_rows} == {
        entry["title"] for entry in snapshot.values()
    }
    assert gm._num_items() == 14

    # List-style reads return bounded previews (large skills would otherwise
    # flood the caller's context window), each pointing at get_guidance.
    from unity.guidance_manager.guidance_manager import GUIDANCE_PREVIEW_CHARS

    preview_slack = 200  # truncation marker text
    for row in builtin_rows:
        assert len(row.content) <= GUIDANCE_PREVIEW_CHARS + preview_slack
    truncated = [
        row for row in builtin_rows if "get_guidance(guidance_id=" in row.content
    ]
    assert truncated, "expected at least one truncated preview"

    # get_guidance returns the complete content verbatim, including entries
    # far beyond both the preview cap and the embedding head window.
    by_title = {entry["title"]: entry for entry in snapshot.values()}
    for row in truncated:
        full = gm.get_guidance(guidance_id=row.guidance_id)
        assert full.content == by_title[full.title]["content"]
        assert len(full.content) > GUIDANCE_PREVIEW_CHARS


@_handle_project
def test_default_library_semantic_search(builtins_test_project):
    seed_builtin_guidance()
    gm = GuidanceManager()

    results = gm.search(
        references={"content": "create a powerpoint presentation slide deck"},
        k=3,
    )
    assert results and results[0].title == "[anthropic] pptx"

    multi = gm.search(
        references={"content": "fill out pdf form fields", "title": "pdf"},
        k=3,
    )
    assert multi and multi[0].title == "[anthropic] pdf"
    assert all(row.is_builtin for row in multi)


@_handle_project
def test_get_guidance_resolves_tenant_and_builtin_entries(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()
    outcome = gm.add_guidance(title="mine", content="my own entry")
    own_id = outcome["details"]["guidance_id"]

    own = gm.get_guidance(guidance_id=own_id)
    assert (own.title, own.content, own.is_builtin) == ("mine", "my own entry", False)

    builtin_id = stable_guidance_id("[test] arxiv-search")
    builtin = gm.get_guidance(guidance_id=builtin_id)
    assert builtin.is_builtin is True
    assert builtin.content == _ENTRIES["test/arxiv-search"]["content"]

    with pytest.raises(ValueError, match="No guidance found"):
        gm.get_guidance(guidance_id=999999999)


# --------------------------------------------------------------------------- #
# Read federation                                                              #
# --------------------------------------------------------------------------- #


@_handle_project
def test_guidance_reads_blend_builtins_and_tenant_entries(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()
    gm.add_guidance(
        title="My deploy checklist",
        content="Run the deploy script and watch the logs.",
    )

    rows = gm.filter(limit=100)
    by_title = {row.title: row for row in rows}
    assert "My deploy checklist" in by_title
    assert by_title["My deploy checklist"].is_builtin is False
    for entry in _ENTRIES.values():
        assert entry["title"] in by_title
        assert by_title[entry["title"]].is_builtin is True
        assert by_title[entry["title"]].guidance_id == stable_guidance_id(
            entry["title"],
        )

    assert gm._num_items() == 3

    # Filtering on the provenance flag targets each population explicitly.
    builtin_only = gm.filter(filter="is_builtin == True", limit=100)
    assert {row.title for row in builtin_only} == {
        entry["title"] for entry in _ENTRIES.values()
    }


@_handle_project
def test_single_term_search_returns_builtins(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()

    results = gm.search(
        references={"content": "extract still images from a video with ffmpeg"},
        k=2,
    )
    assert results
    assert results[0].title == "[test] ffmpeg-frames"
    assert results[0].is_builtin is True


@_handle_project
def test_multi_term_search_combines_builtins_scores(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()

    results = gm.search(
        references={
            "content": "find academic papers about machine learning",
            "title": "arxiv search",
        },
        k=2,
    )
    assert results
    assert results[0].title == "[test] arxiv-search"
    assert results[0].is_builtin is True


@_handle_project
def test_exclude_ids_apply_to_builtins(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()
    excluded = stable_guidance_id("[test] ffmpeg-frames")
    gm.exclude_ids = frozenset({excluded})

    titles = {row.title for row in gm.filter(limit=100)}
    assert "[test] ffmpeg-frames" not in titles
    assert "[test] arxiv-search" in titles


# --------------------------------------------------------------------------- #
# Immutability                                                                 #
# --------------------------------------------------------------------------- #


@_handle_project
def test_update_and_delete_builtin_guidance_refused(builtins_test_project):
    seed_builtin_guidance(entries=_ENTRIES)
    gm = GuidanceManager()
    builtin_id = stable_guidance_id("[test] arxiv-search")

    with pytest.raises(ValueError, match="built-in"):
        gm.update_guidance(guidance_id=builtin_id, content="tampered")
    with pytest.raises(ValueError, match="built-in"):
        gm.delete_guidance(guidance_id=builtin_id)

    # The catalogue row is untouched and tenant CRUD still works normally.
    rows = _builtin_rows(builtins_test_project)
    assert rows["[test] arxiv-search"]["content"] == (
        _ENTRIES["test/arxiv-search"]["content"]
    )
    outcome = gm.add_guidance(title="mine", content="my own entry")
    own_id = outcome["details"]["guidance_id"]
    gm.update_guidance(guidance_id=own_id, content="my updated entry")
    gm.delete_guidance(guidance_id=own_id)
