"""Loading the curated workflow catalogue from disk.

File-only symbolic tests: the loader turns bundle directories into
``WorkflowBundle``s through the same collectors the deployment sync
uses, so what these pin is the manifest contract — identity, strictness,
and the requirement/params parsing the console gallery renders.
"""

import json
from pathlib import Path

import pytest

from unify.workflow_manager.catalog import (
    MANIFEST_FILENAME,
    load_bundle,
    load_catalog,
)

_MANIFEST = """\
slug: daily_briefing
name: Daily briefing
version: "1.2.0"
category: comms
icon_id: briefing
description: Your calendar and the unread that matters, before stand-up.
requirements:
  - slug: gmail
    name: Gmail
    required_secrets: [GMAIL_TOKEN]
  - web
capabilities: [filesystem]
params_schema:
  mailbox:
    required: true
    help: Which mailbox to read.
"""


def _write_bundle(root: Path, slug: str = "daily_briefing") -> Path:
    bundle_dir = root / slug
    bundle_dir.mkdir(parents=True)
    (bundle_dir / MANIFEST_FILENAME).write_text(_MANIFEST)

    guidance_dir = bundle_dir / "guidance"
    guidance_dir.mkdir()
    (guidance_dir / "guidance.jsonl").write_text(
        json.dumps(
            {
                "key": "wf/triage",
                "title": "Triage",
                "content": "Oldest first.",
            },
        )
        + "\n",
    )
    tasks_dir = bundle_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "tasks.jsonl").write_text(
        json.dumps(
            {
                "key": "wf/morning",
                "name": "Morning run",
                "description": "The recurring job.",
                "repeat": [{"frequency": "daily"}],
            },
        )
        + "\n",
    )
    return bundle_dir


def test_load_bundle_reads_identity_and_content(tmp_path: Path):
    bundle = load_bundle(_write_bundle(tmp_path))

    assert bundle.slug == "daily_briefing"
    assert bundle.name == "Daily briefing"
    assert bundle.version == "1.2.0"
    assert bundle.category == "comms"
    assert bundle.icon_id == "briefing"
    assert bundle.capabilities == ("filesystem",)
    assert bundle.params_schema["mailbox"]["required"] is True

    # Only the content directories actually present become surfaces.
    assert sorted(bundle.surfaces) == ["guidance", "tasks"]
    assert "wf/triage" in bundle.surfaces["guidance"]
    assert bundle.surfaces["guidance"]["wf/triage"]["custom_hash"]


def test_load_bundle_parses_requirements_in_both_shapes(tmp_path: Path):
    """A bare string is shorthand for a requirement with nothing to gate
    on yet; the mapping shape carries the secrets that mark it met."""
    bundle = load_bundle(_write_bundle(tmp_path))

    gmail, web = bundle.requirements
    assert gmail.slug == "gmail"
    assert gmail.required_secrets == ("GMAIL_TOKEN",)
    assert not gmail.connected(frozenset())
    assert web.slug == "web"
    assert web.connected(frozenset())


def test_slug_must_match_the_directory_name(tmp_path: Path):
    """The slug is stamped as managed_by on every planted row, so a
    directory rename must never silently re-identify a workflow."""
    bundle_dir = _write_bundle(tmp_path)
    renamed = tmp_path / "renamed_briefing"
    bundle_dir.rename(renamed)

    with pytest.raises(ValueError, match="identity migration"):
        load_bundle(renamed)


def test_manifest_without_identity_is_refused(tmp_path: Path):
    bundle_dir = tmp_path / "nameless"
    bundle_dir.mkdir()
    (bundle_dir / MANIFEST_FILENAME).write_text("slug: nameless\n")

    with pytest.raises(ValueError, match="'name'"):
        load_bundle(bundle_dir)


def test_load_catalog_walks_only_bundle_directories(tmp_path: Path):
    _write_bundle(tmp_path)
    (tmp_path / "not_a_bundle").mkdir()
    (tmp_path / "README.md").write_text("shelf notes\n")

    bundles = load_catalog(tmp_path)
    assert [b.slug for b in bundles] == ["daily_briefing"]


def test_load_catalog_of_a_missing_root_is_empty(tmp_path: Path):
    assert load_catalog(tmp_path / "nowhere") == []


def test_load_catalog_is_strict(tmp_path: Path):
    """The direct loader raises on a malformed bundle — a curated bundle
    silently vanishing from the shelf is worse than a loud failure. The
    boot path isolates per-bundle failures on top of this."""
    _write_bundle(tmp_path)
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / MANIFEST_FILENAME).write_text("slug: broken\n")

    with pytest.raises(ValueError, match="'name'"):
        load_catalog(tmp_path)
