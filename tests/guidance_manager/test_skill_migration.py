"""Tests for the SKILL.md → GuidanceManager migration utilities.

Three layers:

* Pure parsing / mapping tests (no backend) covering frontmatter parsing,
  discovery, title namespacing, and content composition for both the
  OpenClaw (single-line JSON ``metadata``) and HermesAgent (nested YAML
  ``metadata``) flavours of the agentskills.io standard.
* Builtins-import tests (no backend, local git fixtures) covering manifest
  parsing, pin verification (SHA + directory integrity hash), snapshot
  building, drift detection, and stable-id determinism.
* End-to-end tests that build a tiny on-disk skill tree and import it into a
  real ``GuidanceManager`` (under ``@_handle_project``), exercising the
  add / skip / overwrite conflict paths.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts.skill_migration.builtins_import import (
    MANIFEST_PATH,
    PinnedSkill,
    build_snapshot_entries,
    check_drift,
    directory_integrity_hash,
    load_manifest,
    pin_skills_at_head,
    write_manifest,
    write_snapshot,
)
from scripts.skill_migration.skill_to_guidance import (
    SkillMigrator,
    compose_guidance_content,
    discover_skills,
    guidance_title,
    parse_skill_file,
    split_frontmatter,
)
from unify.guidance_manager.builtins_catalog import (
    entry_hash,
    load_snapshot,
    stable_guidance_id,
)
from unify.guidance_manager.guidance_manager import GuidanceManager
from tests.helpers import _handle_project

# --------------------------------------------------------------------------- #
# Fixtures / helpers                                                           #
# --------------------------------------------------------------------------- #


def _write_skill(
    root: Path,
    name: str,
    *,
    description: str = "",
    body: str = "body",
    metadata_block: str = "",
    scripts: dict[str, str] | None = None,
    references: dict[str, str] | None = None,
) -> Path:
    """Create ``<root>/<name>/SKILL.md`` (+ optional scripts/refs)."""
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    fm = f"---\nname: {name}\ndescription: {description}\n{metadata_block}---\n\n{body}\n"
    (skill_dir / "SKILL.md").write_text(fm, encoding="utf-8")
    for rel, content in (scripts or {}).items():
        p = skill_dir / "scripts" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    for rel, content in (references or {}).items():
        p = skill_dir / "references" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return skill_dir / "SKILL.md"


# --------------------------------------------------------------------------- #
# Pure parsing / mapping tests                                                 #
# --------------------------------------------------------------------------- #


def test_split_frontmatter_openclaw_json_metadata():
    text = (
        "---\n"
        "name: github\n"
        "description: GitHub ops\n"
        'metadata: { "openclaw": { "emoji": "🐙", "tags": ["git"] } }\n'
        "---\n\n"
        "# GitHub Skill\n\nUse gh.\n"
    )
    fm, body = split_frontmatter(text)
    assert fm["name"] == "github"
    assert fm["metadata"]["openclaw"]["emoji"] == "🐙"
    assert body.startswith("# GitHub Skill")


def test_split_frontmatter_hermes_nested_yaml():
    text = (
        "---\n"
        "name: arxiv\n"
        "description: search arxiv\n"
        "metadata:\n"
        "  hermes:\n"
        "    tags: [research, papers]\n"
        "---\n\n"
        "Body here.\n"
    )
    fm, body = split_frontmatter(text)
    assert fm["metadata"]["hermes"]["tags"] == ["research", "papers"]
    assert body.strip() == "Body here."


def test_split_frontmatter_no_frontmatter():
    fm, body = split_frontmatter("# Just markdown\n\ncontent")
    assert fm == {}
    assert body.startswith("# Just markdown")


def test_split_frontmatter_unterminated_fence():
    text = "---\nname: broken\nno closing fence here\n"
    fm, body = split_frontmatter(text)
    assert fm == {}
    assert "no closing fence" in body


def test_parse_skill_file_falls_back_to_dir_name(tmp_path: Path):
    skill_dir = tmp_path / "skills" / "weather"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\ndescription: forecast\n---\n\nbody\n",
        encoding="utf-8",
    )
    skill = parse_skill_file(
        skill_dir / "SKILL.md",
        source="openclaw",
        repo_root=tmp_path,
    )
    assert skill.name == "weather"  # from directory name
    assert skill.description == "forecast"
    assert skill.rel_path == "skills/weather/SKILL.md"


def test_discover_skills_dedups_and_sorts(tmp_path: Path):
    _write_skill(tmp_path / "skills", "beta")
    _write_skill(tmp_path / "skills", "alpha")
    _write_skill(tmp_path / "optional-skills", "gamma")

    skills = discover_skills(
        [tmp_path / "skills", tmp_path / "optional-skills", tmp_path / "missing"],
        source="hermes",
        repo_root=tmp_path,
    )
    # Deterministic ordering is by relative path; missing roots are ignored.
    rel_paths = [s.rel_path for s in skills]
    assert rel_paths == sorted(rel_paths)
    assert {s.name for s in skills} == {"alpha", "beta", "gamma"}


def test_guidance_title_prefix_and_truncation(tmp_path: Path):
    skill_dir = tmp_path / "skills" / "longskill"
    skill_dir.mkdir(parents=True)
    long_name = "x" * 300
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {long_name}\ndescription: d\n---\n\nbody\n",
        encoding="utf-8",
    )
    skill = parse_skill_file(
        skill_dir / "SKILL.md",
        source="openclaw",
        repo_root=tmp_path,
    )
    title = guidance_title(skill, title_prefix="[openclaw] ")
    assert title.startswith("[openclaw] ")
    assert len(title) <= 200


def test_compose_folds_description_inlines_scripts_and_footer(tmp_path: Path):
    md = _write_skill(
        tmp_path / "skills",
        "video-frames",
        description="Extract frames using ffmpeg.",
        body="## Quick start\n\nRun the script.",
        metadata_block='metadata: { "openclaw": { "tags": ["video"] } }\n',
        scripts={"frame.sh": '#!/usr/bin/env bash\nffmpeg -i "$1" out.jpg\n'},
        references={"notes.md": "extra docs"},
    )
    skill = parse_skill_file(md, source="openclaw", repo_root=tmp_path)
    content = compose_guidance_content(skill, inline_scripts=True)

    assert "Extract frames using ffmpeg." in content  # description folded in
    assert "## Quick start" in content  # body preserved
    assert "## Bundled scripts (textual reference)" in content
    assert "ffmpeg -i" in content  # script inlined verbatim
    assert "### scripts/frame.sh" in content
    assert "Source: openclaw" in content  # provenance footer
    assert "Tags: video" in content
    assert "notes.md" in content  # references listed, not inlined
    assert "extra docs" not in content


def test_compose_can_skip_script_inlining(tmp_path: Path):
    md = _write_skill(
        tmp_path / "skills",
        "demo",
        description="d",
        scripts={"run.py": "print('hi')\n"},
    )
    skill = parse_skill_file(md, source="hermes", repo_root=tmp_path)
    content = compose_guidance_content(skill, inline_scripts=False)
    assert "Bundled scripts" not in content
    assert "print('hi')" not in content


def test_build_entries_is_pure(tmp_path: Path):
    _write_skill(tmp_path / "skills", "alpha", description="first")
    _write_skill(tmp_path / "skills", "beta", description="second")
    skills = discover_skills(
        [tmp_path / "skills"],
        source="openclaw",
        repo_root=tmp_path,
    )
    migrator = SkillMigrator(skills, title_prefix="[openclaw] ")
    entries = migrator.build_entries()
    assert [e.title for e in entries] == ["[openclaw] alpha", "[openclaw] beta"]
    assert all(e.source == "openclaw" for e in entries)
    assert "first" in entries[0].content


def test_compose_folds_compatibility_into_content(tmp_path: Path):
    md = _write_skill(
        tmp_path / "skills",
        "needs-node",
        description="Run node tooling.",
        body="Use npx.",
        metadata_block="compatibility: Requires node >= 20 and network access\n",
    )
    skill = parse_skill_file(md, source="openclaw", repo_root=tmp_path)
    content = compose_guidance_content(skill)
    assert "Compatibility: Requires node >= 20 and network access" in content


# --------------------------------------------------------------------------- #
# Builtins import: manifest, pin verification, snapshot, drift                 #
# --------------------------------------------------------------------------- #


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _make_skill_repo(tmp_path: Path) -> tuple[Path, str]:
    """Create a local git repo with two committed skills; return (repo, sha)."""
    repo = tmp_path / "upstream"
    repo.mkdir()
    _git("init", "-q", cwd=repo)
    _git("config", "user.email", "test@test.invalid", cwd=repo)
    _git("config", "user.name", "test", cwd=repo)
    # Allow fetching pinned (non-HEAD) commits from this fixture repo.
    _git("config", "uploadpack.allowAnySHA1InWant", "true", cwd=repo)
    _write_skill(
        repo / "skills",
        "ffmpeg-frames",
        description="Extract frames from a video.",
        body="Use ffmpeg to grab a frame.",
        scripts={"frame.sh": '#!/usr/bin/env bash\nffmpeg -i "$1" out.jpg\n'},
    )
    _write_skill(
        repo / "skills",
        "arxiv-search",
        description="Search arXiv for papers.",
        body="Query the arXiv API.",
    )
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "add skills", cwd=repo)
    return repo, _git("rev-parse", "HEAD", cwd=repo)


def _pin(repo: Path, sha: str, name: str, *, key: str | None = None) -> PinnedSkill:
    return PinnedSkill(
        key=key or f"test/{name}",
        source="test",
        repo=str(repo),
        path=f"skills/{name}",
        commit=sha,
        integrity=directory_integrity_hash(repo / "skills" / name),
    )


def test_load_manifest_parses_and_validates(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pin = _pin(repo, sha, "arxiv-search")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"skills": [pin.__dict__]}), encoding="utf-8")

    pins = load_manifest(manifest)
    assert pins == [pin]


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("commit", "abc123", "40-char"),
        ("integrity", "md5-deadbeef", "sha256-"),
    ],
)
def test_load_manifest_rejects_bad_pins(tmp_path: Path, field, value, match):
    raw = {
        "key": "test/x",
        "source": "test",
        "repo": "https://example.invalid/repo",
        "path": "skills/x",
        "commit": "0" * 40,
        "integrity": "sha256-" + "0" * 64,
    }
    raw[field] = value
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"skills": [raw]}), encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        load_manifest(manifest)


def test_load_manifest_rejects_duplicate_keys(tmp_path: Path):
    raw = {
        "key": "test/x",
        "source": "test",
        "repo": "https://example.invalid/repo",
        "path": "skills/x",
        "commit": "0" * 40,
        "integrity": "sha256-" + "0" * 64,
    }
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"skills": [raw, raw]}), encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate skill key"):
        load_manifest(manifest)


def test_directory_integrity_hash_deterministic_and_sensitive(tmp_path: Path):
    _write_skill(tmp_path / "skills", "alpha", description="first")
    skill_dir = tmp_path / "skills" / "alpha"

    first = directory_integrity_hash(skill_dir)
    assert first == directory_integrity_hash(skill_dir)
    assert first.startswith("sha256-")

    (skill_dir / "SKILL.md").write_text("---\nname: alpha\n---\n\nchanged\n")
    assert directory_integrity_hash(skill_dir) != first


def test_stable_guidance_id_deterministic_int32():
    first = stable_guidance_id("[test] arxiv-search")
    assert first == stable_guidance_id("[test] arxiv-search")
    assert 0 <= first <= 0x7FFFFFFF
    assert first != stable_guidance_id("[test] ffmpeg-frames")


def test_entry_hash_changes_with_title_and_content():
    base = entry_hash("t", "c")
    assert base == entry_hash("t", "c")
    assert base != entry_hash("t", "c2")
    assert base != entry_hash("t2", "c")


def test_build_snapshot_entries_imports_pinned_skills(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pins = [_pin(repo, sha, "arxiv-search"), _pin(repo, sha, "ffmpeg-frames")]

    entries = build_snapshot_entries(pins, workdir=tmp_path / "work")

    assert set(entries) == {"test/arxiv-search", "test/ffmpeg-frames"}
    arxiv = entries["test/arxiv-search"]
    assert arxiv["title"] == "[test] arxiv-search"
    assert "Search arXiv for papers." in arxiv["content"]
    assert "ffmpeg -i" in entries["test/ffmpeg-frames"]["content"]

    snapshot = tmp_path / "snapshot.json"
    write_snapshot(entries, snapshot)
    assert load_snapshot(snapshot) == entries
    # Deterministic output: rewriting produces identical bytes.
    before = snapshot.read_bytes()
    write_snapshot(entries, snapshot)
    assert snapshot.read_bytes() == before


def test_build_snapshot_imports_pinned_commit_not_head(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pin = _pin(repo, sha, "arxiv-search")

    # Advance upstream past the pin; the import must still see the pinned state.
    (repo / "skills" / "arxiv-search" / "SKILL.md").write_text(
        "---\nname: arxiv-search\ndescription: CHANGED UPSTREAM\n---\n\nnew body\n",
        encoding="utf-8",
    )
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "mutate skill", cwd=repo)

    entries = build_snapshot_entries([pin], workdir=tmp_path / "work")
    assert "Search arXiv for papers." in entries["test/arxiv-search"]["content"]
    assert "CHANGED UPSTREAM" not in entries["test/arxiv-search"]["content"]


def test_build_snapshot_rejects_integrity_mismatch(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pin = _pin(repo, sha, "arxiv-search")
    tampered = PinnedSkill(
        key=pin.key,
        source=pin.source,
        repo=pin.repo,
        path=pin.path,
        commit=pin.commit,
        integrity="sha256-" + "0" * 64,
    )
    with pytest.raises(ValueError, match="integrity mismatch"):
        build_snapshot_entries([tampered], workdir=tmp_path / "work")


def test_build_snapshot_rejects_title_collisions(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    first = _pin(repo, sha, "arxiv-search", key="test/one")
    second = _pin(repo, sha, "arxiv-search", key="test/two")
    with pytest.raises(ValueError, match="collides"):
        build_snapshot_entries([first, second], workdir=tmp_path / "work")


def test_check_drift_converged_and_changed(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pins = [_pin(repo, sha, "arxiv-search"), _pin(repo, sha, "ffmpeg-frames")]

    assert check_drift(pins, workdir=tmp_path / "work1") == []

    (repo / "skills" / "arxiv-search" / "SKILL.md").write_text(
        "---\nname: arxiv-search\ndescription: drifted\n---\n\nnew\n",
        encoding="utf-8",
    )
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "drift", cwd=repo)

    drifts = check_drift(pins, workdir=tmp_path / "work2")
    assert [d["key"] for d in drifts] == ["test/arxiv-search"]
    assert drifts[0]["status"] == "changed-upstream"
    assert drifts[0]["pinned_commit"] == sha
    assert drifts[0]["head_commit"] != sha


def test_check_drift_reports_upstream_removal(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)
    pin = _pin(repo, sha, "ffmpeg-frames")

    _git("rm", "-q", "-r", "skills/ffmpeg-frames", cwd=repo)
    _git("commit", "-q", "-m", "remove skill", cwd=repo)

    drifts = check_drift([pin], workdir=tmp_path / "work")
    assert len(drifts) == 1
    assert drifts[0]["status"] == "removed-upstream"


def test_pin_skills_at_head_round_trips_through_manifest(tmp_path: Path):
    repo, sha = _make_skill_repo(tmp_path)

    pins = pin_skills_at_head(
        str(repo),
        ["skills/arxiv-search", "skills/ffmpeg-frames"],
        source="test",
        workdir=tmp_path / "pin-work",
    )
    assert [pin.key for pin in pins] == ["test/arxiv-search", "test/ffmpeg-frames"]
    assert all(pin.commit == sha for pin in pins)

    manifest = tmp_path / "manifest.json"
    write_manifest(pins, manifest)
    assert load_manifest(manifest) == sorted(pins, key=lambda p: p.key)

    # The generated pins are immediately importable (integrity verifies).
    entries = build_snapshot_entries(pins, workdir=tmp_path / "import-work")
    assert set(entries) == {"test/arxiv-search", "test/ffmpeg-frames"}


# --------------------------------------------------------------------------- #
# Default builtins library (committed manifest + snapshot)                     #
# --------------------------------------------------------------------------- #

DEFAULT_ANTHROPIC_SKILLS = {
    "algorithmic-art",
    "brand-guidelines",
    "canvas-design",
    "doc-coauthoring",
    "docx",
    "frontend-design",
    "internal-comms",
    "pdf",
    "pptx",
    "slack-gif-creator",
    "theme-factory",
    "web-artifacts-builder",
    "webapp-testing",
    "xlsx",
}
EXCLUDED_ANTHROPIC_SKILLS = {"mcp-builder", "claude-api", "skill-creator"}


def test_default_manifest_pins_the_anthropic_library():
    pins = load_manifest(MANIFEST_PATH)

    assert {pin.key for pin in pins} == {
        f"anthropic/{name}" for name in DEFAULT_ANTHROPIC_SKILLS
    }
    for pin in pins:
        assert pin.source == "anthropic"
        assert pin.repo == "https://github.com/anthropics/skills"
        assert pin.path == f"skills/{pin.key.split('/', 1)[1]}"
    # Deliberately excluded skills must never sneak into the default library.
    excluded_keys = {f"anthropic/{name}" for name in EXCLUDED_ANTHROPIC_SKILLS}
    assert not excluded_keys & {pin.key for pin in pins}
    # The whole library is pinned at one commit of the upstream repo.
    assert len({pin.commit for pin in pins}) == 1


def test_default_snapshot_matches_default_manifest():
    pins = load_manifest(MANIFEST_PATH)
    snapshot = load_snapshot()

    assert set(snapshot) == {pin.key for pin in pins}
    for pin in pins:
        entry = snapshot[pin.key]
        skill_name = pin.key.split("/", 1)[1]
        assert entry["title"] == f"[anthropic] {skill_name}"
        assert entry["content"].strip()
        assert "Source: anthropic" in entry["content"]  # provenance footer
    # Stable ids derived from the snapshot titles are collision-free.
    ids = {stable_guidance_id(entry["title"]) for entry in snapshot.values()}
    assert len(ids) == len(snapshot)


# --------------------------------------------------------------------------- #
# End-to-end tests against a real GuidanceManager                              #
# --------------------------------------------------------------------------- #


def _seed_tree(tmp_path: Path) -> Path:
    skills_root = tmp_path / "skills"
    _write_skill(
        skills_root,
        "ffmpeg-frames",
        description="Extract frames from a video.",
        body="Use ffmpeg to grab a frame.",
        scripts={"frame.sh": '#!/usr/bin/env bash\nffmpeg -i "$1" out.jpg\n'},
    )
    _write_skill(
        skills_root,
        "arxiv-search",
        description="Search arXiv for papers.",
        body="Query the arXiv API.",
    )
    return skills_root


@_handle_project
def test_migrate_imports_skills_into_guidance(tmp_path: Path):
    skills_root = _seed_tree(tmp_path)
    skills = discover_skills(
        [skills_root],
        source="openclaw",
        repo_root=tmp_path,
    )
    gm = GuidanceManager()
    migrator = SkillMigrator(
        skills,
        guidance_manager=gm,
        title_prefix="[octest] ",
    )
    report = migrator.run(execute=True)

    assert report["summary"]["added"] == 2
    assert report["summary"]["errors"] == 0

    rows = gm.filter(filter="title == '[octest] ffmpeg-frames'")
    assert rows and rows[0].title == "[octest] ffmpeg-frames"
    assert "Extract frames from a video." in rows[0].content
    assert "ffmpeg -i" in rows[0].content  # bundled script inlined
    assert "Source: openclaw" in rows[0].content


@_handle_project
def test_migrate_skips_existing_titles_on_rerun(tmp_path: Path):
    skills_root = _seed_tree(tmp_path)
    skills = discover_skills(
        [skills_root],
        source="openclaw",
        repo_root=tmp_path,
    )
    gm = GuidanceManager()

    first = SkillMigrator(
        skills,
        guidance_manager=gm,
        title_prefix="[skiptest] ",
    ).run(execute=True)
    assert first["summary"]["added"] == 2

    second = SkillMigrator(
        skills,
        guidance_manager=gm,
        title_prefix="[skiptest] ",
    ).run(execute=True)
    assert second["summary"]["added"] == 0
    assert second["summary"]["skipped"] == 2

    # Still exactly one entry per title (no duplicates created).
    rows = gm.filter(filter="title == '[skiptest] arxiv-search'")
    assert len(rows) == 1


@_handle_project
def test_migrate_overwrite_updates_existing_entry(tmp_path: Path):
    skills_root = tmp_path / "skills"
    md = _write_skill(
        skills_root,
        "changing-skill",
        description="Original description.",
        body="original body",
    )
    gm = GuidanceManager()

    skills = discover_skills([skills_root], source="hermes", repo_root=tmp_path)
    first = SkillMigrator(
        skills,
        guidance_manager=gm,
        title_prefix="[ovtest] ",
    ).run(execute=True)
    original_id = first["items"][0]["guidance_id"]

    # Mutate the skill on disk, re-discover, and overwrite.
    md.write_text(
        "---\nname: changing-skill\ndescription: Updated description.\n---\n\nupdated body\n",
        encoding="utf-8",
    )
    skills2 = discover_skills([skills_root], source="hermes", repo_root=tmp_path)
    second = SkillMigrator(
        skills2,
        guidance_manager=gm,
        title_prefix="[ovtest] ",
        conflict_mode="overwrite",
    ).run(execute=True)

    assert second["summary"]["updated"] == 1
    assert second["items"][0]["guidance_id"] == original_id  # id stable

    rows = gm.filter(filter="title == '[ovtest] changing-skill'")
    assert len(rows) == 1
    assert "Updated description." in rows[0].content
    assert "updated body" in rows[0].content
