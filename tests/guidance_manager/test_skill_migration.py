"""Tests for the SKILL.md → GuidanceManager migration utilities.

Two layers:

* Pure parsing / mapping tests (no backend) covering frontmatter parsing,
  discovery, title namespacing, and content composition for both the
  OpenClaw (single-line JSON ``metadata``) and HermesAgent (nested YAML
  ``metadata``) flavours of the agentskills.io standard.
* End-to-end tests that build a tiny on-disk skill tree and import it into a
  real ``GuidanceManager`` (under ``@_handle_project``), exercising the
  add / skip / overwrite conflict paths.
"""

from __future__ import annotations

from pathlib import Path

from scripts.skill_migration.skill_to_guidance import (
    SkillMigrator,
    compose_guidance_content,
    discover_skills,
    guidance_title,
    parse_skill_file,
    split_frontmatter,
)
from unity.guidance_manager.guidance_manager import GuidanceManager
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
