#!/usr/bin/env python3
"""Import OpenClaw skills into Unity's GuidanceManager.

OpenClaw ships skills as ``SKILL.md`` folders in three places: the bundled
``skills/`` tree, plugin-shipped ``extensions/*/skills/`` trees, and the
repo-internal ``.agents/skills/`` tree.  This CLI walks all of them, maps
each skill to a guidance entry, and (with ``--execute``) writes them.

Usage
-----
    # Dry run against a sibling ./openclaw checkout
    .venv/bin/python -m scripts.skill_migration.openclaw_to_guidance

    # Import for real, skipping any titles that already exist
    .venv/bin/python -m scripts.skill_migration.openclaw_to_guidance --execute

    # Point at an explicit checkout and overwrite existing entries
    .venv/bin/python -m scripts.skill_migration.openclaw_to_guidance \
        --repo-root ~/openclaw --execute --conflict overwrite
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from ._cli import _main_with

SOURCE = "openclaw"
REPO_NAME = "openclaw"
DEFAULT_TITLE_PREFIX = "[openclaw] "


def skill_roots(repo_root: Path) -> List[Path]:
    """Skill-bearing directories within an OpenClaw checkout."""
    return [
        repo_root / "skills",
        repo_root / "extensions",
        repo_root / ".agents" / "skills",
    ]


def main() -> None:
    _main_with(
        source=SOURCE,
        repo_name=REPO_NAME,
        roots_fn=skill_roots,
        default_title_prefix=DEFAULT_TITLE_PREFIX,
    )


if __name__ == "__main__":
    main()
