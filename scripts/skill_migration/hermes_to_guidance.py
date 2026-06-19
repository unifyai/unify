#!/usr/bin/env python3
"""Import HermesAgent skills into Droid's GuidanceManager.

HermesAgent ships skills as ``SKILL.md`` folders under ``skills/`` (bundled)
and ``optional-skills/`` (opt-in), plus the occasional plugin-shipped skill
under ``plugins/``.  This CLI walks all of them, maps each skill to a
guidance entry, and (with ``--execute``) writes them.

Usage
-----
    # Dry run against a sibling ./hermes-agent checkout
    .venv/bin/python -m scripts.skill_migration.hermes_to_guidance

    # Import for real, skipping any titles that already exist
    .venv/bin/python -m scripts.skill_migration.hermes_to_guidance --execute

    # Point at an explicit checkout and overwrite existing entries
    .venv/bin/python -m scripts.skill_migration.hermes_to_guidance \
        --repo-root ~/hermes-agent --execute --conflict overwrite
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from ._cli import _main_with

SOURCE = "hermes"
REPO_NAME = "hermes-agent"
DEFAULT_TITLE_PREFIX = "[hermes] "


def skill_roots(repo_root: Path) -> List[Path]:
    """Skill-bearing directories within a HermesAgent checkout."""
    return [
        repo_root / "skills",
        repo_root / "optional-skills",
        repo_root / "plugins",
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
