"""Skill → GuidanceManager migration utilities.

Imports ``SKILL.md``-style skills (the agentskills.io standard used by
OpenClaw and HermesAgent) into Unity's ``GuidanceManager`` as guidance
entries.  See :mod:`scripts.skill_migration.skill_to_guidance` for the
reusable core and the ``openclaw_to_guidance`` / ``hermes_to_guidance``
convenience CLIs alongside it.
"""

from .skill_to_guidance import (
    ParsedSkill,
    SkillMigrator,
    compose_guidance_content,
    discover_skills,
    guidance_title,
    parse_skill_file,
    split_frontmatter,
)

__all__ = [
    "ParsedSkill",
    "SkillMigrator",
    "compose_guidance_content",
    "discover_skills",
    "guidance_title",
    "parse_skill_file",
    "split_frontmatter",
]
