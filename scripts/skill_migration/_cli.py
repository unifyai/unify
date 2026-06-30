"""Shared argument parsing / execution for the skill-migration CLIs.

``openclaw_to_guidance`` and ``hermes_to_guidance`` are thin wrappers that
only differ in their source label, default repo location, and where skills
live within that repo.  Everything else (flags, discovery, reporting,
GuidanceManager wiring) lives here.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, List

from .skill_to_guidance import SkillMigrator, discover_skills


def default_repo_root(repo_name: str) -> Path:
    """Best-effort default: a sibling checkout next to the unity repo.

    ``scripts/skill_migration/_cli.py`` → unity root is ``parents[2]``; the
    sibling repo is expected alongside it (matches the common local layout
    where ``unity``, ``openclaw`` and ``hermes-agent`` are siblings).
    """
    unity_root = Path(__file__).resolve().parents[2]
    return unity_root.parent / repo_name


def _print_report(report: dict, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, indent=2))
        return

    summary = report["summary"]
    mode = "EXECUTE" if report["executed"] else "DRY RUN"
    print(
        f"\n[{mode}] {report['source']} → GuidanceManager  "
        f"(prefix={report['title_prefix']!r}, conflict={report['conflict_mode']})",
    )
    print(
        "  discovered={discovered} added={added} updated={updated} "
        "skipped={skipped} errors={errors}".format(**summary),
    )
    for item in report["items"]:
        marker = {
            "added": "+",
            "updated": "~",
            "skipped": "=",
            "error": "!",
        }.get(item["status"], "?")
        line = f"  {marker} {item['title']}"
        if item.get("reason"):
            line += f"  ({item['reason']})"
        print(line)
    if not report["executed"]:
        print("\nNothing was written. Re-run with --execute to import.")


def run_cli(
    *,
    source: str,
    repo_name: str,
    roots_fn: Callable[[Path], List[Path]],
    default_title_prefix: str,
    argv: List[str] | None = None,
) -> int:
    """Entry point shared by both source-specific migration CLIs."""
    parser = argparse.ArgumentParser(
        description=(
            f"Import {source} skills (SKILL.md files) into Unity's "
            "GuidanceManager as guidance entries."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo_root(repo_name),
        help=f"Path to the {source} checkout (default: sibling ./{repo_name}).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write to the GuidanceManager (default: dry run).",
    )
    parser.add_argument(
        "--conflict",
        choices=["skip", "overwrite"],
        default="skip",
        help="What to do when a guidance entry with the same title exists.",
    )
    parser.add_argument(
        "--title-prefix",
        default=default_title_prefix,
        help=(
            "Prefix applied to every skill name to namespace titles "
            f"(default: {default_title_prefix!r})."
        ),
    )
    parser.add_argument(
        "--no-inline-scripts",
        action="store_true",
        help="Do not inline bundled scripts/ files into guidance content.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only migrate the first N discovered skills (handy for trials).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the migration report as JSON.",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.expanduser().resolve()
    if not repo_root.exists():
        parser.error(f"{source} repo not found at {repo_root}. Pass --repo-root.")

    roots = roots_fn(repo_root)
    skills = discover_skills(roots, source=source, repo_root=repo_root)
    if args.limit is not None:
        skills = skills[: args.limit]

    if not skills:
        print(f"No SKILL.md files found under {repo_root}.")
        return 1

    gm = None
    if args.execute:
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except Exception:
            pass
        from unify.guidance_manager.guidance_manager import GuidanceManager

        gm = GuidanceManager()

    migrator = SkillMigrator(
        skills,
        guidance_manager=gm,
        title_prefix=args.title_prefix,
        conflict_mode=args.conflict,
        inline_scripts=not args.no_inline_scripts,
    )
    report = migrator.run(execute=args.execute)
    _print_report(report, as_json=args.json)

    return 1 if report["summary"]["errors"] else 0


def _main_with(
    *,
    source: str,
    repo_name: str,
    roots_fn: Callable[[Path], List[Path]],
    default_title_prefix: str,
) -> None:
    sys.exit(
        run_cli(
            source=source,
            repo_name=repo_name,
            roots_fn=roots_fn,
            default_title_prefix=default_title_prefix,
        ),
    )
