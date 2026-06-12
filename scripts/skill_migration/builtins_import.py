"""Importer for the builtin guidance library: pinned upstream skills → snapshot.

The default library is defined by a committed lockfile-style manifest
(``builtins_skills.lock.json``) pinning every imported skill to an exact
upstream state:

.. code-block:: json

    {
      "skills": [
        {
          "key": "vercel/find-skills",
          "source": "vercel",
          "repo": "https://github.com/vercel-labs/skills",
          "path": "skills/find-skills",
          "commit": "<40-char git SHA>",
          "integrity": "sha256-<hex digest of the skill directory>"
        }
      ]
    }

The importer clones each source repository at the **pinned commit**,
verifies the directory integrity hash (so what is imported provably matches
the pin), parses each ``SKILL.md`` with the shared ``skill_to_guidance``
helpers, and writes the committed snapshot consumed by
``unity.guidance_manager.builtins_catalog.seed_builtin_guidance``.

Updates are always explicit: bump the pin in the manifest, re-run the
importer, review the snapshot diff, run the guidance eval suites, land.
``--check`` compares upstream HEAD against the pins and reports drift
without applying anything (non-zero exit on drift, for CI/cron alerts).

Usage::

    .venv/bin/python -m scripts.skill_migration.builtins_import           # import
    .venv/bin/python -m scripts.skill_migration.builtins_import --check   # drift report

    # Pin skills from a repo at its current HEAD (writes/updates the manifest):
    .venv/bin/python -m scripts.skill_migration.builtins_import \
        --pin-repo https://github.com/anthropics/skills --source anthropic \
        --skill-paths skills/pdf skills/docx
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .skill_to_guidance import (
    compose_guidance_content,
    guidance_title,
    parse_skill_file,
)

MANIFEST_PATH = Path(__file__).with_name("builtins_skills.lock.json")

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_INTEGRITY_RE = re.compile(r"^sha256-[0-9a-f]{64}$")


@dataclass(frozen=True)
class PinnedSkill:
    """One skill pinned to an exact upstream commit and directory hash."""

    key: str
    source: str
    repo: str
    path: str
    commit: str
    integrity: str


def load_manifest(path: Optional[Path] = None) -> List[PinnedSkill]:
    """Load and validate the pinned-skills manifest."""
    path = path or MANIFEST_PATH
    data = json.loads(path.read_text(encoding="utf-8"))
    pins: List[PinnedSkill] = []
    seen_keys: set[str] = set()
    for raw in data.get("skills", []):
        pin = PinnedSkill(
            key=str(raw["key"]),
            source=str(raw["source"]),
            repo=str(raw["repo"]),
            path=str(raw["path"]).strip("/"),
            commit=str(raw["commit"]),
            integrity=str(raw["integrity"]),
        )
        if not _COMMIT_RE.fullmatch(pin.commit):
            raise ValueError(
                f"Skill {pin.key!r}: commit must be a full 40-char lowercase "
                f"hex SHA, got {pin.commit!r}",
            )
        if not _INTEGRITY_RE.fullmatch(pin.integrity):
            raise ValueError(
                f"Skill {pin.key!r}: integrity must look like 'sha256-<hex>', "
                f"got {pin.integrity!r}",
            )
        if pin.key in seen_keys:
            raise ValueError(f"Duplicate skill key in manifest: {pin.key!r}")
        seen_keys.add(pin.key)
        pins.append(pin)
    return pins


def directory_integrity_hash(directory: Path) -> str:
    """Deterministic sha256 over a skill directory's file tree.

    Hashes the sorted relative POSIX paths together with each file's content
    digest, mirroring the skills-lock pinning model.
    """
    digest = hashlib.sha256()
    for file_path in sorted(p for p in directory.rglob("*") if p.is_file()):
        rel = file_path.relative_to(directory).as_posix()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(hashlib.sha256(file_path.read_bytes()).hexdigest().encode())
        digest.update(b"\x00")
    return f"sha256-{digest.hexdigest()}"


def _git(*args: str, cwd: Optional[Path] = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def clone_at_commit(repo: str, commit: str, dest: Path) -> Path:
    """Shallow-fetch *repo* at exactly *commit* into *dest* and check it out."""
    dest.mkdir(parents=True, exist_ok=True)
    _git("init", "--quiet", str(dest))
    _git("remote", "add", "origin", repo, cwd=dest)
    _git("fetch", "--quiet", "--depth", "1", "origin", commit, cwd=dest)
    _git("checkout", "--quiet", "--detach", "FETCH_HEAD", cwd=dest)
    return dest


def resolve_remote_head(repo: str) -> str:
    """Return the commit SHA of the remote default branch HEAD."""
    out = _git("ls-remote", repo, "HEAD")
    for line in out.splitlines():
        sha, _, ref = line.partition("\t")
        if ref.strip() == "HEAD":
            return sha.strip()
    raise RuntimeError(f"Could not resolve HEAD for {repo!r}")


def _checkout_cache(
    pins: List[PinnedSkill],
    workdir: Path,
) -> Dict[tuple[str, str], Path]:
    """Clone each unique (repo, commit) pair once."""
    cache: Dict[tuple[str, str], Path] = {}
    for pin in pins:
        cache_key = (pin.repo, pin.commit)
        if cache_key in cache:
            continue
        slug = hashlib.sha256(f"{pin.repo}@{pin.commit}".encode()).hexdigest()[:12]
        cache[cache_key] = clone_at_commit(
            pin.repo,
            pin.commit,
            workdir / f"checkout_{slug}",
        )
    return cache


def build_snapshot_entries(
    pins: List[PinnedSkill],
    *,
    workdir: Path,
) -> Dict[str, Dict[str, str]]:
    """Clone, verify, and parse every pinned skill into snapshot entries."""
    checkouts = _checkout_cache(pins, workdir)
    entries: Dict[str, Dict[str, str]] = {}
    titles: Dict[str, str] = {}
    for pin in pins:
        checkout = checkouts[(pin.repo, pin.commit)]
        skill_dir = checkout / pin.path
        if not skill_dir.is_dir():
            raise FileNotFoundError(
                f"Skill {pin.key!r}: path {pin.path!r} not found in "
                f"{pin.repo} at {pin.commit}",
            )
        actual = directory_integrity_hash(skill_dir)
        if actual != pin.integrity:
            raise ValueError(
                f"Skill {pin.key!r}: integrity mismatch at pinned commit "
                f"{pin.commit}: manifest has {pin.integrity}, directory "
                f"hashes to {actual}. Refusing to import.",
            )
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            raise FileNotFoundError(f"Skill {pin.key!r}: no SKILL.md in {pin.path!r}")
        skill = parse_skill_file(skill_md, source=pin.source, repo_root=checkout)
        title = guidance_title(skill, title_prefix=f"[{pin.source}] ")
        if title in titles:
            raise ValueError(
                f"Skill {pin.key!r} produces title {title!r} which collides "
                f"with skill {titles[title]!r}; stable ids would clash.",
            )
        titles[title] = pin.key
        entries[pin.key] = {
            "title": title,
            "content": compose_guidance_content(skill),
        }
    return entries


def write_snapshot(entries: Dict[str, Dict[str, str]], path: Path) -> None:
    """Write the snapshot deterministically (sorted keys, trailing newline)."""
    payload = {"skills": {key: entries[key] for key in sorted(entries)}}
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def pin_skills_at_head(
    repo: str,
    skill_paths: List[str],
    *,
    source: str,
    workdir: Path,
) -> List[PinnedSkill]:
    """Build manifest pins for *skill_paths* at the repo's current HEAD.

    This is the explicit update path: pinning (or re-pinning) records the
    HEAD commit and per-skill directory integrity hashes, after which the
    import is fully reproducible from the manifest alone.
    """
    head = resolve_remote_head(repo)
    slug = hashlib.sha256(f"{repo}@{head}".encode()).hexdigest()[:12]
    checkout = clone_at_commit(repo, head, workdir / f"pin_{slug}")
    pins: List[PinnedSkill] = []
    for skill_path in skill_paths:
        skill_path = skill_path.strip("/")
        skill_dir = checkout / skill_path
        if not (skill_dir / "SKILL.md").is_file():
            raise FileNotFoundError(
                f"No SKILL.md at {skill_path!r} in {repo} at {head}",
            )
        pins.append(
            PinnedSkill(
                key=f"{source}/{skill_dir.name}",
                source=source,
                repo=repo,
                path=skill_path,
                commit=head,
                integrity=directory_integrity_hash(skill_dir),
            ),
        )
    return pins


def write_manifest(pins: List[PinnedSkill], path: Path) -> None:
    """Write the manifest deterministically (sorted by key)."""
    payload = {
        "skills": [pin.__dict__ for pin in sorted(pins, key=lambda p: p.key)],
    }
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def check_drift(pins: List[PinnedSkill], *, workdir: Path) -> List[Dict[str, str]]:
    """Compare upstream HEAD folder hashes against the pins.

    Returns one report per drifted skill; empty list means fully converged.
    Nothing is applied — updates always go through an explicit pin bump.
    """
    drifts: List[Dict[str, str]] = []
    head_checkouts: Dict[str, tuple[str, Path]] = {}
    for repo in dict.fromkeys(pin.repo for pin in pins):
        head = resolve_remote_head(repo)
        slug = hashlib.sha256(f"{repo}@HEAD".encode()).hexdigest()[:12]
        checkout = clone_at_commit(repo, head, workdir / f"head_{slug}")
        head_checkouts[repo] = (head, checkout)

    for pin in pins:
        head, checkout = head_checkouts[pin.repo]
        skill_dir = checkout / pin.path
        if not skill_dir.is_dir():
            drifts.append(
                {
                    "key": pin.key,
                    "status": "removed-upstream",
                    "pinned_commit": pin.commit,
                    "head_commit": head,
                },
            )
            continue
        head_integrity = directory_integrity_hash(skill_dir)
        if head_integrity != pin.integrity:
            drifts.append(
                {
                    "key": pin.key,
                    "status": "changed-upstream",
                    "pinned_commit": pin.commit,
                    "head_commit": head,
                    "pinned_integrity": pin.integrity,
                    "head_integrity": head_integrity,
                },
            )
    return drifts


def main(argv: Optional[List[str]] = None) -> int:
    from unity.guidance_manager.builtins_catalog import SNAPSHOT_PATH

    parser = argparse.ArgumentParser(
        description=(
            "Import pinned Agent Skills into the builtin guidance snapshot, "
            "or check the pins for upstream drift."
        ),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=MANIFEST_PATH,
        help="Path to the pinned-skills manifest.",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=SNAPSHOT_PATH,
        help="Path of the snapshot to (re)generate.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Report upstream drift against the pins without applying anything.",
    )
    parser.add_argument(
        "--pin-repo",
        help="Pin --skill-paths from this repo at its current HEAD, then import.",
    )
    parser.add_argument(
        "--source",
        help="Source label for --pin-repo (namespaces titles as '[source] name').",
    )
    parser.add_argument(
        "--skill-paths",
        nargs="+",
        default=[],
        help="Skill directories within --pin-repo to pin.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the report as JSON.",
    )
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="builtins_skills_") as tmp:
        workdir = Path(tmp)
        if args.pin_repo:
            if not args.source or not args.skill_paths:
                parser.error("--pin-repo requires --source and --skill-paths")
            existing = {
                pin.key: pin
                for pin in (
                    load_manifest(args.manifest) if args.manifest.exists() else []
                )
            }
            new_pins = pin_skills_at_head(
                args.pin_repo,
                args.skill_paths,
                source=args.source,
                workdir=workdir,
            )
            for pin in new_pins:
                existing[pin.key] = pin
            write_manifest(list(existing.values()), args.manifest)
            print(f"Pinned {len(new_pins)} skills at HEAD into {args.manifest}")

        pins = load_manifest(args.manifest)
        if not pins:
            print("Manifest is empty; nothing to do.")
            return 0
        if args.check:
            drifts = check_drift(pins, workdir=workdir)
            if args.json:
                print(json.dumps({"drifts": drifts}, indent=2))
            elif not drifts:
                print(f"All {len(pins)} pinned skills match upstream HEAD.")
            else:
                for drift in drifts:
                    print(
                        f"DRIFT {drift['key']}: {drift['status']} "
                        f"(pinned {drift['pinned_commit'][:12]}, "
                        f"head {drift['head_commit'][:12]})",
                    )
            return 1 if drifts else 0

        entries = build_snapshot_entries(pins, workdir=workdir)
        write_snapshot(entries, args.snapshot)
        if args.json:
            print(
                json.dumps(
                    {"imported": sorted(entries), "snapshot": str(args.snapshot)},
                    indent=2,
                ),
            )
        else:
            print(f"Imported {len(entries)} skills into {args.snapshot}:")
            for key in sorted(entries):
                print(f"  + {entries[key]['title']}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
