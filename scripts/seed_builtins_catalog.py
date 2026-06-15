#!/usr/bin/env python3
"""Seed the global builtins catalogues (primitives and guidance).

Creates (or converges) the public-read ``Builtins`` Unify project holding
one platform-wide copy of every manager's static primitive rows and the
builtin guidance library imported from the Agent Skills ecosystem, plus the
vector columns required for read-only ranked search.

Run with the API key of the account that should OWN the catalogue (the
platform admin account on hosted deployments; the shared key on self-host):

    UNIFY_KEY=<admin-key> ORCHESTRA_URL=<api-url> \
        .venv/bin/python scripts/seed_builtins_catalog.py

The run is idempotent and hash-guarded (per manager for primitives, per
skill for guidance), so it is safe (and cheap) to invoke on every deploy.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    from unity.common.builtins import builtins_project
    from unity.function_manager.builtins_catalog import seed_builtin_primitives
    from unity.guidance_manager.builtins_catalog import seed_builtin_guidance

    primitives_changed = seed_builtin_primitives()
    guidance_changed = seed_builtin_guidance()
    project = builtins_project()
    for name, changed in (
        ("primitives", primitives_changed),
        ("guidance", guidance_changed),
    ):
        state = "updated" if changed else "already up to date"
        print(f"Builtins {name} catalogue ({project}): {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
