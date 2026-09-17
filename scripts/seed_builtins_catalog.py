"""Seed the builtins catalogues (primitive functions and guidance).

The catalogues live in the public-read ``Builtins`` project of the local store.
Each seeder compares the committed snapshot against what is stored and rewrites
only the entries whose content hash changed, so re-running is cheap and
idempotent.

Usage::

    .venv/bin/python -m scripts.seed_builtins_catalog
"""

from __future__ import annotations

import logging
import sys

from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    import unify
    from unify.common.builtins import builtins_project
    from unify.function_manager.builtins_catalog import seed_builtin_primitives
    from unify.guidance_manager.builtins_catalog import seed_builtin_guidance

    unify.init()
    project = builtins_project()
    for name, changed in (
        ("primitives", seed_builtin_primitives()),
        ("guidance", seed_builtin_guidance()),
    ):
        state = "updated" if changed else "already up to date"
        print(f"Builtins {name} catalogue ({project}): {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
