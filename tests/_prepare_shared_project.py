#!/usr/bin/env python3
"""
Internal module to prepare the shared UnityTests project for parallel runs.

This script is called by parallel_run.sh before spawning tmux sessions.
It ensures the shared project and contexts exist, making subsequent
parallel pytest sessions race-free.

The script is idempotent: calling it multiple times has no adverse effects.
If Orchestra is unreachable (e.g. CI runner failed to start it, or local dev
without `unity setup`), the script exits 0 with a clear note — pytest will
then run, and individual tests handle the missing backend via their own
`requires_orchestra` marker / `_check_orchestra_available` skip logic in
`tests/conftest.py`. Crashing here would kill the pytest session before
any test (including pure unit tests that don't need Orchestra) gets to run.

Usage (internal - typically invoked via parallel_run.sh):
    python3 tests/_prepare_shared_project.py
"""

# Suppress urllib3 connection pool warnings before any imports configure logging
import logging

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

import os
import sys
from pathlib import Path

# Ensure repo root is in sys.path so we can import unity.
# This script is invoked directly by parallel_run.sh, not via pytest
# which handles path setup automatically.
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from dotenv import load_dotenv

load_dotenv()

PROJECT = "UnityTests"


def _orchestra_reachable() -> bool:
    """Quick health probe so we can no-op cleanly when the backend is down.

    Mirrors what `tests/conftest.py:_check_orchestra_available` does: a 200,
    401, or 403 from `<base>/v0/projects` all mean "the server is up". We
    only no-op on connection-level failures.
    """
    base = os.environ.get("ORCHESTRA_URL", "http://localhost:8000")
    if base.endswith("/v0"):
        url = f"{base}/projects"
    else:
        url = f"{base.rstrip('/')}/v0/projects"

    try:
        import httpx

        with httpx.Client(timeout=2.0) as client:
            return client.get(url).status_code in (200, 401, 403)
    except Exception:
        return False


def prepare_shared_project() -> None:
    """Prepare the shared UnityTests project and Combined context."""
    try:
        import unify
    except ImportError:
        print(
            "Error: 'unify' package not found. Ensure the virtualenv is active.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not _orchestra_reachable():
        # No backend to set up against. Pytest itself will skip Orchestra-
        # requiring tests via `requires_orchestra`; pure unit tests still run.
        print(
            "Skipping shared-project prep: Orchestra is not reachable at "
            f"{os.environ.get('ORCHESTRA_URL', 'http://localhost:8000')}. "
            "Tests marked `requires_orchestra` will skip; others run as normal.",
        )
        return

    # 1. Activate/create project (idempotent - does not overwrite if exists)
    try:
        unify.activate(PROJECT, overwrite=False)
    except Exception as e:
        # Tolerate activation failures (e.g., project already active in another process)
        print(f"Note: Project activation returned: {e}", file=sys.stderr)

    unify.set_user_logging(False)

    # 2. Ensure Combined context with fields (idempotent). Wrapped because the
    # project-activate above may have raced with another worker / partial
    # connectivity, leaving us in a state where create_context still fails.
    try:
        unify.create_context("Combined")
    except Exception as e:
        print(f"Note: create_context('Combined') returned: {e}", file=sys.stderr)

    # Ensure fields exist (idempotent - create_fields tolerates existing fields)
    try:
        unify.create_fields(
            context="Combined",
            fields={
                "test_fpath": {"type": "str", "mutable": True},
                "tags": {"type": "list", "mutable": True},
                "duration": {"type": "float", "mutable": True},
                "llm_io": {"type": "list", "mutable": True},
                "settings": {"type": "dict", "mutable": True},
            },
        )
    except Exception:
        # Tolerate field creation errors (may already exist)
        pass

    # Pre-create assistant-derived contexts via unity.init() to avoid races
    # when parallel pytest sessions (xdist, tmux, CI) all call unity.init()
    try:
        import unity

        unity.init(PROJECT)
    except Exception as e:
        # Tolerate if contexts already exist (another process created them)
        if "already exists" not in str(e).lower():
            print(f"Note: unity.init() returned: {e}", file=sys.stderr)

    # Seed the global builtins catalogues (public-read project shared by
    # primitives, guidance, and integration catalogues). Hash-guarded, so
    # this is a no-op in all but the first run after a catalogue change.
    from unity.function_manager.builtins_catalog import seed_builtin_primitives
    from unity.guidance_manager.builtins_catalog import seed_builtin_guidance
    from unity.integrations.builtins_catalog import seed_builtin_integrations

    seed_builtin_primitives()
    seed_builtin_guidance()
    seed_builtin_integrations()
    print("Seeded builtins catalogues (primitives + guidance + integrations)")

    print(f"Prepared shared project '{PROJECT}'")


if __name__ == "__main__":
    prepare_shared_project()
