"""
Fixture test that records the environment parallel_run.sh gave its session.

It opens the store named by UNIFY_STORE_PATH (creating the file, exactly as
the runtime's first write would) and drops a JSON snapshot of the variables
the runner is responsible for next to it, as ``<store>.env.json``. The
runner's own tests read that snapshot to check what each session received.
"""

import json
import os
import sqlite3
from pathlib import Path

REPORTED_VARS = (
    "UNIFY_STORE_PATH",
    "UNILLM_CACHE",
    "UNIFY_TEST_SOCKET",
    "UNIFY_LOG_SUBDIR",
    "UNIFY_TMUX_SESSION_ID",
)


def test_report_env():
    store = Path(os.environ["UNIFY_STORE_PATH"])
    assert store.parent.is_dir(), f"store directory missing: {store.parent}"
    sqlite3.connect(store).close()
    assert store.is_file()

    snapshot = {name: os.environ.get(name) for name in REPORTED_VARS}
    Path(f"{store}.env.json").write_text(json.dumps(snapshot), encoding="utf-8")
