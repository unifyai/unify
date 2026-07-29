# --------------------------------------------------------------------------- #
#  Helper to seed a deterministic task set for TaskScheduler testing         #
# --------------------------------------------------------------------------- #
from __future__ import annotations

import os
from typing import Dict, Any, List, Tuple

import pytest
import pytest_asyncio
import unisdk

from unify.task_scheduler.task_scheduler import TaskScheduler
from unify.task_scheduler.types.task import Task
from unify.manager_registry import ManagerRegistry
from unify.common.context_registry import ContextRegistry
from tests.helpers import (
    scenario_file_lock,
    mutation_test_lock,
    restore_scenario_context,
)

# Separate commit hash storage for read vs mutation contexts
_READ_SCENARIO_COMMIT_HASHES: Dict[str, Any] = {}
_MUTATION_SCENARIO_COMMIT_HASHES: Dict[str, Any] = {}


# Task data for seeding (shared by both scenarios). The arming split is load
# bearing: read-only tests count armed against paused tasks, so a scenario where
# every task carries the same `enabled` value cannot tell a correct answer from a
# uniform one.
_TASKS_DATA: List[Dict[str, Any]] = [
    {
        "name": "Write quarterly report",
        "description": "Draft the Q2 report (send email to finance).",
        "enabled": True,
    },
    {
        "name": "Prepare slide deck",
        "description": "Create slides for the board meeting. Email once done.",
        "enabled": True,
    },
    {
        "name": "Client follow-up email",
        "description": "Send email to prospective client about proposal.",
        "enabled": False,
    },
]


def _declared_drift(task: Task, declared: Dict[str, Any]) -> Dict[str, Any]:
    """Return the declared fields whose stored values no longer match."""

    return {
        field: value
        for field, value in declared.items()
        if field != "name" and getattr(task, field) != value
    }


def _seed_tasks(ts: TaskScheduler) -> Tuple[List[int], bool]:
    """Reconcile the scenario against _TASKS_DATA.

    Returns the task IDs in declaration order, plus whether anything was
    written. Tasks are matched by name and updated in place when a declared
    field has drifted, so editing _TASKS_DATA converges an already-seeded
    context instead of being silently ignored.
    """

    task_ids: List[int] = []
    changed = False
    for task_data in _TASKS_DATA:
        name = task_data["name"]
        existing = ts._filter_tasks(filter=f"name == {name!r}", limit=1)
        if existing:
            task_id = existing[0].task_id
            drift = _declared_drift(existing[0], task_data)
            if drift:
                ts._update_task(task_id=task_id, **drift)
                changed = True
        else:
            task_id = ts._create_task(**task_data)["details"]["task_id"]
            changed = True
        task_ids.append(task_id)
    return task_ids, changed


def _rebuild_commit_hashes(
    ctx_prefix: str,
    commit_hashes: Dict[str, Any],
) -> None:
    """Rebuild commit hashes from existing context commits."""
    existing_contexts = unisdk.get_contexts(prefix=ctx_prefix)
    for ctx_name in existing_contexts.keys():
        history = unisdk.get_context_commits(ctx_name)
        if history:
            commit_hashes[ctx_name] = history[0]["commit_hash"]


def _commit_contexts_for_rollback(
    ctx_prefix: str,
    commit_hashes: Dict[str, Any],
) -> None:
    """Commit all contexts under prefix and store hashes for rollback."""
    existing_contexts = unisdk.get_contexts(prefix=ctx_prefix)
    for ctx_name in existing_contexts.keys():
        commit_info = unisdk.commit_context(
            name=ctx_name,
            commit_message="Initial seed data for task scheduler tests",
        )
        commit_hashes[ctx_name] = commit_info["commit_hash"]


def _setup_scenario(
    request: pytest.FixtureRequest,
    ctx: str,
    lock_name: str,
    commit_hashes: Dict[str, Any],
) -> Tuple[TaskScheduler, List[int]]:
    """
    Common setup logic for seeding a task scheduler scenario.

    Creates/reuses a versioned context, seeds tasks if needed,
    and returns the scheduler + task ID list.
    """
    ManagerRegistry.clear()
    ContextRegistry.clear()
    os.environ["TQDM_DISABLE"] = "1"

    overwrite_scenarios = request.config.getoption("--overwrite-scenarios")

    # If --overwrite-scenarios is set, delete existing contexts first
    if overwrite_scenarios:
        existing_contexts = unisdk.get_contexts(prefix=ctx)
        for ctx_name in existing_contexts.keys():
            unisdk.delete_context(ctx_name)

    # Set context before any operations
    unisdk.create_context(ctx)  # exist_ok=True by default
    unisdk.set_context(ctx, relative=False)

    # Create scheduler
    ts = TaskScheduler()
    task_ids: List[int] = []

    # Use file lock to coordinate seeding across parallel processes
    with scenario_file_lock(lock_name):
        task_ids, changed = _seed_tasks(ts)
        if changed:
            # The rollback baseline has to contain the reconciled data.
            print(f"Seeded task scheduler scenario ({ctx})...")
            _commit_contexts_for_rollback(ctx, commit_hashes)
        else:
            print(f"Scenario already seeded ({ctx}), rebuilding local state...")
            _rebuild_commit_hashes(ctx, commit_hashes)

    unisdk.unset_context()
    return ts, task_ids


# ---------------------------------------------------------------------------
# READ-ONLY SCENARIO (for test_ask.py, test_sys_msgs.py, etc.)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def task_read_scenario(
    request: pytest.FixtureRequest,
) -> Tuple[TaskScheduler, List[int]]:
    """
    Session-scoped scenario for READ-ONLY tests.

    Uses context: tests/task_scheduler/ReadScenario

    Read-only tests can run fully in parallel since they only read data
    and their rollbacks don't affect mutation tests (separate context).
    """
    return _setup_scenario(
        request,
        ctx="tests/task_scheduler/ReadScenario",
        lock_name="ts_read_scenario",
        commit_hashes=_READ_SCENARIO_COMMIT_HASHES,
    )


@pytest.fixture(scope="function")
def task_scheduler_read_scenario(task_read_scenario):
    """
    Per-test fixture for READ-ONLY tests (e.g., test_ask.py, test_sys_msgs.py).

    Rolls back to committed state before each test. These tests can run
    fully in parallel since they use a separate context from mutation tests.
    """
    ts, task_ids = task_read_scenario

    def rollback_context(ctx):
        unisdk.rollback_context(
            name=ctx,
            commit_hash=_READ_SCENARIO_COMMIT_HASHES[ctx],
        )

    # Rollback to clean state before test
    restore_scenario_context("tests/task_scheduler/ReadScenario")
    ctx_names = list(_READ_SCENARIO_COMMIT_HASHES.keys())
    if ctx_names:
        unisdk.map(rollback_context, ctx_names, mode="asyncio")

    restore_scenario_context("tests/task_scheduler/ReadScenario")
    yield ts, task_ids


# ---------------------------------------------------------------------------
# MUTATION SCENARIO (for test_update_complex.py, etc.)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def task_mutation_scenario(
    request: pytest.FixtureRequest,
) -> Tuple[TaskScheduler, List[int]]:
    """
    Session-scoped scenario for MUTATION tests.

    Uses context: tests/task_scheduler/MutationScenario

    Mutation tests use a separate context from read-only tests, ensuring
    that read tests' rollbacks cannot interfere with mutation operations.
    """
    return _setup_scenario(
        request,
        ctx="tests/task_scheduler/MutationScenario",
        lock_name="ts_mutation_scenario",
        commit_hashes=_MUTATION_SCENARIO_COMMIT_HASHES,
    )


@pytest.fixture(scope="function")
def task_scheduler_mutation_scenario(task_mutation_scenario):
    """
    Per-test fixture for tests that MUTATE task data (create, update, delete).

    Uses a SEPARATE context from read-only tests, plus a file lock to serialize
    mutation tests among themselves. This ensures:

    1. Read tests' rollbacks cannot affect mutation tests (different context)
    2. Mutation tests don't race with each other (serialized via lock)
    3. The full sequence (rollback → mutate → verify) is atomic
    """
    ts, task_ids = task_mutation_scenario

    def rollback_context(ctx):
        unisdk.rollback_context(
            name=ctx,
            commit_hash=_MUTATION_SCENARIO_COMMIT_HASHES[ctx],
        )

    with mutation_test_lock("ts_mutation"):
        restore_scenario_context("tests/task_scheduler/MutationScenario")
        # Rollback INSIDE the lock to prevent other mutation tests
        # from rolling back while this test is running
        ctx_names = list(_MUTATION_SCENARIO_COMMIT_HASHES.keys())
        if ctx_names:
            unisdk.map(rollback_context, ctx_names, mode="asyncio")

        restore_scenario_context("tests/task_scheduler/MutationScenario")
        yield ts, task_ids
