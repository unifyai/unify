"""
Fixtures for ConversationManager → CodeActActor integration tests.

Key properties:
- Function-scoped ConversationManager and CodeActActor (shared async event loop per test)
- Deterministic waits: no fixed sleeps; explicit timeouts everywhere
"""

from __future__ import annotations

import os

# Must be set before SETTINGS is first imported so per-test contexts are pre-created.
os.environ["UNIFY_PRETEST_CONTEXT_CREATE"] = "true"

from pathlib import Path
from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from tests.helpers import scenario_file_lock
from tests.conversation_manager.cm_test_driver import CMStepDriver


def pytest_configure(config) -> None:
    """
    Configure environment variables for CodeActActor integration tests.

    Note: parent CM tests' conftest sets UNIFY_ACTOR_IMPL="simulated". We
    inject CodeActActor directly, so we do NOT rely on UNIFY_ACTOR_IMPL.
    """
    os.environ["UNIFY_PRETEST_CONTEXT_CREATE"] = "true"
    import tests.settings as test_settings_module

    test_settings_module._SettingsProxy._instance = None

    os.environ.setdefault("TEST", "true")

    # These tests exercise the production actor over real stored skills.
    os.environ["UNIFY_FUNCTION_IMPL"] = "real"
    os.environ["UNIFY_GUIDANCE_IMPL"] = "real"

    # Production actor/model defaults (openai/gpt-5.6-sol@openrouter) come from SETTINGS.

    # Ensure NEW marker comparisons are stable in tests.
    os.environ.setdefault("UNIFY_INCREMENTING_TIMESTAMPS", "true")


@pytest.fixture(autouse=True)
def _isolate_local_workspace_home(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    """Give each CM↔CodeAct integration test its own local workspace root.

    ``parallel_run.sh`` runs these tests concurrently. A shared workspace
    would let one test's outbound files be overwritten or removed by another
    mid-flight.

    Hash the node id so the same test always gets the same path (stable
    LLM cache keys that embed the workspace root), matching the actor
    suite's isolation fixture.

    After switching the root, clear manager singletons and create the
    directory the actor prompt embeds via ``get_local_root()``.
    """
    import hashlib
    import shutil
    import tempfile
    from pathlib import Path

    from unify.workspace import get_local_root
    from unify.manager_registry import ManagerRegistry
    from unify.settings import SETTINGS

    suffix = hashlib.md5(request.node.nodeid.encode("utf-8")).hexdigest()[:12]
    test_home = os.path.join(tempfile.gettempdir(), f"unity_test_home_{suffix}")
    os.makedirs(test_home, exist_ok=True)
    monkeypatch.setenv("UNIFY_LOCAL_ROOT", test_home)
    monkeypatch.setattr(SETTINGS, "UNIFY_LOCAL_ROOT", test_home)
    ManagerRegistry.clear()
    Path(get_local_root()).mkdir(parents=True, exist_ok=True)
    yield test_home
    shutil.rmtree(test_home, ignore_errors=True)


@pytest_asyncio.fixture(autouse=True)
async def _reset_litellm_logging_worker_per_test():
    """
    Pytest-asyncio runs different fixture scopes on different event loops.

    LiteLLM's GLOBAL_LOGGING_WORKER uses an asyncio.Queue bound to the event loop
    that first initialized it. When later used from a different loop, it can raise:
      "Queue ... is bound to a different event loop"

    We reset the worker per test so it always initializes on the current test loop.
    """
    try:
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

        try:
            await GLOBAL_LOGGING_WORKER.stop()
        except Exception:
            pass
        # Force re-init on next use.
        try:
            GLOBAL_LOGGING_WORKER._worker_task = None
            GLOBAL_LOGGING_WORKER._running_tasks.clear()
            GLOBAL_LOGGING_WORKER._queue = None
            GLOBAL_LOGGING_WORKER._sem = None
        except Exception:
            pass
    except Exception:
        # If litellm isn't installed/available, ignore.
        pass

    yield


@pytest_asyncio.fixture(scope="function")
async def conversation_manager_codeact(
    request,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[CMStepDriver]:
    """
    Start ConversationManager in-process for CodeActActor integration tests.

    NOTE: This fixture is function-scoped because ConversationManager spawns asyncio
    tasks (e.g., actor_watch_result) that must run on the same event loop as the
    test's CodeActActor handle. Module-scoped async fixtures run on a different
    loop under pytest-asyncio strict mode, which can prevent ActorResult propagation.

    The runtime honours the per-test context as its session root only in test
    mode, so ``SETTINGS.TEST`` is pinned on for the fixture's lifetime; without
    it the managers bind to the default assistant root while the test body
    reads from its own re-rooted context.
    """
    from unify import db
    from unify import settings as unify_settings
    from tests.settings import SETTINGS

    monkeypatch.setattr(unify_settings.SETTINGS, "TEST", True)
    from unify.conversation_manager.event_broker import reset_event_broker
    from unify.conversation_manager import start_async, stop_async
    from unify.conversation_manager.domains import managers_utils
    from unify.common.prompt_helpers import now as prompt_now

    test_ctx = getattr(request.node, "_unity_unify_test_ctx", None)
    assert (
        test_ctx
    ), "Integration tests require the per-test Unify context from conftest"

    db.activate(SETTINGS.test_project_name, overwrite=False)
    db.set_context(test_ctx, relative=False, skip_create=False)

    reset_event_broker()

    from unify.common.context_registry import ContextRegistry
    from unify.common.runtime_context import bind_runtime_context_root

    bind_runtime_context_root(strict=True)

    original_init_managers = managers_utils._init_managers

    def _init_managers_with_test_context(cm, loop, actor=None):
        db.activate(SETTINGS.test_project_name, overwrite=False)
        db.set_context(test_ctx, relative=False, skip_create=True)
        bind_runtime_context_root(strict=True)
        ContextRegistry.set_base_context(test_ctx)
        return original_init_managers(cm, loop, actor)

    managers_utils._init_managers = _init_managers_with_test_context

    cm = await start_async(project_name="TestProject")

    # Initialize managers once. Actor created here is a placeholder; tests override per-test.
    try:
        cm.initialized = False
        with scenario_file_lock("cm_integration_codeact"):
            bind_runtime_context_root(strict=True)
            await managers_utils.init_conv_manager(cm)
        await managers_utils.wait_for_initialization(cm)

        db.activate(SETTINGS.test_project_name, overwrite=False)
        db.set_context(test_ctx, relative=False, skip_create=True)
        bind_runtime_context_root(strict=True)
        ContextRegistry.set_base_context(test_ctx)
    finally:
        managers_utils._init_managers = original_init_managers

    # Reset last_snapshot to the (possibly patched) prompt_now time.
    cm.last_snapshot = prompt_now(as_string=False)

    driver = CMStepDriver(cm)
    yield driver

    await stop_async()
    reset_event_broker()


@pytest_asyncio.fixture
async def code_act_actor() -> AsyncIterator[object]:
    """Create a production-wired CodeActActor for CM integration tests."""
    from unify.actor.code_act_actor import CodeActActor
    from unify.actor.environments import ActorEnvironment
    from unify.manager_registry import ManagerRegistry

    ManagerRegistry.clear()
    # Built directly rather than through the registry: the parent suite pins
    # the registry's actor implementation to the simulated one, and these
    # tests exercise the production actor.
    actor = CodeActActor(environments=[ActorEnvironment()])

    try:
        yield actor
    finally:
        try:
            await actor.close()
        except Exception:
            pass


@pytest.fixture
def initialized_cm_codeact(
    conversation_manager_codeact: CMStepDriver,
    code_act_actor: object,
) -> CMStepDriver:
    """
    Provide a clean CM state + a fresh CodeActActor bound to cm.actor.

    Clears cross-test state, then injects the per-test actor instance.
    """
    driver = conversation_manager_codeact

    # Clear any conversation state from previous tests.
    driver.cm.chat_history.clear()
    driver.cm.in_flight_actions.clear()
    driver.cm.completed_actions.clear()
    driver.cm.brain_messages.clear()

    # Bind per-test actor.
    driver.cm.actor = code_act_actor

    # Ensure fixture files exist (sanity check).
    fixtures_dir = Path(__file__).parent / "fixtures"
    assert fixtures_dir.exists(), f"Missing fixtures directory: {fixtures_dir}"

    # Reset last_snapshot to use current (possibly patched) prompt time.
    from unify.common.prompt_helpers import now as prompt_now

    driver.cm.last_snapshot = prompt_now(as_string=False)

    return driver


@pytest.fixture(scope="module")
def test_files(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """
    Return absolute paths for sample fixture files.
    """
    fixtures_dir = Path(__file__).parent / "fixtures"

    return {
        "test_report.pdf": str(fixtures_dir / "test_report.pdf"),
        "test_data.csv": str(fixtures_dir / "test_data.csv"),
    }
