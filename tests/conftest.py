"""
tests/conftest.py
=================

Global pytest configuration for Unity test suite.

Sections:
  1. Imports and logging guard
  2. Test stubs (Redis, ComputerWorker, DateTime)
  3. Singleton isolation
  4. Command-line options
  5. Custom logging helpers
  6. Session lifecycle hooks
  7. Test run hooks
  8. HTTP client cleanup
  9. Pre-run context creation
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import time

import hashlib

import httpx
import pytest
import requests
import unisdk
from pytest_metadata.plugin import metadata_key

# Imported as a symbol (not via module attribute access at call time) so a
# unisdk checkout that predates it fails collection with an ImportError here,
# rather than an AttributeError inside per-test setup's broad exception
# handlers — which would silently skip set_context and collapse every test
# into one shared context root.
from unisdk.utils.http import default_timeout as unisdk_default_timeout

from datetime import datetime, timezone

# --------------------------------------------------------------------------- #
# 1. Early logging guard                                                      #
# --------------------------------------------------------------------------- #
# Ensure a handler exists before imports that might call logging.basicConfig()
_root_logger_early = logging.getLogger()
if not _root_logger_early.handlers:
    _root_logger_early.addHandler(logging.NullHandler())

from tests.helpers import PRECREATED_CONTEXTS, set_session_tags
from tests.settings import SETTINGS
from unify.session_details import UNASSIGNED_ASSISTANT_CONTEXT, UNASSIGNED_USER_CONTEXT


# --------------------------------------------------------------------------- #
# Orchestra availability check for requires_orchestra marker                   #
# --------------------------------------------------------------------------- #
def _check_orchestra_available() -> bool:
    """Check if Orchestra server is reachable.

    The verdict is cached for the process lifetime so every caller —
    pytest_sessionstart's init decision, per-test ``requires_orchestra``
    skips, per-test remote context setup — agrees with the decision made at
    session start. A mid-session flip would be worse than either steady
    state: if session setup skipped ``unify.init()``, later tests seeing
    True would run against an uninitialised runtime.

    Under parallel_run (``UNITY_TEST_SOCKET`` set) the first call retries
    for up to 30s before concluding False: parallel_run boots Orchestra
    just before spawning sessions, and dozens of sessions probing a
    still-warming server can also transiently time out even after
    ``local.sh check`` passed. Caching one racy False made
    pytest_sessionstart skip ``unify.init()`` while tests still ran,
    crashing later with "EVENT_BUS has not been initialised yet". Outside
    parallel_run a single probe decides immediately, so a developer running
    plain pytest without Orchestra doesn't wait.
    """
    if hasattr(_check_orchestra_available, "_cached"):
        return _check_orchestra_available._cached

    base = os.environ.get("ORCHESTRA_URL", "http://localhost:8000")
    # ORCHESTRA_URL may or may not include the `/v0` API prefix — both
    # `http://127.0.0.1:8000` and `http://127.0.0.1:8000/v0` are valid in
    # practice (parallel_run.sh exports the latter via local.sh's
    # cmd_check). Mirror the URL handling in _prepare_shared_project.py
    # (added in e47d5c648) so we hit `/v0/projects` exactly once. Before
    # this fix, a `/v0`-suffixed ORCHESTRA_URL would resolve to
    # `/v0/v0/projects` → 404 → returns False → pytest_sessionstart
    # silently skipped unify.init() → eval tests crashed downstream with
    # "EVENT_BUS has not been initialised yet".
    if base.endswith("/v0"):
        url = f"{base}/projects"
    else:
        url = f"{base.rstrip('/')}/v0/projects"

    deadline = time.monotonic() + (30.0 if os.environ.get("UNITY_TEST_SOCKET") else 0.0)
    while True:
        try:
            with httpx.Client(timeout=2.0) as client:
                resp = client.get(url)
                # 200 = success, 401/403 = auth required but server is up
                available = resp.status_code in (200, 401, 403)
        except Exception:
            available = False
        if available or time.monotonic() >= deadline:
            break
        time.sleep(1.0)

    _check_orchestra_available._cached = available
    return available


def _derive_test_context(item: pytest.Item) -> str:
    """
    Derive a per-test Unify context path that is stable and unique.

    Matches the intent of tests.helpers._TestContext.setup(), but runs early enough
    (pytest_runtest_setup) to wrap fixture setup + teardown, preventing cross-test
    interference when fixtures create/clear managers that delete contexts.
    """
    # Build "tests/<relpath-without-.py>/<func_name>" prefix
    file_path = str(getattr(item, "fspath", "") or "")
    parts = file_path.split(f"{os.sep}tests{os.sep}")
    if len(parts) > 1:
        rel_path = parts[1].replace(os.sep, "/")
        if rel_path.endswith(".py"):
            rel_path = rel_path[:-3]
        test_path = f"tests/{rel_path}"
    else:
        # Fallback (should be rare): use nodeid as the "path"
        test_path = "tests/unknown"

    func_name = getattr(item, "originalname", None) or getattr(item, "name", "test")

    # Parametrized tests: include a stable suffix so contexts don't collide
    nodeid = getattr(item, "nodeid", "")
    if "[" in nodeid:
        normalized = _normalize_pytest_nodeid(nodeid)
        if normalized is None:
            normalized = hashlib.md5(nodeid.encode("utf-8")).hexdigest()[:8]
        func_name = f"{func_name}/{normalized}"

    # Mirror production hierarchy: .../{user_id}/{assistant_id}
    return f"{test_path}/{func_name}/{UNASSIGNED_USER_CONTEXT}/{UNASSIGNED_ASSISTANT_CONTEXT}"


# Trips permanently for this process the first time a per-test setup call
# stalls or loses the connection, so one network incident costs one bounded
# timeout instead of taxing every subsequent test. Local context activation
# still happens for every test; only the remote round-trips are skipped.
_ORCHESTRA_SETUP_BROKEN = False

_SETUP_NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)


def _trip_orchestra_setup_breaker(exc: Exception) -> None:
    global _ORCHESTRA_SETUP_BROKEN
    if not _ORCHESTRA_SETUP_BROKEN:
        _ORCHESTRA_SETUP_BROKEN = True
        print(
            "\n[unity-conftest] Orchestra became unreachable during test setup "
            f"({type(exc).__name__}); switching to local-only context setup for "
            "the rest of this session. Tests that need Orchestra will fail or "
            "skip on their own.\n",
        )


def _orchestra_usable_for_setup() -> bool:
    return not _ORCHESTRA_SETUP_BROKEN and _check_orchestra_available()


def _reset_singleton_registries() -> None:
    # Ensure singleton registries don't leak across tests and that fixtures see
    # the correct context for any context-derived subcontexts (e.g. FunctionManager).
    try:
        from unify.common.context_registry import ContextRegistry
        from unify.manager_registry import ManagerRegistry
        from unify.events.event_bus import EVENT_BUS

        ManagerRegistry.clear()
        ContextRegistry.clear()
        EVENT_BUS.clear(delete_contexts=False)
    except Exception:
        pass


def _assert_test_context_active(ctx: str) -> None:
    # set_context activates the local context vars before any remote
    # round-trip, so after setup they must hold the per-test root no matter
    # what the network did. If they don't, every subsequent test would share
    # one context root and cross-contaminate — fail the session here instead.
    active = unisdk.get_active_context()
    assert active.get("read") == ctx and active.get("write") == ctx, (
        f"Per-test Unify context activation failed: expected {ctx!r}, "
        f"active is {active!r}. Test isolation would be lost."
    )


def _set_unify_context_for_test(item: pytest.Item) -> None:
    """Set a unique per-test Unify context early (before fixtures)."""
    ctx = _derive_test_context(item)
    setattr(item, "_unity_unify_test_ctx", ctx)

    if not _orchestra_usable_for_setup():
        # Local-only: activate the per-test context without touching the
        # network, keeping context isolation for tests that never leave the
        # process. Tests that need Orchestra fail or skip on their own.
        unisdk.set_context(ctx, relative=False, skip_create=True)
        _reset_singleton_registries()
        _assert_test_context_active(ctx)
        return

    # Clean slate unless contexts are pre-created during collection.
    skip_ctx_create = False
    if SETTINGS.UNIFY_PRETEST_CONTEXT_CREATE:
        skip_ctx_create = ctx in PRECREATED_CONTEXTS
    else:
        try:
            with unisdk_default_timeout(SETTINGS.UNIFY_TEST_SETUP_HTTP_TIMEOUT):
                unisdk.delete_context(ctx)
        except _SETUP_NETWORK_ERRORS as exc:
            _trip_orchestra_setup_breaker(exc)
            unisdk.set_context(ctx, relative=False, skip_create=True)
            _reset_singleton_registries()
            _assert_test_context_active(ctx)
            return
        except Exception:
            pass

    try:
        with unisdk_default_timeout(SETTINGS.UNIFY_TEST_SETUP_HTTP_TIMEOUT):
            unisdk.set_context(ctx, relative=False, skip_create=skip_ctx_create)
    except _SETUP_NETWORK_ERRORS as exc:
        # set_context activates the local context vars before its remote
        # round-trip, so the test still runs in the right context.
        _trip_orchestra_setup_breaker(exc)
    except Exception:
        pass  # Project may not exist yet; tests that need it will fail on their own

    _reset_singleton_registries()
    _assert_test_context_active(ctx)


def _uses_unify_context(item: pytest.Item) -> bool:
    """Return whether this test needs the default per-test Unify context."""

    return item.get_closest_marker("no_unify_context") is None


def _unset_unify_context_for_test(item: pytest.Item) -> None:
    """Unset (and optionally delete) the per-test Unify context after fixture teardown."""
    ctx = getattr(item, "_unity_unify_test_ctx", None)
    try:
        if (
            ctx
            and SETTINGS.UNIFY_DELETE_CONTEXT_ON_EXIT
            and _orchestra_usable_for_setup()
        ):
            try:
                with unisdk_default_timeout(
                    SETTINGS.UNIFY_TEST_SETUP_HTTP_TIMEOUT,
                ):
                    unisdk.delete_context(ctx)
            except _SETUP_NETWORK_ERRORS as exc:
                _trip_orchestra_setup_breaker(exc)
            except Exception:
                pass
    finally:
        try:
            unisdk.unset_context()
        except Exception:
            pass


def pytest_report_header(config):
    settings_str = [f"{k}={v}" for k, v in SETTINGS.model_dump().items()]
    return [
        f"orchestra_url={os.environ.get('ORCHESTRA_URL')}",
        f"unity_comms_url={os.environ.get('UNITY_COMMS_URL')}",
        f"unify_project={unisdk.active_project()}",
        f"UNILLM_CACHE={os.environ.get('UNILLM_CACHE', 'not set')}",
    ] + settings_str


# --------------------------------------------------------------------------- #
# 2. Test stubs (ComputerWorker, DateTime)                                     #
# --------------------------------------------------------------------------- #

_FIXED_DATETIME = datetime(2025, 6, 13, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def static_now():
    return _FIXED_DATETIME


@pytest.fixture(autouse=True)
def stub_external_deps(monkeypatch):
    """
    This fixture automatically stubs heavy external dependencies for tests.
    It runs for every test.
    """

    # --- DateTime stub for prompts (centralized) -----------------------------------
    # All timestamps in prompts come from prompt_helpers.now() which returns either:
    # - A formatted string (as_string=True): "Friday, June 13, 2025 at 12:00 PM UTC"
    # - A datetime object (as_string=False): for timestamp comparisons
    #
    # When UNITY_INCREMENTING_TIMESTAMPS is enabled (e.g., ConversationManager tests),
    # datetime objects auto-increment by microseconds so last_snapshot < message.timestamp
    # comparisons work correctly for **NEW** markers.

    from datetime import timedelta

    _timestamp_counter = {"value": 0}

    def _static_now(time_only: bool = False, as_string: bool = True):
        """Return a fixed timestamp for testing."""
        if SETTINGS.UNITY_INCREMENTING_TIMESTAMPS and not as_string:
            # Return incrementing datetime for **NEW** marker comparisons
            _timestamp_counter["value"] += 1
            return _FIXED_DATETIME + timedelta(microseconds=_timestamp_counter["value"])

        if not as_string:
            return _FIXED_DATETIME

        label = "UTC"
        if time_only:
            return _FIXED_DATETIME.strftime("%I:%M %p ") + label
        return _FIXED_DATETIME.strftime("%A, %B %d, %Y at %I:%M %p ") + label

    # Patch prompt_helpers.now everywhere it's imported
    monkeypatch.setattr("unify.common.prompt_helpers.now", _static_now)
    monkeypatch.setattr("unify.secret_manager.prompt_builders.now", _static_now)
    monkeypatch.setattr("unify.image_manager.prompt_builders.now", _static_now)
    monkeypatch.setattr("unify.memory_manager.prompt_builders.now", _static_now)
    monkeypatch.setattr("unify.file_manager.prompt_builders.now", _static_now)
    monkeypatch.setattr("unify.conversation_manager.prompt_builders.now", _static_now)
    monkeypatch.setattr("unify.conversation_manager.events.prompt_now", _static_now)
    monkeypatch.setattr(
        "unify.conversation_manager.domains.contact_index.prompt_now",
        _static_now,
    )
    monkeypatch.setattr(
        "unify.conversation_manager.domains.managers_utils.prompt_now",
        _static_now,
    )
    monkeypatch.setattr(
        "unify.conversation_manager.conversation_manager.prompt_now",
        _static_now,
    )

    # --- DateTime stub for production wall-clock comparisons ---------------
    # task_scheduler.task_scheduler uses `datetime.now(timezone.utc)`
    # directly (not prompt_helpers.now) to decide whether a
    # schedule.start_at lands in the future. With prompt_helpers.now stubbed
    # to 2025-06-13 (so cached LLM responses are deterministic), the LLM
    # generates start_at values relative to 2025-06-13 — e.g. "next Monday"
    # → 2025-06-16. But production then compares those LLM-generated
    # timestamps against the real wall-clock, which is now ~12 months in
    # the future. Result: tasks the LLM intends as future-scheduled may get
    # an incorrect resolved status. Patch task_scheduler.datetime so the
    # two time sources agree under test.
    #
    # NOTE: the production class is `from datetime import datetime`, so we
    # patch the imported name on the module. A subclass-with-overridden-
    # now() shim is needed because only `.now()` should be overridden —
    # the rest of the datetime API (datetime.combine, datetime.strptime,
    # etc., as used in repeat-pattern math) must keep working.
    class _StubbedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return _FIXED_DATETIME
            return _FIXED_DATETIME.astimezone(tz)

    monkeypatch.setattr(
        "unify.task_scheduler.task_scheduler.datetime",
        _StubbedDatetime,
    )

    def _static_perf_counter() -> float:
        return 1000.0

    monkeypatch.setattr(
        "unify.common._async_tool.time_context.perf_counter",
        _static_perf_counter,
    )


# --------------------------------------------------------------------------- #
# 3. Singleton isolation                                                      #
# --------------------------------------------------------------------------- #

from unify.common.context_registry import ContextRegistry
from unify.manager_registry import ManagerRegistry


@pytest.fixture(autouse=True)
def _clear_singletons_between_tests():
    """Ensure *singleton* instances never leak from one test to the next."""
    yield
    ManagerRegistry.clear()  # Clear the registry after each test
    ContextRegistry.clear()  # Clear the context handler after each test


@pytest.fixture(autouse=True)
def _exact_cache_keying_for_evals(request):
    """An eval never accepts a canonically-equivalent recording.

    Canonical keying deliberately lets a recording survive mundane prompt
    churn, and the invariance it buys includes description rewording --
    which is one of the ordinary ways to change which tool a model picks.
    For a functional test that trade is right: the assertion is about the
    code around the call. For an eval it inverts, because the model's
    behaviour *is* the subject, so a canonical hit would score the
    trajectory recorded before the change and report it green.

    Scoped per test rather than per shard: a shard is a directory, and
    directories hold both kinds.
    """
    if request.node.get_closest_marker("eval") is None:
        yield
        return

    from unillm.settings import SETTINGS as UNILLM_SETTINGS

    previous = UNILLM_SETTINGS.UNILLM_CACHE_KEYING
    UNILLM_SETTINGS.UNILLM_CACHE_KEYING = "exact"
    try:
        yield
    finally:
        UNILLM_SETTINGS.UNILLM_CACHE_KEYING = previous


@pytest.fixture(autouse=True)
def _enable_eventbus_for_marked_tests(request):
    """Enable EventBus publishing for tests marked with @pytest.mark.enable_eventbus.

    By default, EventBus publishing is disabled during tests (via SETTINGS).
    Tests that need to verify event publishing behavior opt-in via the marker.
    """
    from unify.events.event_bus import EventBus

    if request.node.get_closest_marker("enable_eventbus"):
        EventBus._publishing_enabled = True
        yield
        EventBus._publishing_enabled = SETTINGS.EVENTBUS_PUBLISHING_ENABLED
    else:
        yield


# --------------------------------------------------------------------------- #
# 4. Command-line options                                                     #
# --------------------------------------------------------------------------- #


def pytest_addoption(parser):
    parser.addoption(
        "--unify-stub",
        action="store_true",
        help="Use an in-memory stub for unite.log / projects whilst "
        "leaving LLM calls intact.",
    )
    parser.addoption(
        "--overwrite-scenarios",
        action="store_true",
        default=False,
        help="Delete and recreate all test scenarios from scratch.",
    )
    parser.addoption(
        "--test-tags",
        action="store",
        default="",
        help="Comma-separated list of tags to associate with this test run "
        "(logged to the Combined context). Falls back to UNIFY_TEST_TAGS env var.",
    )

    group = parser.getgroup("custom-logging")
    group.addoption(
        "--test-log-enable",
        action="store_true",
        default=False,
        help="Enable test-aware logging (adds test name to log records).",
    )
    group.addoption(
        "--test-log-file",
        action="store",
        default="tests.log",
        help="Filename to write test-aware logs to (only applies if --test-log-enable is used).",
    )
    group.addoption(
        "--test-log-format",
        action="store",
        default="[%(levelname)s] %(asctime)s - %(test_name)s: %(message)s",
        help="Custom log format string (only applies if --test-log-enable is used).",
    )


# --------------------------------------------------------------------------- #
# 5. Custom logging helpers                                                   #
# --------------------------------------------------------------------------- #


class TestNameLogFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.test_name = None

    def set_test_name(self, test_name):
        self.test_name = test_name.split("tests/")[-1]

    def reset_test_name(self):
        self.test_name = ""

    def filter(self, record):
        record.test_name = self.test_name or "UNKNOWN"
        return True


test_name_log_filter = TestNameLogFilter()


@pytest.fixture(scope="session", autouse=True)
def configure_logging(request):
    config = request.config
    if not is_test_logging_enabled(config):
        return

    logger = logging.getLogger()
    file_handler = logging.FileHandler(get_test_log_file(config), mode="w")
    formatter = logging.Formatter(
        get_test_log_format(config),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(test_name_log_filter)
    logger.addHandler(file_handler)


def is_test_logging_enabled(config):
    return config.getoption("--test-log-enable")


def get_test_log_file(config):
    return config.getoption("--test-log-file")


def get_test_log_format(config):
    return config.getoption("--test-log-format")


# --------------------------------------------------------------------------- #
# 6. Session lifecycle hooks                                                  #
# --------------------------------------------------------------------------- #


def pytest_sessionstart(session):
    if os.environ.get("SKIP_UNITY_TEST_INIT"):
        return

    # ------------------------------------------------------------------
    #  Initialize Unity's OpenTelemetry TracerProvider FIRST
    #  This ensures Unity owns the provider (service: "unity") before
    #  any library (unify, unillm) makes traced calls.
    # ------------------------------------------------------------------
    from unify.logger import get_tracer

    get_tracer()  # Creates TracerProvider with service="unity" if OTEL enabled

    # ------------------------------------------------------------------
    #  Skip Orchestra-dependent setup if Orchestra isn't reachable.
    #
    #  Two scenarios produce this state in practice:
    #   - CI: parallel_run.sh's `local.sh start` failed (e.g. docker
    #     unavailable, port conflict). The warning is logged but the
    #     session still launches.
    #   - Local: developer running `pytest` without `unity setup`.
    #
    #  In both cases, crashing here would kill the whole pytest session
    #  before any test (including pure unit tests that don't need
    #  Orchestra) gets to run. Bailing out of session setup instead
    #  leaves the per-test `requires_orchestra` skip logic to do its
    #  job — Orchestra-needing tests skip, unit tests still run.
    # ------------------------------------------------------------------
    if not _check_orchestra_available():
        print(
            "\n[unity-conftest] Orchestra not reachable at "
            f"{os.environ.get('ORCHESTRA_URL', 'http://localhost:8000')}; "
            "skipping session-level project/context setup. "
            "Tests marked `requires_orchestra` will skip; others run as normal.\n",
        )
        return

    # ------------------------------------------------------------------
    #  Optionally delete the project before starting (clean slate)
    #  Skip in shared project mode (UNIFY_SKIP_SESSION_SETUP) because
    #  parallel_run.sh handles deletion at the script level to avoid
    #  race conditions between parallel sessions.
    # ------------------------------------------------------------------

    project_name = SETTINGS.test_project_name

    if (
        SETTINGS.UNIFY_TESTS_DELETE_PROJ_ON_START
        and not SETTINGS.UNIFY_SKIP_SESSION_SETUP
    ):
        try:
            unisdk.delete_project(project_name)
        except Exception:
            pass  # Project may not exist yet

    # ------------------------------------------------------------------
    #  Activate the UnityTests project
    # ------------------------------------------------------------------

    if os.environ.get("GITHUB_ACTIONS"):
        import unillm

        unillm.set_cache_backend("local_separate")

    if SETTINGS.UNIFY_SKIP_SESSION_SETUP:
        # Project and shared contexts already prepared externally (e.g., by
        # ._prepare_shared_project.sh). Just activate without overwrite.
        unisdk.activate(project_name, overwrite=False)
        unisdk.set_user_logging(False)
    else:
        unisdk.activate(
            project_name,
            overwrite=SETTINGS.UNIFY_OVERWRITE_PROJECT,
        )
        unisdk.set_user_logging(False)

    # ------------------------------------------------------------------
    #  Ensure the unity runtime is fully initialised for the test suite
    # ------------------------------------------------------------------

    import unify  # local import to avoid affecting stub installation order

    try:
        unify.init(project_name)
    except Exception:
        # Fallback to default project if UnityTests not available yet
        unify.init()

    # ------------------------------------------------------------------
    #  Ensure the global builtins catalogues (primitives + guidance)
    #  exist. Normally seeded once by tests/_prepare_shared_project.py
    #  before sessions spawn (making this a cheap hash check); the
    #  cross-process lock serialises the rare cold-start race between
    #  parallel sessions.
    # ------------------------------------------------------------------
    from unify.common.builtins import (
        builtins_project,
        builtins_seed_key_override,
    )
    from unify.common.embed_utils import _cross_process_column_lock
    from unify.function_manager.builtins_catalog import seed_builtin_primitives
    from unify.guidance_manager.builtins_catalog import seed_builtin_guidance
    from unify.integrations.builtins_catalog import seed_builtin_integrations
    from unify.workflow_manager.builtins_catalog import ensure_catalog_storage

    with _cross_process_column_lock(builtins_project(), "builtins_seed"):
        with builtins_seed_key_override():
            seed_builtin_primitives()
            seed_builtin_guidance()
            seed_builtin_integrations()
            # Workflows: storage only. The catalogue is a shared, public
            # shelf keyed by project name alone, so it is the same rows a
            # deployment serves — seeding desired state from here publishes
            # whatever this checkout happens to hold, and an empty desired
            # state deletes the shelf out from under everyone pointed at
            # the same backend. Tests that need rows seed their own.
            ensure_catalog_storage(builtins_project())

    # ------------------------------------------------------------------
    #  Configure EventBus publishing (disabled by default in tests)
    # ------------------------------------------------------------------
    from unify.events.event_bus import EventBus

    EventBus._publishing_enabled = SETTINGS.EVENTBUS_PUBLISHING_ENABLED

    # ------------------------------------------------------------------
    #  Parse and store session-level test tags for duration logging
    #  Priority: CLI --test-tags > env var UNIFY_TEST_TAGS
    # ------------------------------------------------------------------
    tags_raw = session.config.getoption("--test-tags", default="")
    if not tags_raw:
        tags_raw = SETTINGS.UNIFY_TEST_TAGS
    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
    set_session_tags(tags)

    # ------------------------------------------------------------------
    #  Ensure the Combined context exists for duration and LLM I/O logging
    #  (idempotent: tolerates pre-existing context/fields and concurrent
    #  creation attempts from parallel pytest sessions)
    # ------------------------------------------------------------------
    if SETTINGS.UNIFY_SKIP_SESSION_SETUP:
        # Combined context already prepared externally; skip creation
        pass
    else:
        unisdk.create_context("Combined")
        try:
            unisdk.create_fields(
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
            pass  # Fields already exist or transient failure


def pytest_sessionfinish(session, exitstatus):
    # Write cache stats to a temp file for parallel_run.sh to consume
    # The file is keyed by UNITY_TMUX_SESSION_ID env var (set by parallel_run.sh)
    try:
        import unillm

        stats = unillm.get_cache_stats()
        session_id = os.environ.get("UNITY_TMUX_SESSION_ID", "")
        if session_id:
            stats_file = f"/tmp/parallel_run_cache_{session_id}.txt"
            with open(stats_file, "w") as f:
                f.write(f"{stats.hits}|{stats.canonical_hits}|{stats.misses}\n")
    except Exception:
        pass  # Don't fail the test run if cache stats writing fails

    # Write LLM provider cost to a temp file for parallel_run.sh to consume
    try:
        session_id = os.environ.get("UNITY_TMUX_SESSION_ID", "")
        if session_id:
            total_cost = sum(cost for _, cost in _session_costs)
            cost_file = f"/tmp/parallel_run_cost_{session_id}.txt"
            with open(cost_file, "w") as f:
                f.write(f"{total_cost:.6g}\n")
    except Exception:
        pass

    # A session that was handed a prepared project does not own it, so it does
    # not tear it down — the runner that prepared it deletes once, after every
    # session has finished. Mirrors the same guard on delete-on-start; without
    # it the first session to exit takes the project out from under the rest.
    if (
        SETTINGS.UNIFY_TESTS_DELETE_PROJ_ON_EXIT
        and not SETTINGS.UNIFY_SKIP_SESSION_SETUP
    ):
        unisdk.delete_project(unisdk.active_project())


def pytest_unconfigure(config):
    """Restore HOME (and HF_HOME if we set it).

    We deliberately do NOT rmtree `/tmp/unity_test_home` here. The path
    is shared across every parallel pytest session that
    ``parallel_run.sh`` spawns (deterministic so LLM cache keys
    embedding ``~/Unity/Local`` stay stable). Wiping it on this
    session's exit also wipes the in-flight venvs (FunctionManager
    creates them under ``$HOME/Unity/Local/.unity/venvs/<ctx>/<id>/``)
    that other still-running pytest sessions are about to invoke —
    producing the "venv python disappeared between prepare_venv() and
    create_subprocess_exec()" RuntimeError that the function_manager/
    python cluster was hitting reliably on every CI matrix run.

    Diagnostic confirming the race: the failure dump showed
    ancestor existence "False" all the way up to ``/tmp/`` —
    i.e. the whole ``unity_test_home/`` tree was gone between
    prepare_venv's verification and the subsequent subprocess
    invocation, ruling out a leaf-only cleanup.

    The directory accumulates on the CI runner across this session
    only — the runner is ephemeral, so it's reclaimed when the runner
    shuts down. On local dev machines users can ``rm -rf
    /tmp/unity_test_home`` manually when they want a clean slate.
    """
    if _original_home is None:
        os.environ.pop("HOME", None)
    else:
        os.environ["HOME"] = _original_home
    if _hf_home_set_by_us:
        os.environ.pop("HF_HOME", None)
    if _speaker_model_set_by_us:
        os.environ.pop("UNIFY_SPEAKER_MODEL_PATH", None)
    if _playwright_path_set_by_us:
        os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if SETTINGS.UNITY_CACHE_STATS:
        import unillm

        stats = unillm.get_cache_stats()
        terminalreporter.section(
            f"Unify cache report | Hits ({stats.get_percentage_of_cache_hits():.2f}%): {stats.hits} ({stats.canonical_hits} canonical) | Misses ({stats.get_percentage_of_cache_misses():.2f}%): {stats.misses} | Reads: {stats.reads} | Writes: {stats.writes}",
        )

    total = sum(cost for _, cost in _session_costs)
    terminalreporter.write_sep("=", f"UNILLM Provider Cost Summary: ${total:.6g}")

    # Record outcome counts for parallel_run.sh. pytest exits 0 when every test
    # skips, so exit status alone cannot tell a session that passed from one
    # that ran nothing — and a summary that calls those the same thing hides
    # coverage silently disappearing.
    try:
        session_id = os.environ.get("UNITY_TMUX_SESSION_ID", "")
        if session_id:
            passed = len(terminalreporter.stats.get("passed", []))
            skipped = len(terminalreporter.stats.get("skipped", []))
            outcome_file = f"/tmp/parallel_run_outcome_{session_id}.txt"
            with open(outcome_file, "w") as f:
                f.write(f"{passed}|{skipped}\n")
    except Exception:
        pass  # Don't fail the test run if outcome stats writing fails


# --------------------------------------------------------------------------- #
# 7. Test run hooks                                                           #
# --------------------------------------------------------------------------- #

from unillm.cost_tracker import capture_costs

_session_costs: list[tuple[str, float]] = []

_original_home: str | None = None
_hf_home_set_by_us: bool = False
_speaker_model_set_by_us: bool = False
_playwright_path_set_by_us: bool = False


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "no_unify_context: skip automatic per-test Unify context setup for pure unit tests",
    )

    # ------------------------------------------------------------------
    # Isolate HOME so that tests never touch the real home directory.
    # get_local_root() defaults to ~/Unity/Local, and the process cwd
    # is set to the same path at startup.  By pointing HOME at a temp
    # dir we keep Attachments/, .env, snapshots, etc. sandboxed.
    #
    # The path is deterministic (not random) so that CodeActActor system
    # prompts — which embed the resolved ~/Unity/Local path — produce
    # stable LLM cache keys across pytest sessions.  Actual test file
    # isolation is handled by pytest's tmp_path fixture, not HOME.
    # ------------------------------------------------------------------
    import tempfile

    global _original_home
    _original_home = os.environ.get("HOME")
    if _original_home:
        os.environ["UNITY_REAL_HOME"] = _original_home
    test_home = os.path.join(tempfile.gettempdir(), "unity_test_home")
    os.makedirs(test_home, exist_ok=True)
    os.environ["HOME"] = test_home

    # Preserve access to the real HuggingFace model cache.  The HOME
    # override above moves ~/.cache/huggingface to a temp dir that won't
    # contain pre-downloaded models (e.g. SmolVLM used by docling's PDF
    # pipeline).  Pinning HF_HOME to the original location avoids
    # redundant multi-GB downloads and the .incomplete-blob hangs that
    # occur when the download is interrupted or raced across sessions.
    global _hf_home_set_by_us
    if "HF_HOME" not in os.environ and _original_home:
        original_hf = os.path.join(_original_home, ".cache", "huggingface")
        if os.path.isdir(original_hf):
            os.environ["HF_HOME"] = original_hf
            _hf_home_set_by_us = True

    # Same problem, same fix, for the speaker-embedding model: it is cached
    # under ~/.cache/unify/speaker_id/ and the HOME override hides it, so
    # every real-model speaker-identification test silently skips instead of
    # running. Globbing (rather than importing speaker_id for the filename)
    # keeps pytest_configure free of heavy imports.
    global _speaker_model_set_by_us
    if "UNIFY_SPEAKER_MODEL_PATH" not in os.environ and _original_home:
        import glob as _glob

        cached_models = _glob.glob(
            os.path.join(_original_home, ".cache", "unify", "speaker_id", "*.onnx"),
        )
        if len(cached_models) == 1:
            os.environ["UNIFY_SPEAKER_MODEL_PATH"] = cached_models[0]
            _speaker_model_set_by_us = True

    # Same problem again, for playwright's browser cache under
    # ~/.cache/ms-playwright.  Without this the canvas render gate finds no
    # chromium and every test that exercises it skips rather than runs.
    global _playwright_path_set_by_us
    if "PLAYWRIGHT_BROWSERS_PATH" not in os.environ and _original_home:
        original_browsers = os.path.join(_original_home, ".cache", "ms-playwright")
        if os.path.isdir(original_browsers):
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = original_browsers
            _playwright_path_set_by_us = True

    config.addinivalue_line(
        "markers",
        "requires_real_unify: mark test as requiring the real unify implementation",
    )
    config.addinivalue_line(
        "markers",
        "eval: mark a test as a fuzzy evaluation test for English language "
        "APIs. Selects the eval tier in discover_test_paths.py, and pins cache "
        "lookups to exact keying so a canonical hit cannot score a trajectory "
        "recorded before the prompt changed.",
    )
    config.addinivalue_line(
        "markers",
        "enable_eventbus: enable EventBus publishing for this test",
    )
    config.addinivalue_line(
        "markers",
        "requires_orchestra: mark test as requiring a running Orchestra server",
    )

    # Required to disable explicit log level if set from pytest.ini or command line options
    if os.environ.get("UNITY_TESTS_CLI_LOGGING", "true").lower() == "false":
        config.option.log_cli_level = None
        config.option.showcapture = "no"
        config.option.capture = "no"

    config.stash[metadata_key]["Settings"] = SETTINGS.model_dump(mode="json")

    # ------------------------------------------------------------------ #
    # Prune non-pytest console handlers so only pytest live logs appear. #
    # Keeps any file handlers (e.g., when --test-log-enable is used).    #
    # ------------------------------------------------------------------ #
    try:
        root = logging.getLogger()
        kept_handlers: list[logging.Handler] = []
        for h in list(root.handlers):
            mod = getattr(h.__class__, "__module__", "")
            is_stream = isinstance(h, logging.StreamHandler)
            is_pytest = mod.startswith("_pytest.logging")
            # Retain pytest's handlers and any non-stream handlers (file, etc.)
            if is_stream and not is_pytest:
                continue
            kept_handlers.append(h)
        root.handlers = kept_handlers
    except Exception:
        # Never fail configuration due to logging hygiene adjustments.
        pass


# Skip tests marked with requires_real_unify when using the unify stub
# Skip tests marked with requires_orchestra when Orchestra is not available
def pytest_runtest_setup(item):
    test_name_log_filter.set_test_name(item.nodeid)
    if not os.environ.get("SKIP_UNITY_TEST_INIT") and _uses_unify_context(item):
        _set_unify_context_for_test(item)

    # Skip requires_orchestra tests if Orchestra is not running
    if item.get_closest_marker("requires_orchestra"):
        if not _check_orchestra_available():
            pytest.skip("Orchestra server not available")


def _normalize_pytest_nodeid(nodeid):
    """
    Try to normalize the pytest nodeid to an alphanumeric string that is
    accepted for unisdk.Context path. If not possible, return None.
    Will fallback to invocation count if empty.
    """
    bracket_match = re.search(r"\[([^\]]+)\]", nodeid)
    if bracket_match:
        bracket_content = bracket_match.group(1)
    else:
        bracket_content = ""

    # Try to normalize to alphanumeric
    normalized = re.sub(r"[^a-zA-Z0-9]", "", bracket_content)

    if len(normalized) == 0:
        return None

    return normalized[:24]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    import types

    func_name = item.originalname

    # For class-based tests, item.obj is a bound method. We need to access
    # the underlying function via __func__ to set/get attributes.
    target_obj = item.obj
    if isinstance(target_obj, types.MethodType):
        target_obj = target_obj.__func__

    if "[" in item.nodeid:  # Any parametrization (markers, fixtures, etc.)
        # Need to keep track of invocation count for parametrized tests
        # In case of a later failure.
        current_count = getattr(target_obj, "_unity_pytest_invocation_count", 0)
        setattr(target_obj, "_unity_pytest_invocation_count", current_count + 1)

        normalized_id = _normalize_pytest_nodeid(item.nodeid)
        if normalized_id is None:
            normalized_id = f"_{current_count}_"
        func_name = f"{func_name}/{normalized_id}"

    setattr(target_obj, "_unity_pytest_nodeid", func_name)

    with capture_costs() as events:
        yield
    item._unillm_cost_events = events


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if call.when == "call":
        events = getattr(item, "_unillm_cost_events", [])
        total = sum(e.provider_cost for e in events)
        report._unillm_cost = total
        _session_costs.append((report.nodeid, total))


@pytest.hookimpl(hookwrapper=True)
def pytest_report_teststatus(report, config):
    outcome = yield
    if report.when == "call":
        result = outcome.get_result()
        if result and len(result) >= 3:
            category, shortletter, verbose = result
            cost = getattr(report, "_unillm_cost", 0.0)
            if isinstance(verbose, str):
                verbose = f"{verbose} [${cost:.6g}]"
            outcome.force_result((category, shortletter, verbose))


def pytest_runtest_teardown(item, nextitem=None):
    if not os.environ.get("SKIP_UNITY_TEST_INIT") and _uses_unify_context(item):
        _unset_unify_context_for_test(item)
    test_name_log_filter.reset_test_name()


def pytest_html_results_summary(prefix, summary, postfix):
    if SETTINGS.UNITY_CACHE_STATS:
        import unillm

        stats = unillm.get_cache_stats()
        prefix.extend(
            [
                f"<h4>Unify Cache Stats Report:</h4>",
                f"<p>Hits ({stats.get_percentage_of_cache_hits():.2f}%): {stats.hits} ({stats.canonical_hits} canonical) | Misses ({stats.get_percentage_of_cache_misses():.2f}%): {stats.misses}</p>",
                f"<p>Reads: {stats.reads} | Writes: {stats.writes}</p>",
            ],
        )


# --------------------------------------------------------------------------- #
# 8. HTTP client cleanup                                                      #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session", autouse=True)
def _close_httpx_clients_at_session_end():
    """
    Track every httpx.AsyncClient that gets created during the session
    and close it gracefully *before* pytest tears the event-loop down.
    """
    created: list[httpx.AsyncClient] = []

    # monkey-patch __init__ to collect instances
    orig_init = httpx.AsyncClient.__init__

    def _patched_init(self, *a, **kw):
        orig_init(self, *a, **kw)
        created.append(self)

    httpx.AsyncClient.__init__ = _patched_init  # type: ignore[assignment]

    yield  # ← tests run here

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for c in created:
        if not c.is_closed:
            # swallow "loop closed" if it still happens for a stray client
            try:
                loop.run_until_complete(c.aclose())
            except RuntimeError:
                pass
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


# --------------------------------------------------------------------------- #
# 9. Pre-run context creation                                                 #
# --------------------------------------------------------------------------- #


def _get_context_name_for_item(item):
    """
    Return the unify context name for a collected pytest item.

    This uses the same logic as tests.helpers._ctx_name to generate a unique context
    path for the test, including any parametrization suffixes.
    """
    original_name = item.originalname or item.name

    # Append normalized parametrization suffix if available
    normalized = _normalize_pytest_nodeid(item.nodeid)

    func_name = original_name
    if normalized is not None:
        func_name = f"{original_name}/{normalized}"

    path = item._nodeid.split(".py")[0]
    return f"{path}/{func_name}"


def pytest_collection_finish(session):
    # Compute all contexts and fire off background creation tasks
    # Skip when UNIFY_SKIP_SESSION_SETUP is set (shared project mode)
    if SETTINGS.UNIFY_PRETEST_CONTEXT_CREATE and not SETTINGS.UNIFY_SKIP_SESSION_SETUP:
        if not _orchestra_usable_for_setup():
            return
        contexts: set[str] = set()
        for item in session.items:
            ctx = _get_context_name_for_item(item)
            contexts.add(ctx)
            contexts.add(f"{ctx}/Events/_callbacks/")

        # TODO: Should delete contexts before creating them
        # But this is mostly fine now for CI purpose, as we create
        # a fresh project anyway
        try:
            unisdk.create_contexts(list(contexts))
        except _SETUP_NETWORK_ERRORS as exc:
            _trip_orchestra_setup_breaker(exc)
            return
        PRECREATED_CONTEXTS.update(contexts)


@pytest.fixture(autouse=True)
def _set_random_seed():
    random.seed(42)
