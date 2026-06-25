"""Fixtures for real-CM core user-flow tests."""

from __future__ import annotations

import os


def _is_local_orchestra_url(url: str) -> bool:
    return not url or "127.0.0.1" in url or "localhost" in url


# Local Orchestra uses a fixed test API key and in-process object storage.
# parallel_run.sh and UnityTests .env often omit both; without them log writes
# fail before flow fixtures run.
if _is_local_orchestra_url(os.environ.get("ORCHESTRA_URL", "")):
    os.environ.setdefault("UNIFY_KEY", "local-test-api-key")
    os.environ.setdefault("SELF_HOST", "1")

# The CM-backed flow sessions (flow_session) run unassigned (agent_id unset)
# BEFORE importing the harness (which imports unity.conversation_manager.main
# -> load_dotenv()). Keeping agent_id unset means the local scheduler, Orchestra
# contact-membership sync, and offline runs (all keyed by agent_id, not by the
# per-test context path) cannot cross-fire between parallel sessions sharing one
# local Orchestra, so those conversation flows stay isolated and fast. The
# task-execution flow (test_task_schedule.py) instead binds the real coordinator
# and the shared "Assistants" project that Orchestra projection requires, and
# restores these globals on teardown.
#
# UnityTests .env often pins a hosted USER_ID / ASSISTANT_ID; those paths do
# not exist in the local Orchestra DB and break real ContactManager writes.
os.environ["ASSISTANT_ID"] = ""
os.environ["USER_ID"] = "default"
# Voice/meet turns are exercised event-driven (no LiveKit audio transport); the
# live-voice path would raise without a LiveKit server, so keep it unset.
os.environ["LIVEKIT_URL"] = ""
os.environ.setdefault("UNITY_CONVERSATION_JOB_NAME", "flow_test_job")
os.environ.setdefault("UNITY_INACTIVITY_TIMEOUT_SECONDS", "0")
# A realistic boss/owner profile rather than placeholder stubs, so contact- and
# transcript-grounded flows read like a real user's data.
os.environ.setdefault("USER_FIRST_NAME", "Alex")
os.environ.setdefault("USER_SURNAME", "Rivera")
os.environ.setdefault("USER_EMAIL", "alex.rivera@example.com")
os.environ.setdefault("USER_NUMBER", "+14155550142")
os.environ.setdefault("TEST", "true")
os.environ.setdefault("UNITY_INCREMENTING_TIMESTAMPS", "true")
os.environ.setdefault("UNIFY_PRETEST_CONTEXT_CREATE", "true")
os.environ.setdefault("UNITY_MEMORY_ENABLED", "false")
os.environ.setdefault("UNITY_GUIDANCE_ENABLED", "false")
# The state-manager combo flow exercises secrets.* end-to-end.
os.environ.setdefault("UNITY_SECRET_ENABLED", "true")
os.environ.setdefault("UNITY_SKILL_ENABLED", "false")
os.environ.setdefault("UNITY_WEB_ENABLED", "false")
# Flow tests run against local Orchestra only; hosted Comms URLs in .env would
# disable the in-process task scheduler and route outbound comms elsewhere.
os.environ["UNITY_COMMS_URL"] = ""
os.environ["UNITY_LOCAL_SCHEDULER"] = "true"
os.environ["UNITY_KNOWLEDGE_ENABLED"] = "true"
os.environ["UNITY_FILE_ENABLED"] = "true"
# Route inbound through the real CommsManager + in-memory ingress transport and
# capture outbound on the in-memory outbound transport, so flow turns exercise
# the same envelope -> dispatch_inbound_envelope normalization as production
# without standing up Pub/Sub. See unity/gateway/factory.py.
os.environ.setdefault("UNITY_CONVERSATION_INGRESS_TRANSPORT", "inmemory")
os.environ.setdefault("UNITY_CONVERSATION_OUTBOUND_TRANSPORT", "inmemory")
# Parallel flow sessions must not share cached LLM completions; otherwise
# unrelated tests' prompts/replies bleed into one another under parallel_run.
# Caching stays off at the merge gate (a cache hit would skip the live brain we
# are trying to smoke-test). For local-dev speed it can be enabled per process
# by exporting UNILLM_CACHE + a unique UNILLM_CACHE_DIR before pytest starts.
if not os.environ.get("UNILLM_CACHE_DIR"):
    os.environ["UNILLM_CACHE"] = "false"

import contextlib
import hashlib
import re

import pytest
import pytest_asyncio
from unillm.cost_tracker import capture_costs

from unity.session_details import UNASSIGNED_ASSISTANT_CONTEXT, UNASSIGNED_USER_CONTEXT
from tests.flows.harness import FlowHarness, build_flow_harness
from tests.helpers import scenario_file_lock
from tests.settings import SETTINGS

_FLOW_MARKERS = (
    pytest.mark.flow,
    pytest.mark.eval,
    pytest.mark.llm_call,
    pytest.mark.requires_orchestra,
    pytest.mark.integration,
    pytest.mark.no_unify_context,
    # Flow turns assert that the brain reached results through the real
    # primitives.* surface by observing the ManagerMethod / DesktopPrimitiveInvoked
    # telemetry the product publishes. That telemetry is the live Console action
    # stream (EVENTBUS_PUBLISHING_ENABLED=true in production); enabling it here
    # keeps the harness production-faithful and is restored after each test by the
    # autouse _enable_eventbus_for_marked_tests fixture in tests/conftest.py.
    pytest.mark.enable_eventbus,
)


def _normalize_pytest_nodeid(nodeid: str) -> str | None:
    bracket_match = re.search(r"\[([^\]]+)\]", nodeid)
    bracket_content = bracket_match.group(1) if bracket_match else ""
    normalized = re.sub(r"[^a-zA-Z0-9]", "", bracket_content)
    if not normalized:
        return None
    return normalized[:24]


def _derive_flow_context(request: pytest.FixtureRequest) -> str:
    """Return an isolated Orchestra context path for one flow test."""

    item = request.node
    file_path = str(getattr(item, "fspath", "") or "")
    if f"{os.sep}tests{os.sep}" in file_path:
        rel_path = file_path.split(f"{os.sep}tests{os.sep}", 1)[1].replace(os.sep, "/")
        if rel_path.endswith(".py"):
            rel_path = rel_path[:-3]
        test_path = f"tests/{rel_path}"
    else:
        test_path = "tests/flows/unknown"

    func_name = getattr(item, "originalname", None) or getattr(item, "name", "test")
    nodeid = getattr(item, "nodeid", "")
    if "[" in nodeid:
        normalized = _normalize_pytest_nodeid(nodeid)
        if normalized is None:
            normalized = hashlib.md5(nodeid.encode("utf-8")).hexdigest()[:8]
        func_name = f"{func_name}/{normalized}"

    return (
        f"{test_path}/{func_name}/"
        f"{UNASSIGNED_USER_CONTEXT}/{UNASSIGNED_ASSISTANT_CONTEXT}"
    )


def pytest_collection_modifyitems(config, items) -> None:
    """Apply flow-lane markers to every test collected under tests/flows/."""

    for item in items:
        if "/tests/flows/" not in str(item.fspath).replace("\\", "/"):
            continue
        for marker in _FLOW_MARKERS:
            item.add_marker(marker)


@pytest_asyncio.fixture(autouse=True)
async def _reset_litellm_logging_worker_per_test():
    """Rebind LiteLLM's logging worker to the current pytest-asyncio loop."""

    try:
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER

        try:
            await GLOBAL_LOGGING_WORKER.stop()
        except Exception:
            pass
        try:
            GLOBAL_LOGGING_WORKER._worker_task = None
            GLOBAL_LOGGING_WORKER._running_tasks.clear()
            GLOBAL_LOGGING_WORKER._queue = None
            GLOBAL_LOGGING_WORKER._sem = None
        except Exception:
            pass
    except Exception:
        pass

    yield


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_call(item):
    """Attribute the brain's LLM cost to the flow test that drove it.

    The shared cost meter (tests/conftest.py) records ``provider_cost`` via a
    ``capture_costs()`` sink opened around the call phase, on the test-body
    task. Flow tests drive the brain from the ConversationManager's
    ``operations_listener_task``, which is spawned during fixture setup and so
    inherits a context predating that sink -- its cost events are dropped,
    reporting $0. The ``flow_harness`` fixture instead opens its own
    ``capture_costs()`` before spawning that task and parks the live events on
    the node; here (outermost wrapper, so this runs after the shared wrapper
    has set its empty call-phase list) we promote those events into the slot
    the shared meter reads, so per-test cost, the session total, and the
    parallel_run cost file reflect real spend without double counting.
    """

    yield
    flow_cost_events = getattr(item, "_flow_cost_events", None)
    if flow_cost_events is not None:
        item._unillm_cost_events = flow_cost_events


@pytest_asyncio.fixture
async def flow_harness(request: pytest.FixtureRequest) -> FlowHarness:
    """Function-scoped real CM: CodeAct tasks must share the test event loop."""

    context_path = _derive_flow_context(request)

    # parallel_run.sh runs one flow test per process; the file lock only serializes
    # sequential pytest workers that share in-process CM globals.
    serialize = (
        contextlib.nullcontext()
        if os.environ.get("UNITY_TMUX_SESSION_ID")
        else scenario_file_lock("flow_harness_cm")
    )

    # Open the cost sink before build_flow_harness spawns the operations
    # listener task, so that task (where the brain's LLM calls actually run)
    # inherits it. pytest_runtest_call above promotes these events into the
    # shared cost meter once the turn completes.
    with capture_costs() as cost_events:
        request.node._flow_cost_events = cost_events
        with serialize:
            harness = await build_flow_harness(
                project_name=SETTINGS.test_project_name,
                context_path=context_path,
            )
            try:
                yield harness
            finally:
                await harness.shutdown()


@pytest_asyncio.fixture
async def flow_session(flow_harness: FlowHarness) -> FlowHarness:
    """Alias kept for readability in flow test modules."""

    yield flow_harness
