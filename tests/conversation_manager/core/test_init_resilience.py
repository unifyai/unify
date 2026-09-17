"""
tests/conversation_manager/core/test_init_resilience.py
=======================================================

Symbolic tests verifying that failures in non-essential initialization steps
degrade gracefully instead of preventing the session from becoming operational.

Each test mocks a single degradable subsystem to raise during init, then
asserts that ``cm.initialized`` still becomes ``True`` — proving the session
would survive and serve requests (with reduced capability) rather than
becoming a zombie.
"""

import pytest
import pytest_asyncio
from unittest.mock import patch, MagicMock

from tests.helpers import scenario_file_lock

# ---------------------------------------------------------------------------
# Fixture: lightweight CM factory for resilience tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def resilience_cm():
    """Create a ConversationManager, run init_conv_manager, and tear down.

    Yields the CM *before* init so individual tests can apply mocks around
    the ``init_conv_manager`` call.
    """
    from unify.conversation_manager.event_broker import reset_event_broker
    from unify.conversation_manager import start_async, stop_async

    reset_event_broker()

    cm = await start_async(project_name="TestInitResilience")

    yield cm

    await stop_async()
    reset_event_broker()


def _simulated_actor():
    from unify.actor.simulated import SimulatedActor

    return SimulatedActor(
        steps=None,
        duration=None,
        log_mode="log",
        emit_notifications=False,
    )


async def _init(cm, lock_name="init_resilience", actor=None):
    """Helper: run manager init with a SimulatedActor under file lock."""
    from unify.conversation_manager.domains import managers_utils

    if actor is None:
        actor = _simulated_actor()
    with scenario_file_lock(lock_name):
        await managers_utils.init_conv_manager(cm, actor=actor)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDegradableStepResilience:
    """Failures in optional init steps must not prevent initialization."""

    @pytest.mark.asyncio
    async def test_guidance_manager_init_failure(self, resilience_cm):
        cm = resilience_cm
        # The simulated actor resolves its own guidance manager, so it is
        # built before the registry is made to fail.
        actor = _simulated_actor()
        with patch(
            "unify.conversation_manager.domains.managers_utils.ManagerRegistry.get_guidance_manager",
            side_effect=ConnectionError("store unreachable"),
        ):
            await _init(cm, "resilience_guidance", actor=actor)

        assert cm.initialized is True

    @pytest.mark.asyncio
    async def test_function_manager_warmup_failure(self, resilience_cm):
        cm = resilience_cm
        with patch(
            "unify.conversation_manager.domains.managers_utils.ManagerRegistry.get_function_manager",
        ) as mock_fm:
            mock_instance = MagicMock()
            mock_instance.warm_embeddings.side_effect = RuntimeError("warm error")
            mock_fm.return_value = mock_instance
            await _init(cm, "resilience_prim")

        assert cm.initialized is True

    @pytest.mark.asyncio
    async def test_embedding_warmup_failure(self, resilience_cm):
        cm = resilience_cm
        with patch(
            "unify.conversation_manager.domains.managers_utils.ManagerRegistry.warm_all_embeddings",
            side_effect=ConnectionError("store unreachable"),
        ):
            await _init(cm, "resilience_embed")

        assert cm.initialized is True


class TestContextRegistryResilience:
    """Individual context creation failures must not crash setup()."""

    def test_partial_context_creation_failure_does_not_raise(self):
        """ContextRegistry.setup() tolerates individual context creation errors."""
        from unify.common.context_registry import ContextRegistry

        original = ContextRegistry._create_context_wrapper

        call_count = 0

        @classmethod
        def _flaky(cls, manager_name, entry):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("transient network failure")
            return original.__func__(cls, manager_name, entry)

        ContextRegistry._setup_complete = False
        try:
            with patch.object(ContextRegistry, "_create_context_wrapper", _flaky):
                ContextRegistry.setup()
        finally:
            ContextRegistry._setup_complete = False

        assert call_count > 1, "Mock was not exercised"
