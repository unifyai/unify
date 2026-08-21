"""
tests/conversation_manager/core/test_inactivity_lifecycle.py
=============================================================

Tests for container inactivity detection and lifecycle management.

These tests verify the critical production behavior documented in INFRA.md:
- Containers shut down after 10 minutes (600s) of *genuine* idleness
- Idle containers ping every 30 seconds to stay alive
- Cleanup is called properly on shutdown
- Jobs are marked as done in AssistantJobs

This is Phase 4.1 of the end-to-end testing roadmap.

What This File Tests:
---------------------
1. **Inactivity detection**: Does check_inactivity() trigger shutdown after timeout?
2. **Activity reset**: Does receiving events reset the inactivity timer?
3. **Unassigned pod lifetime**: Is a pod with no assistant exempt from the timer?
4. **Cleanup sequence**: Is cleanup called in the correct order on shutdown?
4b. **Busy declaration**: does every kind of in-flight work hold the pod open?
5. **Job marking**: Is assistant_jobs.mark_job_done() called for live containers?
6. **Event broker close**: Is the event broker properly closed on shutdown?

Production Context (from INFRA.md):
-----------------------------------
- Inactivity timeout: 10 minutes (600 seconds)
- Ping interval: 30 seconds, and a ping is not presence — an unassigned pod
  is kept alive by being exempt from the timer, not by its own keepalive
- Idle containers use assistant_id=None
- Live containers have a real assistant_id
- On shutdown: cleanup() → mark_job_done() → stop.set()
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def event_broker():
    """Create a fresh in-memory event broker."""
    from unify.conversation_manager.in_memory_event_broker import (
        create_in_memory_event_broker,
        reset_in_memory_event_broker,
    )

    reset_in_memory_event_broker()
    broker = create_in_memory_event_broker()
    yield broker
    await broker.aclose()
    reset_in_memory_event_broker()


@pytest.fixture(autouse=True)
def assigned_pod():
    """Default every test here to a pod that has been given an assistant.

    Production sets this on the StartupEvent, via ``SESSION_DETAILS.populate``,
    and it is what the runtime reads to tell an idle pod from a live one. The
    idle timer only applies to a live one, so a test that leaves it unset is
    exercising an unassigned pod whatever it passed to ConversationManager.
    """
    from unify.session_details import SESSION_DETAILS

    with patch.object(SESSION_DETAILS.assistant, "agent_id", 42):
        yield


@pytest.fixture
def mock_loop():
    """Create a mock event loop with controllable time."""
    mock = MagicMock()
    mock.time.return_value = 0.0
    return mock


@pytest.fixture
def minimal_cm_config():
    """Minimal configuration for creating a ConversationManager."""
    return {
        "job_name": "test-job-123",
        "user_id": "user_1",
        "assistant_id": "assistant_1",
        "user_first_name": "Test",
        "user_surname": "User",
        "assistant_first_name": "Test",
        "assistant_surname": "Assistant",
        "assistant_age": "25",
        "assistant_nationality": "American",
        "assistant_about": "A helpful assistant",
        "assistant_number": "+15555550000",
        "assistant_email": "assistant@test.com",
        "user_number": "+15555551111",
        "user_email": "user@test.com",
        "voice_provider": "cartesia",
        "voice_id": "test_voice",
    }


# =============================================================================
# Test: Inactivity Detection Basics
# =============================================================================


class TestInactivityDetectionBasics:
    """Tests for basic inactivity timeout detection."""

    @pytest.mark.asyncio
    async def test_inactivity_timeout_triggers_shutdown(self, event_broker):
        """
        Verify that check_inactivity() triggers shutdown after the timeout.

        This is the core behavior: after 6 minutes of no activity, the container
        should shut down gracefully.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        # Set very short timeouts for testing
        cm.inactivity_timeout = 0.1  # 100ms timeout
        cm.inactivity_check_interval = 0.05  # 50ms check interval

        # Set last_activity_time to a time that makes timeout already exceeded
        cm.last_activity_time = cm.loop.time() - 1.0  # 1 second ago

        # Run check_inactivity for a short time
        check_task = asyncio.create_task(cm.check_inactivity())

        # Wait for stop to be set
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail("Inactivity timeout did not trigger shutdown")
        finally:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass

        assert stop_event.is_set(), "Stop event should be set after inactivity timeout"
        assert cm.shutdown_reason == "idle_timeout"

    @pytest.mark.asyncio
    async def test_activity_resets_inactivity_timer(self, event_broker):
        """
        Verify that receiving events resets the inactivity timer.

        When wait_for_events() receives a message, it should update
        last_activity_time to the current loop time.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        initial_activity_time = cm.last_activity_time

        # Simulate activity update (what wait_for_events does when receiving an event)
        # We use direct time manipulation instead of sleeping - this makes the test
        # deterministic regardless of actual wall-clock time
        cm.last_activity_time = cm.loop.time() + 0.1

        assert (
            cm.last_activity_time > initial_activity_time
        ), "Activity time should be updated after receiving an event"

    @pytest.mark.asyncio
    async def test_no_shutdown_when_activity_continues(self, event_broker):
        """
        Verify that continuous activity prevents shutdown.

        If last_activity_time keeps getting updated, the inactivity timeout
        should never be reached.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        # Set short timeouts
        cm.inactivity_timeout = 0.2
        cm.inactivity_check_interval = 0.05

        # Start inactivity check
        check_task = asyncio.create_task(cm.check_inactivity())

        # Keep updating activity time faster than the timeout
        for _ in range(5):
            cm.last_activity_time = cm.loop.time()
            await asyncio.sleep(0.05)

        # Stop should NOT be set
        assert (
            not stop_event.is_set()
        ), "Stop should not be set while activity continues"

        check_task.cancel()
        try:
            await check_task
        except asyncio.CancelledError:
            pass


# =============================================================================
# Test: EventBus Activity Keep-Alive
# =============================================================================


class TestEventBusKeepAlive:
    """Tests that internal EventBus publishes prevent inactivity shutdown.

    When the assistant is actively working (LLM calls, tool-loop turns, manager
    methods) the EventBus fires events even though no external pubsub messages
    arrive.  check_inactivity() must treat these as activity.
    """

    @pytest.mark.asyncio
    async def test_eventbus_publish_prevents_shutdown(self, event_broker):
        """EventBus.publish() keeps the container alive even without pubsub events."""
        import time as _time

        from unify.conversation_manager.conversation_manager import ConversationManager
        from unify.events.event_bus import EventBus

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.3
        cm.inactivity_check_interval = 0.05

        # Simulate stale pubsub — no external events for a long time
        cm.last_activity_time = cm.loop.time() - 10.0

        # But the EventBus was active very recently
        EventBus.last_publish_monotonic = _time.monotonic()

        check_task = asyncio.create_task(cm.check_inactivity())

        # Keep bumping the EventBus timestamp faster than the timeout
        for _ in range(8):
            EventBus.last_publish_monotonic = _time.monotonic()
            await asyncio.sleep(0.05)

        assert (
            not stop_event.is_set()
        ), "Container should stay alive when EventBus is active"

        check_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await check_task

    @pytest.mark.asyncio
    async def test_shutdown_when_both_sources_idle(self, event_broker):
        """Container shuts down when both pubsub and EventBus are idle."""
        import time as _time

        from unify.conversation_manager.conversation_manager import ConversationManager
        from unify.events.event_bus import EventBus

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.1
        cm.inactivity_check_interval = 0.05

        # Both sources are stale
        cm.last_activity_time = cm.loop.time() - 10.0
        EventBus.last_publish_monotonic = _time.monotonic() - 10.0

        check_task = asyncio.create_task(cm.check_inactivity())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail("Inactivity timeout should have triggered shutdown")
        finally:
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert stop_event.is_set()


class TestActiveWorkKeepAlive:
    """Tests that active work prevents quiet long-running execution from idling out."""

    @pytest.mark.asyncio
    async def test_active_work_prevents_shutdown_until_work_completes(
        self,
        event_broker,
    ):
        import time as _time

        from unify.conversation_manager.conversation_manager import ConversationManager
        from unify.events.active_work import ACTIVE_WORK
        from unify.events.event_bus import EventBus

        ACTIVE_WORK.clear()

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.08
        cm.inactivity_check_interval = 0.02
        cm.last_activity_time = cm.loop.time() - 10.0
        EventBus.last_publish_monotonic = _time.monotonic() - 10.0

        active_work = ACTIVE_WORK.begin(label="test_work")
        check_task = asyncio.create_task(cm.check_inactivity())

        try:
            await asyncio.sleep(0.15)
            assert not stop_event.is_set()

            active_work.end()
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        finally:
            active_work.end()
            ACTIVE_WORK.clear()
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert stop_event.is_set()
        assert cm.shutdown_reason == "idle_timeout"


# =============================================================================
# Test: Unassigned Pods Are Not Retired By The Idle Timer
# =============================================================================


class TestUnassignedPodLifetime:
    """A pod waiting for an assistant is not retired for having no traffic.

    The pre-startup keepalive is the only thing on an unassigned pod's bus,
    and it is not presence — it says the process is up, never that anyone
    wants anything. What keeps such a pod alive is that the idle timer does
    not apply to it: its lifetime belongs to the warm pool, which deletes
    stale-image members and trims the rest to target.

    Without that exemption the whole pool retires itself one timeout after
    it is filled, and the next inbound cold-starts.
    """

    @pytest.mark.asyncio
    async def test_an_unassigned_pod_does_not_retire_itself(self, event_broker):
        from unify.conversation_manager.conversation_manager import ConversationManager
        from unify.session_details import SESSION_DETAILS

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id=None,  # Idle container
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.05
        cm.inactivity_check_interval = 0.01
        # Idle since long before the timeout — a live pod would shut down here.
        cm.last_activity_time = cm.loop.time() - 100.0

        with patch.object(SESSION_DETAILS.assistant, "agent_id", None):
            check_task = asyncio.create_task(cm.check_inactivity())
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=0.3)
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert not stop_event.is_set(), (
            "An unassigned pod retired itself on the idle timer; the warm pool "
            "empties one timeout after it is filled"
        )
        assert cm.shutdown_reason is None

    @pytest.mark.asyncio
    async def test_an_assigned_pod_still_retires_on_the_idle_timer(
        self,
        event_broker,
    ):
        """The exemption is scoped to pods with no assistant.

        A live session going quiet must still release its pod, or a deploy
        cannot reach it.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager
        from unify.session_details import SESSION_DETAILS

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.05
        cm.inactivity_check_interval = 0.01
        cm.last_activity_time = cm.loop.time() - 100.0

        with patch.object(SESSION_DETAILS.assistant, "agent_id", 42):
            check_task = asyncio.create_task(cm.check_inactivity())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pytest.fail("An assigned pod did not retire on the idle timer")
            finally:
                check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await check_task

        assert cm.shutdown_reason == "idle_timeout"


# =============================================================================
# Test: Cleanup Sequence
# =============================================================================


class TestCleanupSequence:
    """Tests for the cleanup sequence on shutdown."""

    @pytest.mark.asyncio
    async def test_cleanup_calls_mark_job_done_for_live_container(self, event_broker):
        """
        Verify that cleanup() calls mark_job_done() for live containers.

        Live containers (with real assistant_id) must mark their job as done
        in AssistantJobs so the system knows they're no longer running.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job-live",
            user_id="user_1",
            assistant_id="real_assistant_123",  # Live container
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ) as mock_mark_done:
            await cm.cleanup()

            mock_mark_done.assert_called_once_with(
                "test-job-live",
                cm.inactivity_timeout,
            )

    @pytest.mark.asyncio
    async def test_cleanup_propagates_idle_timeout_reason_to_mark_job_done(
        self,
        event_broker,
    ):
        """
        Verify cleanup preserves the idle-timeout intent when shutting down.

        A graceful inactivity shutdown should tell AssistantJobs to stop the
        session so Comms can transition offline instead of restarting.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job-live",
            user_id="user_1",
            assistant_id="real_assistant_123",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.shutdown_reason = "idle_timeout"

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ) as mock_mark_done:
            await cm.cleanup()

            mock_mark_done.assert_called_once_with(
                "test-job-live",
                cm.inactivity_timeout,
                shutdown_reason="idle_timeout",
            )

    @pytest.mark.asyncio
    async def test_cleanup_skips_mark_job_done_for_idle_container(self, event_broker):
        """
        Verify that cleanup() skips mark_job_done() for idle containers.

        Idle containers (with assistant_id=None) were never "live" in the
        AssistantJobs sense, so we don't need to mark them as done.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job-idle",
            user_id="user_1",
            assistant_id=None,  # Idle container
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ) as mock_mark_done:
            await cm.cleanup()

            mock_mark_done.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_calls_cleanup_call_proc(self, event_broker):
        """
        Verify that cleanup() calls cleanup_call_proc() to terminate voice agents.

        Any running voice agent subprocess must be terminated on shutdown.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        # Mock cleanup_call_proc
        cm.call_manager.cleanup_call_proc = AsyncMock()

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ):
            await cm.cleanup()

            cm.call_manager.cleanup_call_proc.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_sets_stop_event(self, event_broker):
        """
        Verify that cleanup() sets the stop event.

        This signals to main.py that shutdown is complete.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        assert not stop_event.is_set(), "Stop event should not be set initially"

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ):
            await cm.cleanup()

        assert stop_event.is_set(), "Stop event should be set after cleanup"


# =============================================================================
# Test: Event Broker Lifecycle
# =============================================================================


class TestEventBrokerLifecycle:
    """Tests for event broker lifecycle management."""

    @pytest.mark.asyncio
    async def test_inactivity_closes_event_broker(self, event_broker):
        """
        Verify that inactivity timeout closes the event broker.

        When check_inactivity() triggers shutdown, it should close the event
        broker to release resources and signal to wait_for_events() to stop.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        # Set very short timeout
        cm.inactivity_timeout = 0.05
        cm.inactivity_check_interval = 0.02
        cm.last_activity_time = cm.loop.time() - 1.0  # Already timed out

        # Track if aclose was called
        original_aclose = event_broker.aclose
        aclose_called = False

        async def mock_aclose():
            nonlocal aclose_called
            aclose_called = True
            return await original_aclose()

        event_broker.aclose = mock_aclose

        # Run check_inactivity
        check_task = asyncio.create_task(cm.check_inactivity())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
        finally:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass

        assert aclose_called, "Event broker should be closed on inactivity timeout"


# =============================================================================
# Test: Full Lifecycle Integration
# =============================================================================


class TestFullLifecycleIntegration:
    """Integration tests for complete lifecycle scenarios."""

    @pytest.mark.asyncio
    async def test_idle_to_live_to_shutdown_lifecycle(self, event_broker):
        """
        Test the complete lifecycle: idle → startup → live → inactivity → shutdown.

        This simulates the production flow:
        1. Container starts in idle state
        2. Receives startup event, becomes live
        3. Processes events, stays alive
        4. No activity for timeout period
        5. Shuts down gracefully
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-lifecycle-job",
            user_id="user_1",
            assistant_id=None,  # Start as idle
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        # Verify idle state
        assert cm.assistant_id is None

        # Simulate startup (transition to live)
        startup_payload = {
            "api_key": "test_key",
            "assistant_id": 456,
            "user_id": "user_1",
            "assistant_first_name": "Live",
            "assistant_surname": "Assistant",
            "assistant_age": "30",
            "assistant_nationality": "British",
            "assistant_about": "A live assistant",
            "assistant_number": "+15555559999",
            "assistant_email": "live@test.com",
            "user_first_name": "Live",
            "user_surname": "User",
            "user_number": "+15555558888",
            "user_email": "live_user@test.com",
            "voice_provider": "cartesia",
            "voice_id": "voice_123",
            "self_contact_id": 42,
            "boss_contact_id": 43,
        }
        cm.set_details(startup_payload)

        # Verify live state
        assert cm.assistant_id == 456

        # Set short timeout for testing
        cm.inactivity_timeout = 0.1
        cm.inactivity_check_interval = 0.03

        # Set activity time to simulate recent activity, then force timeout
        # We use direct time manipulation instead of sleeping
        cm.last_activity_time = cm.loop.time() - 1.0  # Force timeout

        # Run check_inactivity
        check_task = asyncio.create_task(cm.check_inactivity())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail("Lifecycle did not complete - shutdown not triggered")
        finally:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass

        assert stop_event.is_set(), "Container should shut down after inactivity"

    @pytest.mark.asyncio
    async def test_cleanup_order_is_correct(self, event_broker):
        """
        Verify cleanup operations happen in the correct order.

        The order matters for production:
        1. Update rolling summaries (persist conversation state)
        2. Store chat history
        3. Cleanup call proc
        4. Mark job done
        5. Set stop event
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-cleanup-order",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        call_order = []

        # Mock all the cleanup functions to track order
        cm.call_manager.cleanup_call_proc = AsyncMock(
            side_effect=lambda: call_order.append("cleanup_call_proc"),
        )

        async def mock_store_chat_history():
            call_order.append("store_chat_history")

        cm.store_chat_history = mock_store_chat_history

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
            side_effect=lambda *args: call_order.append("mark_job_done"),
        ):
            await cm.cleanup()

        # Verify order
        expected_order = [
            "store_chat_history",
            "cleanup_call_proc",
            "mark_job_done",
        ]
        assert (
            call_order == expected_order
        ), f"Cleanup order incorrect. Expected {expected_order}, got {call_order}"

        # Stop event should be set at the end
        assert stop_event.is_set()


# =============================================================================
# Test: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_cleanup_handles_missing_job_name(self, event_broker):
        """
        Verify cleanup handles missing job_name gracefully.

        In some error scenarios, job_name might not be set.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="",  # Empty job name
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        with patch(
            "unify.conversation_manager.conversation_manager.assistant_jobs.mark_job_done",
        ) as mock_mark_done:
            # Should not raise
            await cm.cleanup()

            # Should not call mark_job_done with empty job name
            mock_mark_done.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_inactivity_checks_dont_double_shutdown(self, event_broker):
        """
        Verify that multiple inactivity checks don't cause issues.

        Once stop is set, subsequent checks should be no-ops.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )

        cm.inactivity_timeout = 0.05
        cm.inactivity_check_interval = 0.02
        cm.last_activity_time = cm.loop.time() - 1.0

        aclose_count = 0
        original_aclose = event_broker.aclose

        async def counting_aclose():
            nonlocal aclose_count
            aclose_count += 1
            # Only actually close on first call
            if aclose_count == 1:
                return await original_aclose()

        event_broker.aclose = counting_aclose

        # Start check_inactivity
        check_task = asyncio.create_task(cm.check_inactivity())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            # Let it run a bit more to see if it tries to close again
            await asyncio.sleep(0.1)
        except asyncio.TimeoutError:
            pass
        finally:
            check_task.cancel()
            try:
                await check_task
            except asyncio.CancelledError:
                pass

        # aclose should only be called once
        assert aclose_count == 1, f"aclose called {aclose_count} times, expected 1"


# =============================================================================
# Test: Control-plane drain
# =============================================================================


class TestDrainShutdown:
    """A drain armed by the control plane must actually end the process."""

    def _cm(self, event_broker, stop_event):
        from unify.conversation_manager.conversation_manager import ConversationManager

        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.inactivity_check_interval = 0.05
        # Nowhere near the idle timeout: a drain must not have to wait for it.
        cm.inactivity_timeout = 600
        cm.last_activity_time = cm.loop.time()
        return cm

    @pytest.mark.asyncio
    async def test_armed_drain_shuts_down_while_the_pod_is_far_from_idle(
        self,
        event_broker,
    ):
        """The drain branch must end the process, not merely announce it.

        It used to call ``await self.stop()`` -- ``stop`` is an Event, so that
        raised TypeError into an ``except Exception`` guarding the probe. The
        pod logged "shutting down for restart" every 30s for as long as it
        lived and never shut down; drains only completed when the control
        plane force-stopped the session at its deadline.
        """
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)

        with patch(
            "unify.runtime.drain_gate.is_admission_blocked",
            return_value=True,
        ):
            check_task = asyncio.create_task(cm.check_inactivity())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Armed drain did not shut the conversation manager down")
            finally:
                check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await check_task

        assert cm.shutdown_reason == "drain_restart"

    @pytest.mark.asyncio
    async def test_a_failing_drain_probe_leaves_the_pod_running(self, event_broker):
        """The probe is best-effort; a control-plane blip must not kill a pod.

        This is the shield the shutdown call was wrongly sharing: it belongs
        around the probe alone.
        """
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)

        with patch(
            "unify.runtime.drain_gate.is_admission_blocked",
            side_effect=RuntimeError("comms unreachable"),
        ):
            check_task = asyncio.create_task(cm.check_inactivity())
            await asyncio.sleep(0.2)
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert not stop_event.is_set()
        assert cm.shutdown_reason is None


# =============================================================================
# Test: Unserviceable pod retirement
# =============================================================================


class TestUnserviceableRetirement:
    """A pod that cannot serve must not keep the assistant's session."""

    @pytest.mark.asyncio
    async def test_a_pod_that_cannot_serve_retires_without_waiting_to_be_idle(
        self,
        event_broker,
    ):
        """Being talked to is not evidence a pod can answer.

        A failed manager init leaves no actor for the rest of the pod's life,
        and every wake it then receives fails. Because presence heartbeats
        count as activity, its idle clock never ran down: one such pod held a
        live assistant for three hours, swallowing two scheduled runs and a
        user message, while the fix sat in an image it could not reach.
        """
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.inactivity_check_interval = 0.05
        cm.inactivity_timeout = 600
        # Freshly "active": the idle path would never fire here.
        cm.last_activity_time = cm.loop.time()
        cm.unserviceable_reason = "manager initialization failed: no LLM credential"

        check_task = asyncio.create_task(cm.check_inactivity())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Unserviceable pod did not retire")
        finally:
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert cm.shutdown_reason == "unserviceable"

    @pytest.mark.asyncio
    async def test_a_serviceable_pod_is_left_alone(self, event_broker):
        """The gate is the recorded reason, not merely being early in startup."""
        from unify.conversation_manager.conversation_manager import ConversationManager

        stop_event = asyncio.Event()
        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.inactivity_check_interval = 0.05
        cm.inactivity_timeout = 600
        cm.last_activity_time = cm.loop.time()

        check_task = asyncio.create_task(cm.check_inactivity())
        await asyncio.sleep(0.2)
        check_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await check_task

        assert not stop_event.is_set()
        assert cm.shutdown_reason is None


class TestBusyDeclarationHoldsThePodOpen:
    """Idle must mean idle.

    Both clocks the check reads are traffic proxies: pubsub says somebody sent
    us something, the EventBus says we published something. Neither knows
    whether work is happening *now*, so work that outlives the call that
    started it -- or that runs outside this process -- has to declare itself or
    it gets torn down mid-flight. One test per kind of work that does.
    """

    def _cm(self, event_broker, stop_event):
        from unify.conversation_manager.conversation_manager import ConversationManager

        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.inactivity_check_interval = 0.05
        cm.inactivity_timeout = 0.1
        # Both clocks stale: without a declaration this pod retires at once.
        cm.last_activity_time = cm.loop.time() - 100.0
        return cm

    async def _survives(self, cm, stop_event) -> bool:
        """Run the checker briefly; True if the pod was still alive after."""
        check_task = asyncio.create_task(cm.check_inactivity())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.6)
            return False
        except asyncio.TimeoutError:
            return True
        finally:
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

    @pytest.mark.asyncio
    async def test_a_stale_pod_with_nothing_in_flight_does_retire(
        self,
        event_broker,
    ):
        """The control: every case below must differ from this one."""
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        from unify.events.event_bus import EventBus

        EventBus.last_publish_monotonic = time.monotonic() - 100.0

        assert not await self._survives(cm, stop_event)
        assert cm.shutdown_reason == "idle_timeout"

    # Each voice surface, driven through the state its read-only predicate
    # actually reads. `meet_joining` is the case the per-channel properties
    # miss: `_call_channel` is unset until the room is joined.
    VOICE_SURFACES = {
        "phone_or_whatsapp_call": lambda mgr: setattr(mgr, "_active_job", True),
        "google_meet": lambda mgr: (
            setattr(mgr, "_meet_session_id", "sess-1"),
            setattr(mgr, "_call_channel", "google_meet"),
        ),
        "teams_meet": lambda mgr: (
            setattr(mgr, "_meet_session_id", "sess-1"),
            setattr(mgr, "_call_channel", "teams_meet"),
        ),
        "meet_joining": lambda mgr: setattr(mgr, "_meet_joining", True),
        "whatsapp_joining": lambda mgr: setattr(mgr, "_whatsapp_call_joining", True),
    }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("surface", sorted(VOICE_SURFACES))
    async def test_a_quiet_call_is_not_an_idle_pod(self, event_broker, surface):
        """Voice runs in a separate process.

        Its LLM calls advance *that* process's EventBus, and the parent only
        ever sees per-turn IPC events -- so a connected-but-silent call looks
        exactly like an empty pod. Retiring here runs ``cleanup_call_proc``,
        which hangs up on whoever is on the line.
        """
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        self.VOICE_SURFACES[surface](cm.call_manager)

        assert await self._survives(cm, stop_event)
        assert cm.shutdown_reason is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "attr",
        ["assistant_screen_share_active", "user_screen_share_active"],
    )
    async def test_somebody_watching_a_screen_is_not_an_idle_pod(
        self,
        event_broker,
        attr,
    ):
        """Watching generates no traffic on either clock."""
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        setattr(cm, attr, True)

        assert await self._survives(cm, stop_event)

    @pytest.mark.asyncio
    async def test_an_in_flight_turn_is_not_an_idle_pod(self, event_broker):
        """The EventBus stamps on *completion*, so one long call is a blind spot."""
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)

        async def _thinking():
            await asyncio.sleep(30)

        turn = asyncio.create_task(_thinking())
        cm.debouncer.running_task = turn
        try:
            assert await self._survives(cm, stop_event)
        finally:
            turn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await turn

    @pytest.mark.asyncio
    async def test_a_parked_persist_session_does_not_hold_the_pod(
        self,
        event_broker,
    ):
        """A registered handle is not work in progress.

        `act(persist=True)` parks in `in_flight_actions` waiting for an
        interjection that may never arrive, so presence in that registry says
        "this handle is steerable", never "something is running". Reading it as
        liveness made every assistant that used persist mode immortal: one
        staging pod held a parked handle for 99 minutes while its EventBus went
        96 minutes without a single publish.

        The registry predates the idle check by two months and was never
        consulted by it at either the 420s or 3600s timeout -- a parked session
        has always been discarded on retirement, and the transcript is what
        carries the conversation into the next pod.
        """
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        from unify.events.event_bus import EventBus

        EventBus.last_publish_monotonic = time.monotonic() - 100.0
        cm.in_flight_actions = {8091: {"query": "monitor the ingestion"}}

        assert not await self._survives(cm, stop_event)
        assert cm.shutdown_reason == "idle_timeout"

    @pytest.mark.asyncio
    async def test_pool_work_outliving_its_caller_is_not_an_idle_pod(
        self,
        event_broker,
    ):
        """Ingestion hands work to a pool thread and returns a handle at once.

        The plan's own ACTIVE_WORK record ends with the plan, so without the
        manager's own declaration the write is invisible to both clocks.
        """
        from unify.ingestion_manager.ingestion_manager import IngestionManager

        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)

        with IngestionManager._pod_work("ingestion_inline", "run-1"):
            assert await self._survives(cm, stop_event)

        assert not await self._survives(cm, stop_event)

    @pytest.mark.asyncio
    async def test_the_ghost_branch_respects_the_same_predicate(self, event_broker):
        """The ghost path used to be a second way in past every floor.

        It is the only branch that can retire a pod whose EventBus clock is
        fresh, so gating it on active work alone meant a declaration the
        ordinary path honoured did not bind it.
        """
        from unify.events.event_bus import EventBus

        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        # Exactly the ghost shape: pubsub long stale, EventBus kept fresh.
        cm.call_manager._active_job = True

        async def _keep_eventbus_fresh():
            while True:
                EventBus.last_publish_monotonic = time.monotonic()
                await asyncio.sleep(0.01)

        pump = asyncio.create_task(_keep_eventbus_fresh())
        try:
            assert await self._survives(cm, stop_event)
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump

    @pytest.mark.asyncio
    async def test_a_drain_waits_for_a_live_call_then_ends_the_pod(
        self,
        event_broker,
    ):
        """A drain is graceful in the pod and forced from outside.

        The in-pod branch exists so a draining pod retires itself once nothing
        is in flight; hanging up a live call to pick up a new image is not a
        trade the pod gets to make. If the call never ends, the control plane
        stops the session at its own deadline -- that path does not come
        through here.
        """
        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        cm.call_manager._active_job = True

        with patch(
            "unify.runtime.drain_gate.is_admission_blocked",
            return_value=True,
        ):
            assert await self._survives(cm, stop_event)
            assert cm.shutdown_reason is None

            # Call over: the same armed drain now retires the pod.
            cm.call_manager._active_job = False
            assert not await self._survives(cm, stop_event)

        assert cm.shutdown_reason == "drain_restart"


class TestManagersDeclareTheirOwnPoolWork:
    """The declarations that live in the managers rather than the CM.

    Each wraps work that runs on a thread or task nobody awaits, makes no LLM
    calls, and writes through DataManager -- which publishes nothing. So none of
    them move either idle clock, and the wrapper is the only thing standing
    between them and an inactivity shutdown landing mid-flight.
    """

    @pytest.mark.asyncio
    async def test_an_integration_tools_sync_declares_itself(self):
        from unify.events.active_work import ACTIVE_WORK
        from unify.integrations.sync_state import IntegrationSyncCoordinator

        coordinator = IntegrationSyncCoordinator()
        seen = {}

        async def _fake_sync(_self, app_slug, **kwargs):
            snapshot = ACTIVE_WORK.snapshot()
            seen["count"] = snapshot.active_count
            seen["labels"] = [w["label"] for w in snapshot.works]
            return None

        with patch.object(
            IntegrationSyncCoordinator,
            "_sync_app",
            new=_fake_sync,
        ):
            await coordinator.sync_app("gmail")

        assert seen["count"] == 1
        assert seen["labels"] == ["integration_tools_sync"]
        assert ACTIVE_WORK.snapshot().active_count == 0

    @pytest.mark.asyncio
    async def test_a_file_sync_transfer_declares_itself(self):
        """The transfer, not the 30s poll loop that schedules them.

        Declaring the loop would mean a desktop-attached pod never retires;
        declaring the transfer means a shutdown cannot stop one half-done, and
        the desktop's writes are what the next session reads.
        """
        from unify.events.active_work import ACTIVE_WORK
        from unify.file_manager.sync.rclone import RcloneSync

        sync = RcloneSync.__new__(RcloneSync)
        seen = {}

        async def _fake_inner(cmd, operation, max_retries=None):
            snapshot = ACTIVE_WORK.snapshot()
            seen["count"] = snapshot.active_count
            seen["labels"] = [w["label"] for w in snapshot.works]
            return None

        with patch.object(
            RcloneSync,
            "_run_with_retry_inner",
            new=staticmethod(_fake_inner),
        ):
            await sync._run_with_retry(["rclone", "bisync"], "bisync")

        assert seen["count"] == 1
        assert seen["labels"] == ["file_sync_transfer"]
        assert ACTIVE_WORK.snapshot().active_count == 0


class TestTheBusyReasonNamesWhatIsHoldingThePod:
    """The reason string is a debugging contract, not decoration.

    It is what the idle log carries as ``busy=``, and it is the only thing that
    turns "a declaration is holding this pod" into "*which* subsystem declared
    it". The count alone was not enough: the first deployed pod reported
    ``active_work(1)`` and nothing in the log could say whether that was a plan
    step, an ingestion run or a file transfer.
    """

    def _cm(self, event_broker):
        from unify.conversation_manager.conversation_manager import ConversationManager

        return ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=asyncio.Event(),
        )

    @pytest.mark.asyncio
    async def test_active_work_is_reported_by_label(self, event_broker):
        from unify.events.active_work import ACTIVE_WORK

        cm = self._cm(event_broker)
        handle = ACTIVE_WORK.begin(label="ingestion_dispatch_upload")
        try:
            assert cm._busy_snapshot() == (
                True,
                "active_work(1:ingestion_dispatch_upload)",
            )
        finally:
            handle.end()

    @pytest.mark.asyncio
    async def test_concurrent_records_collapse_by_label(self, event_broker):
        """Distinct labels, not one entry per record.

        A run that fans out over chunks would otherwise turn one fact into a
        log line that scrolls.
        """
        from unify.events.active_work import ACTIVE_WORK

        cm = self._cm(event_broker)
        handles = [
            ACTIVE_WORK.begin(label="ingestion_inline"),
            ACTIVE_WORK.begin(label="ingestion_inline"),
            ACTIVE_WORK.begin(label="file_sync_transfer"),
        ]
        try:
            # Count is every record; labels are the distinct set, sorted so the
            # string is stable to compare across checks.
            assert cm._busy_snapshot() == (
                True,
                "active_work(3:file_sync_transfer,ingestion_inline)",
            )
        finally:
            for handle in handles:
                handle.end()


class TestStaleDeclarationIsReportedNotActedOn:
    """A declaration standing while both clocks are stale is reported only.

    Real work moves at least one clock, so a declaration that outlives both is
    the shape of one that has gone stale. It is not proof: a call silent for
    twenty minutes looks identical to a call flag nobody cleared, and the only
    difference between them is whether somebody is still holding. Retiring on
    this signal would hang up on them, so the pod says what it sees and keeps
    serving; the 12h stale-runtime sweep is the absolute backstop.
    """

    def _cm(self, event_broker, stop_event):
        from unify.conversation_manager.conversation_manager import ConversationManager

        cm = ConversationManager(
            event_broker=event_broker,
            job_name="test-job",
            user_id="user_1",
            assistant_id="assistant_1",
            user_first_name="Test",
            user_surname="User",
            assistant_first_name="Test",
            assistant_surname="Assistant",
            assistant_age="25",
            assistant_nationality="American",
            assistant_about="Test bio",
            assistant_number="+15555550000",
            assistant_email="assistant@test.com",
            user_number="+15555551111",
            user_email="user@test.com",
            stop=stop_event,
        )
        cm.inactivity_check_interval = 0.05
        cm.inactivity_timeout = 0.1
        cm.last_activity_time = cm.loop.time() - 100.0
        return cm

    @pytest.mark.asyncio
    async def test_a_long_silent_call_is_reported_and_kept_alive(
        self,
        event_broker,
    ):
        from unify.events.event_bus import EventBus

        stop_event = asyncio.Event()
        cm = self._cm(event_broker, stop_event)
        # Both clocks far past the timeout, with a call still declared.
        EventBus.last_publish_monotonic = time.monotonic() - 100.0
        cm.call_manager._active_job = True

        logged = []
        cm._session_logger.info = lambda tag, msg, *a, **k: logged.append(msg)

        check_task = asyncio.create_task(cm.check_inactivity())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.6)
            pytest.fail("A declared call must not be retired on a suspicion")
        except asyncio.TimeoutError:
            pass
        finally:
            check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await check_task

        assert cm.shutdown_reason is None
        assert any("Declaration suspect: busy=active_call" in m for m in logged), logged
