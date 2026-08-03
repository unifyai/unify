"""The bridge from a viewer's press to a run.

Three separate files have to agree for a canvas action to work at all: the event
must parse, it must be routed by its system-event type, and it must reach a
handler. Missing any one produces no error — the invocation simply stays pending
forever while the viewer watches a control that says "working". That silence is
what these tests exist to break.
"""

from __future__ import annotations

import pytest

from unify.conversation_manager.domains.canvas_actions import (
    handle_canvas_invocation_requested,
)
from unify.conversation_manager.events import CanvasInvocationRequested


class _Record:
    def __init__(self, status: str = "succeeded", error: str | None = None):
        self.status = status
        self.error = error
        self.action_name = "bulk_send"


class _Canvas:
    """Stands in for CanvasManager, recording how it was called."""

    def __init__(self, record: _Record | None = None, raises: Exception | None = None):
        self.record = record or _Record()
        self.raises = raises
        self.calls: list[tuple[int, str]] = []

    def run_invocation(self, invocation_id: int, *, token: str):
        self.calls.append((invocation_id, token))
        if self.raises:
            raise self.raises
        return self.record


@pytest.fixture()
def stub_canvas(monkeypatch):
    """Swap the registry's canvas manager for a stub."""

    def install(canvas: _Canvas) -> _Canvas:
        from unify.manager_registry import ManagerRegistry

        monkeypatch.setattr(
            ManagerRegistry,
            "get_canvas_manager",
            classmethod(lambda cls: canvas),
        )
        return canvas

    return install


@pytest.fixture()
def published(monkeypatch):
    """Capture what the handler reports back, instead of publishing it."""
    from unify.conversation_manager.domains import comms_utils

    captured: list[dict] = []
    monkeypatch.setattr(
        comms_utils,
        "publish_canvas_invocation",
        lambda **kwargs: captured.append(kwargs),
    )
    return captured


class TestEventParsing:
    def test_both_identifiers_are_required(self):
        # A run cannot be addressed by either alone: invocation ids are sequential
        # per canvas, so a token-less id would address the wrong canvas's run.
        assert CanvasInvocationRequested.from_dict({"invocation_id": 1}) is None
        assert CanvasInvocationRequested.from_dict({"canvas_token": "abc"}) is None

    def test_invocation_zero_parses(self):
        # Auto-counted ids are 0-based, so any falsy check here drops the very
        # first action run of every canvas.
        event = CanvasInvocationRequested.from_dict(
            {"canvas_token": "abc123", "invocation_id": 0},
        )

        assert event is not None
        assert event.invocation_id == 0

    def test_fields_are_read_from_the_system_event_envelope(self):
        # Comms nests them under `extra_event_fields`; reading only the top level
        # would parse nothing in production while passing a naive test.
        event = CanvasInvocationRequested.from_dict(
            {
                "assistant_id": 7,
                "extra_event_fields": {
                    "canvas_token": "abc123",
                    "invocation_id": 4,
                    "action_name": "bulk_send",
                },
            },
        )

        assert event is not None
        assert event.canvas_token == "abc123"
        assert event.invocation_id == 4
        assert "bulk_send" in event.reason


class TestRouting:
    def test_the_system_event_type_is_routed(self):
        from unify.conversation_manager import comms_manager

        built = comms_manager._canvas_invocation_event_from_payload(
            {"extra_event_fields": {"canvas_token": "abc123", "invocation_id": 1}},
        )

        assert isinstance(built, CanvasInvocationRequested)

    def test_the_event_reaches_a_handler(self):
        # Without registration the event is parsed, published and then dropped.
        from unify.conversation_manager.domains.event_handlers import EventHandler

        assert CanvasInvocationRequested in EventHandler._registry


class TestReporting:
    """How a run ends has to reach the canvas that started it.

    The parent resolves the action's promise as soon as Orchestra accepts it,
    because the work outlives the request. The terminal status published here is
    therefore the only thing that ever moves the control out of its working state
    — without it a button says "working" until the tab closes, whether the action
    sent the email or failed outright.
    """

    @pytest.mark.asyncio
    async def test_a_finished_run_is_reported(self, stub_canvas, published):
        stub_canvas(_Canvas(_Record(status="succeeded")))
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=0)

        await handle_canvas_invocation_requested(event, cm=None)

        assert len(published) == 1
        # Id 0 is the first run of a canvas, and it must survive the round trip.
        assert published[0]["invocation_id"] == 0
        assert published[0]["token"] == "abc123"
        assert published[0]["status"] == "succeeded"

    @pytest.mark.asyncio
    async def test_a_failed_run_reports_its_reason(self, stub_canvas, published):
        stub_canvas(_Canvas(_Record(status="failed", error="SMTP refused")))
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=2)

        await handle_canvas_invocation_requested(event, cm=None)

        assert published[0]["status"] == "failed"
        assert published[0]["error"] == "SMTP refused"

    @pytest.mark.asyncio
    async def test_a_run_that_cannot_start_is_still_reported(
        self,
        stub_canvas,
        published,
    ):
        # The most important of the three. `run_invocation` records its own
        # failures, so an exception escaping it means nothing was written and
        # nothing will be — leaving this unreported is the one path that hangs a
        # control forever.
        stub_canvas(_Canvas(raises=ValueError("Canvas 'abc123' has no invocation 9")))
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=9)

        await handle_canvas_invocation_requested(event, cm=None)

        assert len(published) == 1
        assert published[0]["status"] == "failed"


class TestHandler:
    @pytest.mark.asyncio
    async def test_it_runs_the_addressed_invocation(self, stub_canvas, published):
        canvas = stub_canvas(_Canvas())
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=0)

        woke = await handle_canvas_invocation_requested(event, cm=None)

        assert canvas.calls == [(0, "abc123")]
        # Deterministic work with its outcome already streamed to the canvas; a
        # conversational turn here would be one nobody asked for.
        assert woke is False

    @pytest.mark.asyncio
    async def test_a_failed_run_does_not_raise(self, stub_canvas, published):
        # The row already records the failure. Raising here would surface as an
        # unhandled event-loop error and tell the viewer nothing extra.
        stub_canvas(_Canvas(_Record(status="failed", error="SMTP refused")))
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=1)

        assert await handle_canvas_invocation_requested(event, cm=None) is False

    @pytest.mark.asyncio
    async def test_an_unrunnable_invocation_is_contained(self, stub_canvas, published):
        """A run that cannot even start must not take the event loop down.

        `run_invocation` records its own failures, so an exception escaping it
        means something more basic went wrong — a missing canvas, an unresolvable
        manager — and the handler is the last place that can absorb it.
        """
        stub_canvas(_Canvas(raises=ValueError("Canvas 'abc123' has no invocation 9")))
        event = CanvasInvocationRequested(canvas_token="abc123", invocation_id=9)

        assert await handle_canvas_invocation_requested(event, cm=None) is False
