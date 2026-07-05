"""Flush must not let one poisoned entry take down its whole batch.

A single non-JSON-serializable entry (e.g. the pre-fix LLM events that carried
a Pydantic ``response_format`` class) previously failed the batched
``create_logs`` call and dropped every event buffered for that context. The
fallback retries entries individually so only the genuinely bad rows are lost.
"""

from __future__ import annotations

import pytest

from tests.helpers import _handle_project
from unify.events.event_bus import EventBus


@pytest.mark.asyncio
@_handle_project
async def test_flush_retries_entries_individually_on_batch_failure(monkeypatch):
    bus = EventBus()

    good_a = {"event_id": "good-a", "type": "LLM", "row_id": 0}
    poisoned = {"event_id": "bad", "type": "LLM", "row_id": 1, "poison": True}
    good_b = {"event_id": "good-b", "type": "LLM", "row_id": 2}
    context = "Events/LLM"
    bus._pending_writes = [
        (good_a, context),
        (poisoned, context),
        (good_b, context),
    ]

    written: list[dict] = []

    def fake_create_logs(*, project=None, context=None, entries=None, **kwargs):
        if any(e.get("poison") for e in entries):
            raise TypeError("Object of type ModelMetaclass is not JSON serializable")
        written.extend(entries)
        return list(range(len(entries)))

    monkeypatch.setattr(
        "unify.events.event_bus.unisdk.create_logs",
        fake_create_logs,
    )

    bus.flush()

    # The batch failed, but both healthy entries were written individually;
    # only the poisoned entry was dropped.
    assert written == [good_a, good_b]
    assert bus._pending_writes == []


@pytest.mark.asyncio
@_handle_project
async def test_flush_healthy_batch_writes_once(monkeypatch):
    bus = EventBus()

    entries = [{"event_id": f"e{i}", "type": "LLM", "row_id": i} for i in range(3)]
    context = "Events/LLM"
    bus._pending_writes = [(e, context) for e in entries]

    calls: list[list[dict]] = []

    def fake_create_logs(*, project=None, context=None, entries=None, **kwargs):
        calls.append(list(entries))
        return list(range(len(entries)))

    monkeypatch.setattr(
        "unify.events.event_bus.unisdk.create_logs",
        fake_create_logs,
    )

    bus.flush()

    # One batched call, no per-entry fallback.
    assert calls == [entries]
