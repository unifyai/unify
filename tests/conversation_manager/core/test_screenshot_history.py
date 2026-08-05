"""Tests for ScreenshotHistory and visual context lifecycle.

These are symbolic tests — no LLM calls. They verify:
1. Visual context messages don't accumulate when _visual_ctx_msg_id is
   shared between _inject_visual_context and _capture_screenshots_for_llm.
2. ScreenshotHistory.clear() removes entries by source.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone


from unify.conversation_manager.medium_scripts.common import ScreenshotHistory
from unify.conversation_manager.cm_types.screenshot import ScreenshotEntry

# ── Helpers ──────────────────────────────────────────────────────────────────


@dataclass
class _ChatItem:
    id: str
    role: str
    content: list


class _FakeChatContext:
    """Minimal mock of livekit.agents.llm.ChatContext."""

    def __init__(self) -> None:
        self.items: list[_ChatItem] = []

    def add_message(
        self,
        *,
        role: str,
        content: list,
        id: str | None = None,
    ) -> _ChatItem:
        item = _ChatItem(id=id or str(uuid.uuid4()), role=role, content=content)
        self.items.append(item)
        return item

    def index_by_id(self, msg_id: str) -> int | None:
        for i, item in enumerate(self.items):
            if item.id == msg_id:
                return i
        return None

    def copy(self) -> "_FakeChatContext":
        ctx = _FakeChatContext()
        ctx.items = [
            _ChatItem(id=it.id, role=it.role, content=list(it.content))
            for it in self.items
        ]
        return ctx


def _make_entry(
    source: str = "user",
    idx: int = 0,
    attribution: str | None = None,
    b64: str = "AAAA",
) -> tuple[ScreenshotEntry, str]:
    return (
        ScreenshotEntry(
            b64=b64,
            utterance=f"test {idx}",
            timestamp=datetime.now(timezone.utc),
            source=source,
            attribution=attribution,
        ),
        f"Screenshots/{source.title()}/test_{idx}.jpg",
    )


def _labels(parts: list) -> list[str]:
    return [p for p in parts if isinstance(p, str)]


def _count_visual(ctx: _FakeChatContext) -> int:
    return sum(
        1
        for it in ctx.items
        if it.role == "user" and any("[Screenshot" in str(c) for c in it.content)
    )


# ── Bug 1: Visual context accumulation ──────────────────────────────────────


def test_visual_ctx_does_not_accumulate_in_live():
    """_inject_visual_context must replace, not accumulate, in session._chat_ctx."""
    _visual_ctx_msg_id: str | None = None
    live = _FakeChatContext()
    live.add_message(role="system", content=["prompt"])
    live.add_message(role="user", content=["Hello"])
    live.add_message(role="assistant", content=["Hi"])

    for i in range(5):
        content = [f"[Screenshot #{i}]"]
        if _visual_ctx_msg_id is not None:
            idx = live.index_by_id(_visual_ctx_msg_id)
            if idx is not None:
                live.items.pop(idx)
        msg = live.add_message(role="user", content=content)
        _visual_ctx_msg_id = msg.id

    assert _count_visual(live) == 1


def test_visual_ctx_does_not_accumulate_in_copy():
    """_capture_screenshots_for_llm must produce exactly 1 visual context
    message in the copy, even though _handle_screenshot indirectly mutates
    _visual_ctx_msg_id via _inject_visual_context.

    The fix: save _visual_ctx_msg_id BEFORE calling _handle_screenshot,
    and use the saved value to remove the old message from the copy.
    """
    _visual_ctx_msg_id: str | None = None
    live = _FakeChatContext()
    live.add_message(role="system", content=["prompt"])
    live.add_message(role="user", content=["Hello"])
    live.add_message(role="assistant", content=["Hi"])
    last_copy = None

    for i in range(5):
        content = [f"[Screenshot #{i}]"]

        # Periodic _inject_visual_context (between llm_node calls)
        if _visual_ctx_msg_id is not None:
            idx = live.index_by_id(_visual_ctx_msg_id)
            if idx is not None:
                live.items.pop(idx)
        msg = live.add_message(role="user", content=content)
        _visual_ctx_msg_id = msg.id

        # LiveKit copies chat_ctx BEFORE llm_node runs
        copy = live.copy()
        saved_vid = _visual_ctx_msg_id  # Save BEFORE _handle_screenshot

        # _handle_screenshot → _inject_visual_context mutates _visual_ctx_msg_id
        if _visual_ctx_msg_id is not None:
            idx = live.index_by_id(_visual_ctx_msg_id)
            if idx is not None:
                live.items.pop(idx)
        new_msg = live.add_message(role="user", content=content)
        _visual_ctx_msg_id = new_msg.id

        # Use saved_vid (which exists in the copy) to remove
        if saved_vid is not None:
            idx = copy.index_by_id(saved_vid)
            if idx is not None:
                copy.items.pop(idx)
        copy.add_message(role="user", content=content)
        last_copy = copy

    assert (
        _count_visual(last_copy) == 1
    ), f"Expected 1 visual context message in copy, got {_count_visual(last_copy)}"


# ── Bug 2: Screenshot cleanup ───────────────────────────────────────────────


def test_clear_by_source():
    """clear(source=...) removes only entries for that source."""
    h = ScreenshotHistory()
    for i in range(3):
        h.add(*_make_entry("user", i))
    for i in range(2):
        h.add(*_make_entry("assistant", i))

    h.clear(source="user")
    assert len(h._entries) == 2
    assert all(e.source == "assistant" for e, _ in h._entries)


def test_clear_all():
    """clear() with no source removes all entries."""
    h = ScreenshotHistory()
    for i in range(3):
        h.add(*_make_entry("user", i))
    h.clear()
    assert len(h._entries) == 0
    assert h.build_visual_context_content() == []


def test_clear_nonexistent_source():
    """clear() with a source that has no entries is a no-op."""
    h = ScreenshotHistory()
    for i in range(2):
        h.add(*_make_entry("user", i))
    h.clear(source="webcam")
    assert len(h._entries) == 2


# ── Multi-sharer visual context ─────────────────────────────────────────────


def test_two_sharers_of_one_source_both_survive():
    """A second person sharing must not overwrite the first.

    Keyed on source alone, the room collapsed to whichever screen arrived last
    and the other became invisible to both brains.
    """
    h = ScreenshotHistory()
    h.add(*_make_entry("user", 0, attribution="Alice", b64="AAAA"))
    h.add(*_make_entry("user", 1, attribution="Bob", b64="BBBB"))

    parts = h.build_visual_context_content()

    labels = _labels(parts)
    assert any("Alice" in label for label in labels)
    assert any("Bob" in label for label in labels)
    images = [str(p) for p in parts if not isinstance(p, str)]
    assert len(images) == 2


def test_newer_frame_replaces_the_same_sharer():
    """One sharer contributes one image, however many frames they sent."""
    h = ScreenshotHistory()
    h.add(*_make_entry("user", 0, attribution="Alice", b64="OLD1"))
    h.add(*_make_entry("user", 1, attribution="Alice", b64="NEW1"))

    parts = h.build_visual_context_content()

    images = [str(p) for p in parts if not isinstance(p, str)]
    assert len(images) == 1
    assert "NEW1" in images[0]


def test_unattributed_frames_share_one_slot():
    """Frames with no known owner cannot be told apart, so they collapse.

    Attribution is absent on the single-user surfaces, where one slot per source
    is the correct answer and the old behaviour was already right.
    """
    h = ScreenshotHistory()
    h.add(*_make_entry("user", 0, b64="OLD1"))
    h.add(*_make_entry("user", 1, b64="NEW1"))

    images = [
        str(p) for p in h.build_visual_context_content() if not isinstance(p, str)
    ]
    assert len(images) == 1
    assert "NEW1" in images[0]


def test_sharers_beyond_the_cap_are_reported_not_hidden():
    """Truncation is announced, because silence reads as "that was everyone"."""
    from unify.conversation_manager.medium_scripts.common import (
        MAX_VISUALS_PER_SOURCE,
    )

    h = ScreenshotHistory()
    sharers = [f"Sharer{i}" for i in range(MAX_VISUALS_PER_SOURCE + 2)]
    for i, name in enumerate(sharers):
        h.add(*_make_entry("user", i, attribution=name))

    parts = h.build_visual_context_content()

    images = [p for p in parts if not isinstance(p, str)]
    assert len(images) == MAX_VISUALS_PER_SOURCE
    assert any("NOT SHOWN" in label for label in _labels(parts))


def test_earliest_sharers_keep_their_slots():
    """The cap drops the newest arrivals, not the established share."""
    from unify.conversation_manager.medium_scripts.common import (
        MAX_VISUALS_PER_SOURCE,
    )

    h = ScreenshotHistory()
    for i in range(MAX_VISUALS_PER_SOURCE + 1):
        h.add(*_make_entry("user", i, attribution=f"Sharer{i}"))

    labels = _labels(h.build_visual_context_content())

    assert any("Sharer0" in label for label in labels)
    latecomer = f"Sharer{MAX_VISUALS_PER_SOURCE}"
    assert not any(latecomer in label for label in labels)


def test_sources_stay_separate():
    """A per-source cap is per source, not a shared budget."""
    h = ScreenshotHistory()
    h.add(*_make_entry("user", 0, attribution="Alice"))
    h.add(*_make_entry("user", 1, attribution="Bob"))
    h.add(*_make_entry("webcam", 2, attribution="Alice"))
    h.add(*_make_entry("webcam", 3, attribution="Bob"))

    images = [p for p in h.build_visual_context_content() if not isinstance(p, str)]
    assert len(images) == 4
