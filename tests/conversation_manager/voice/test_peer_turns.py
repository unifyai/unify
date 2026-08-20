"""The log of what co-assistants said, which never reaches this session's STT.

A teammate's speech is not in this assistant's transcript, so without this the
only honest thing the prompts can say is "a quiet question is not evidence it
went unanswered" — a rule asking the model to reason around missing information.
These tests pin the information itself: what gets broadcast, what gets recorded,
and what expires.
"""

from __future__ import annotations

import json

import pytest

from unify.conversation_manager.medium_scripts.peer_turns import (
    MAX_GIST_CHARS,
    MAX_RETAINED,
    RECENT_WINDOW_S,
    PeerTurnLog,
)


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _log(local_id: str = "1", name: str = "Lila", clock=None) -> tuple:
    sent: list[dict] = []

    async def _publish(payload: dict) -> None:
        sent.append(payload)

    clock = clock or _Clock()
    return (
        PeerTurnLog(
            local_id=local_id,
            local_name=name,
            publish=_publish,
            now=clock,
        ),
        sent,
        clock,
    )


def _spoke(assistant_id: str, text: str, name: str = "A-DA") -> bytes:
    return json.dumps(
        {"kind": "spoke", "assistant_id": assistant_id, "name": name, "text": text},
    ).encode()


class TestAnnounce:
    @pytest.mark.asyncio
    async def test_it_broadcasts_the_line_with_its_own_identity(self):
        log, sent, _ = _log(local_id="7", name="Lila Down")
        await log.announce("The renewal closes on the 30th.")
        assert sent == [
            {
                "kind": "spoke",
                "assistant_id": "7",
                "name": "Lila Down",
                "text": "The renewal closes on the 30th.",
            },
        ]

    @pytest.mark.asyncio
    async def test_it_says_nothing_about_an_empty_line(self):
        """Whitespace is not a turn, and an empty entry reads as a silent peer."""
        log, sent, _ = _log()
        await log.announce("   ")
        assert sent == []

    @pytest.mark.asyncio
    async def test_a_long_line_is_trimmed(self):
        log, sent, _ = _log()
        await log.announce("x" * (MAX_GIST_CHARS + 200))
        assert len(sent[0]["text"]) == MAX_GIST_CHARS

    @pytest.mark.asyncio
    async def test_a_failed_broadcast_never_reaches_the_caller(self):
        """This runs inside the utterance path; a lost packet must not break it."""

        async def _boom(_payload):
            raise RuntimeError("channel down")

        log = PeerTurnLog(
            local_id="1",
            local_name="Lila",
            publish=_boom,
            now=_Clock(),
        )
        await log.announce("still fine")  # must not raise


class TestRecording:
    def test_a_peer_line_is_recorded_with_its_speaker(self):
        log, _, _ = _log()
        log.handle_message(_spoke("9", "I've sent the quote over."))
        assert log.recent() == ["A-DA: I've sent the quote over."]

    def test_its_own_broadcast_is_ignored(self):
        """Every assistant hears its own data packets; reading them back would
        have it treat its own line as a teammate's."""
        log, _, _ = _log(local_id="7")
        log.handle_message(_spoke("7", "Something I said."))
        assert log.recent() == []

    def test_an_unnamed_peer_still_reads_as_somebody(self):
        log, _, _ = _log()
        log.handle_message(
            json.dumps(
                {"kind": "spoke", "assistant_id": "9", "text": "Done."},
            ).encode(),
        )
        assert log.recent() == ["assistant 9: Done."]

    @pytest.mark.parametrize(
        "payload",
        [
            b"not json at all",
            b"[]",
            b'{"kind": "hold", "assistant_id": "9"}',
            b'{"kind": "spoke", "assistant_id": "9", "text": "  "}',
            b'{"kind": "spoke", "text": "no sender"}',
        ],
    )
    def test_junk_and_foreign_kinds_are_dropped(self, payload):
        """The floor's own kinds share the room; only 'spoke' is ours."""
        log, _, _ = _log()
        log.handle_message(payload)
        assert log.recent() == []

    def test_only_the_last_few_are_kept(self):
        log, _, _ = _log()
        for i in range(MAX_RETAINED + 4):
            log.handle_message(_spoke("9", f"line {i}"))
        lines = log.recent()
        assert len(lines) == MAX_RETAINED
        # Oldest first, and the oldest surviving entry is the newest window.
        assert lines[-1].endswith(f"line {MAX_RETAINED + 3}")

    def test_lines_are_oldest_first(self):
        """The block reads as a transcript, so order has to match speech order."""
        log, _, _ = _log()
        log.handle_message(_spoke("9", "first"))
        log.handle_message(_spoke("9", "second"))
        assert log.recent() == ["A-DA: first", "A-DA: second"]


class TestExpiry:
    def test_a_stale_line_drops_out_of_the_window(self):
        """A line from ten minutes ago is history.

        Treating it as live invites standing down from a question that has since
        been asked again.
        """
        log, _, clock = _log()
        log.handle_message(_spoke("9", "answered ages ago"))
        clock.advance(RECENT_WINDOW_S + 1)
        assert log.recent() == []

    def test_a_fresh_line_is_inside_the_window(self):
        log, _, clock = _log()
        log.handle_message(_spoke("9", "just now"))
        clock.advance(RECENT_WINDOW_S - 1)
        assert log.recent() == ["A-DA: just now"]

    def test_the_window_is_callable_per_read(self):
        log, _, clock = _log()
        log.handle_message(_spoke("9", "a minute ago"))
        clock.advance(60)
        assert log.recent(within=30) == []
        assert log.recent(within=120) == ["A-DA: a minute ago"]
