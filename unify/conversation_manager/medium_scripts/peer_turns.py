"""What the other assistants on an org meet have just said.

Each assistant in a shared LiveKit room runs its own voice session and is linked
to a single human participant, so a teammate's speech never reaches its STT: the
transcript it reasons over has no record that anyone else answered. The prompts
work around that by telling it a quiet question is not evidence of an unanswered
one — true, but it asks a model to reason around missing information rather than
supplying it.

This carries the missing information. When an assistant says anything it
broadcasts the line on its own data-channel topic; peers keep a short, expiring
log and read it back as context on their next turn. An assistant that can see a
teammate already answered can choose silence, which is both the right behaviour
and cheaper than every other way of arriving at it.

Deliberately separate from ``meet_floor``: that is a mutual-exclusion protocol
whose state machine decides who may speak, this is an informational log that
decides nothing. They share a room, not a topic — a malformed message here can
never confuse the floor's claim/hold/release parsing.
"""

from __future__ import annotations

import json
import time
from typing import Awaitable, Callable

PEER_TURN_TOPIC = "unify_peer_turns"

# One line per teammate turn, newest last. Small on purpose: this is context for
# "was the current turn already handled", and a long tail of older lines buries
# the one that matters.
MAX_RETAINED = 5
# Nothing beyond this is relevant to the turn being decided now — a line from
# ten minutes ago is history, and treating it as live invites standing down from
# a question that has since been re-asked.
RECENT_WINDOW_S = 120.0
# Long enough to tell what a line was about, short enough that the block stays
# readable next to everything else in the turn prompt.
MAX_GIST_CHARS = 240


class PeerTurnLog:
    """One assistant's view of what its teammates have said.

    ``publish`` broadcasts a JSON payload on ``PEER_TURN_TOPIC``. Sends and
    receives are independent: an assistant with no peers still announces (nobody
    is listening, and the cost is one small unreliable packet), and one that
    never speaks still learns.
    """

    def __init__(
        self,
        *,
        local_id: str,
        local_name: str,
        publish: Callable[[dict], Awaitable[None]],
        now: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self._local_id = str(local_id)
        self._local_name = (local_name or "").strip()
        self._publish = publish
        self._now = now
        self._log = log or (lambda _msg: None)
        # (speaker, text, received_at), oldest first.
        self._turns: list[tuple[str, str, float]] = []

    async def announce(self, text: str) -> None:
        """Tell peers what this assistant just said."""
        gist = " ".join((text or "").split())[:MAX_GIST_CHARS].strip()
        if not gist:
            return
        payload = {
            "kind": "spoke",
            "assistant_id": self._local_id,
            "name": self._local_name,
            "text": gist,
        }
        try:
            await self._publish(payload)
        except Exception as exc:  # noqa: BLE001 - never break the utterance path
            self._log(f"peer-turn announce failed: {exc}")

    def handle_message(self, data: bytes | dict) -> None:
        """Record one inbound payload from the peer-turn topic."""
        if isinstance(data, (bytes, bytearray)):
            try:
                data = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return
        if not isinstance(data, dict):
            return
        if str(data.get("kind") or "") != "spoke":
            return
        sender = str(data.get("assistant_id") or "")
        if not sender or sender == self._local_id:
            return
        text = " ".join(str(data.get("text") or "").split())[:MAX_GIST_CHARS].strip()
        if not text:
            return
        # The sender is authoritative about its own name; the id is the fallback
        # so an unnamed teammate still reads as somebody rather than as nobody.
        speaker = str(data.get("name") or "").strip() or f"assistant {sender}"
        self._turns.append((speaker, text, self._now()))
        del self._turns[:-MAX_RETAINED]

    def recent(self, *, within: float = RECENT_WINDOW_S) -> list[str]:
        """Teammate lines from the last ``within`` seconds, oldest first."""
        cutoff = self._now() - within
        return [
            f"{speaker}: {text}" for speaker, text, at in self._turns if at >= cutoff
        ]
