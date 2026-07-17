"""Distributed speaking-floor coordination for multi-assistant Unify Meets.

Every assistant on an org call runs its own independent voice session in the
shared LiveKit room, so without coordination two assistants happily talk over
each other. This module implements a small claim/hold/release protocol over
the LiveKit data channel (topic ``unify_floor``) that guarantees at most one
assistant holds the speaking floor at a time:

- ``claim``   — an assistant wants to speak. It broadcasts the claim and waits
  ``CLAIM_WINDOW_S`` for competing claims; the lowest assistant id wins a
  simultaneous claim (deterministic tie-break).
- ``hold``    — the winner announces (and periodically re-announces) that it
  holds the floor, carrying a TTL so a crashed holder cannot deadlock the
  room: peers treat an un-refreshed hold as expired.
- ``release`` — the holder finished its playout; the floor is free.

TTS playout is gated on ``acquire()``; the gate fails open on timeout so a
lost packet can never mute an assistant forever — worst case the call briefly
degrades to today's uncoordinated behavior.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable

FLOOR_TOPIC = "unify_floor"

# How long a claimant listens for competing claims before speaking.
CLAIM_WINDOW_S = 0.35
# Holder heartbeat cadence and the TTL peers apply to each hold/claim.
HEARTBEAT_S = 5.0
DEFAULT_TTL_S = 15.0
# Fail-open bound: never block a reply behind the floor for longer than this.
ACQUIRE_TIMEOUT_S = 20.0


def _rank(assistant_id: str) -> tuple[int, int | str]:
    """Sort key for the deterministic tie-break: lowest assistant id wins."""
    try:
        return (0, int(assistant_id))
    except (TypeError, ValueError):
        return (1, str(assistant_id))


class MeetFloor:
    """One participant's view of the shared speaking floor.

    ``publish`` broadcasts a JSON payload on the floor topic; ``peer_probe``
    reports whether any *other* assistant is currently in the room, which
    lets a lone assistant skip the claim window entirely (no added latency
    on 1:1 calls or while peers have not joined yet).
    """

    def __init__(
        self,
        *,
        local_id: str,
        publish: Callable[[dict], Awaitable[None]],
        peer_probe: Callable[[], bool] | None = None,
        now: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self._local_id = str(local_id)
        self._publish = publish
        self._peer_probe = peer_probe
        self._now = now
        self._log = log or (lambda _msg: None)
        self._holder_id: str | None = None
        self._holder_expires = 0.0
        self._holding = False
        self._heartbeat_task: asyncio.Task | None = None
        self._changed = asyncio.Event()

    # ------------------------------------------------------------------
    # Inbound protocol messages
    # ------------------------------------------------------------------

    def handle_message(self, data: bytes | dict) -> None:
        """Feed one raw data-channel payload from the floor topic."""
        if isinstance(data, (bytes, bytearray)):
            try:
                data = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return
        if not isinstance(data, dict):
            return
        kind = str(data.get("kind") or "")
        sender = str(data.get("assistant_id") or "")
        if not sender or sender == self._local_id:
            return
        now = self._now()
        ttl = float(data.get("ttl") or DEFAULT_TTL_S)

        if kind in ("claim", "hold"):
            if self._holding and kind == "hold":
                # Split-brain: two holders can only happen after message loss.
                # The deterministic rank decides who yields.
                if _rank(sender) < _rank(self._local_id):
                    self._log(
                        f"floor conflict: yielding to assistant {sender} "
                        f"(outranked)",
                    )
                    self._holding = False
                    self._set_holder(sender, now + ttl)
                return
            current_live = (
                self._holder_id is not None
                and self._holder_id != sender
                and self._holder_expires > now
            )
            if current_live and self._holder_id == self._local_id:
                # We hold the floor; a hold heartbeat (sent by us) covers it.
                return
            if not current_live or _rank(sender) < _rank(self._holder_id or ""):
                self._set_holder(sender, now + ttl)
        elif kind == "release":
            if sender == self._holder_id:
                self._set_holder(None, 0.0)

    def _set_holder(self, holder_id: str | None, expires: float) -> None:
        self._holder_id = holder_id
        self._holder_expires = expires
        self._changed.set()
        self._changed = asyncio.Event()

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    @property
    def holding(self) -> bool:
        return self._holding

    def _floor_available(self) -> bool:
        if self._holder_id is None or self._holder_id == self._local_id:
            return True
        return self._holder_expires <= self._now()

    def _peers_present(self) -> bool:
        if self._peer_probe is None:
            return True
        try:
            return bool(self._peer_probe())
        except Exception:  # noqa: BLE001 - probe must never break the gate
            return True

    # ------------------------------------------------------------------
    # Acquire / release
    # ------------------------------------------------------------------

    async def acquire(
        self,
        *,
        timeout: float = ACQUIRE_TIMEOUT_S,
        ttl: float = DEFAULT_TTL_S,
    ) -> bool:
        """Take the speaking floor; returns False on fail-open timeout."""
        if self._holding:
            return True
        if not self._peers_present():
            # Lone assistant: no coordination partner, speak immediately.
            self._holding = True
            self._set_holder(self._local_id, self._now() + ttl)
            self._start_heartbeat(ttl)
            return True

        deadline = self._now() + timeout
        while True:
            remaining = deadline - self._now()
            if remaining <= 0:
                self._log("floor acquire timed out; speaking anyway (fail-open)")
                return False
            if not self._floor_available():
                wait = min(remaining, max(0.05, self._holder_expires - self._now()))
                changed = self._changed
                try:
                    await asyncio.wait_for(changed.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
                continue

            await self._broadcast("claim", ttl=ttl)
            window_end = self._now() + CLAIM_WINDOW_S
            lost = False
            while self._now() < window_end:
                changed = self._changed
                try:
                    await asyncio.wait_for(
                        changed.wait(),
                        timeout=max(0.01, window_end - self._now()),
                    )
                except asyncio.TimeoutError:
                    break
                if (
                    self._holder_id is not None
                    and self._holder_id != self._local_id
                    and self._holder_expires > self._now()
                    and _rank(self._holder_id) < _rank(self._local_id)
                ):
                    lost = True
                    break
            if lost:
                continue

            self._holding = True
            self._set_holder(self._local_id, self._now() + ttl)
            await self._broadcast("hold", ttl=ttl)
            self._start_heartbeat(ttl)
            return True

    async def release(self) -> None:
        if not self._holding:
            return
        self._holding = False
        self._stop_heartbeat()
        self._set_holder(None, 0.0)
        await self._broadcast("release")

    def release_soon(self) -> None:
        """Fire-and-forget release for non-async call sites (finally blocks)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self.release())

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _broadcast(self, kind: str, *, ttl: float | None = None) -> None:
        payload: dict = {"kind": kind, "assistant_id": self._local_id}
        if ttl is not None:
            payload["ttl"] = ttl
        try:
            await self._publish(payload)
        except Exception as exc:  # noqa: BLE001 - the gate must fail open
            self._log(f"floor broadcast failed ({kind}): {exc}")

    def _start_heartbeat(self, ttl: float) -> None:
        self._stop_heartbeat()

        async def _beat() -> None:
            while self._holding:
                await asyncio.sleep(HEARTBEAT_S)
                if not self._holding:
                    return
                self._set_holder(self._local_id, self._now() + ttl)
                await self._broadcast("hold", ttl=ttl)

        self._heartbeat_task = asyncio.create_task(_beat())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
