"""Human-like browser motor skills (eased scroll, cursor drift, key-scroll).

Composed on top of the low-level computer-session actions (``scroll`` /
``move`` / ``press_key``) so any browser-driven flow can pace itself like a
person instead of snapping through a page like a bot:

* **eased, chunky wheel scroll** — a screenful-plus delivered as a handful of
  ease-in-out wheel ticks (not one jump, not a pixel crawl), dispatched over a
  neutral gutter so a hovered ``<video>``/embedded scroller can't swallow the
  wheel (the "feed stops scrolling" trap);
* **bezier cursor drift** — an occasional idle "reading" glide of the pointer;
* **PageDown fallback** — key events go to the document, so they can't be
  intercepted by a hovered element; used as an occasional variation and as the
  stall recovery when a wheel scroll mounts nothing new;
* **jittered dwell / settle** — randomised pauses around a base.

Session-agnostic: :class:`HumanInput` works against any object exposing async
``scroll(x, y, delta_x, delta_y)``, ``move(x, y)`` and ``press_key(key)`` — i.e.
a :class:`~unify.function_manager.primitives.runtime.WebSessionHandle`.
Unsupported actions self-disable *for that instance* (an older agent-service
without ``move``/``press_key`` degrades gracefully rather than erroring), so
callers can enable humanisation unconditionally.

The defaults are sensible for a typical single-column web feed; tune via
:class:`HumanizeConfig`. Nothing here is site-specific — per-site policy (how
many scrolls, when to escalate to the keyboard, which gutter) stays with the
caller.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class HumanizeConfig:
    """Tunables for :class:`HumanInput` (jitter is applied per action)."""

    # Distance per scroll ≈ 1–1.7 viewports. Small deltas split into many tiny
    # ticks look like a pixel crawl; a human flicks a screenful-plus at a time.
    delta_min: int = 900
    delta_max: int = 1600
    # Dwell after a scroll (seconds).
    dwell_min: float = 1.2
    dwell_max: float = 3.4
    # Initial settle before the first action (seconds) — a person orients first.
    initial_min: float = 1.8
    initial_max: float = 4.5
    # Chance of a brief up-scroll (re-reading) instead of scrolling down.
    upscroll_prob: float = 0.12
    # Chance of a bezier cursor drift before a scroll.
    move_prob: float = 0.6
    # Cursor "reading" drift target — roughly the content column.
    anchor_x_min: int = 360
    anchor_x_max: int = 660
    anchor_y_min: int = 300
    anchor_y_max: int = 520
    # The wheel is dispatched over a neutral gutter, NOT the content column:
    # hovering a <video>/embedded scroller lets it swallow the wheel. A small
    # left-margin x routes the wheel to the page scroller.
    scroll_gutter_x_min: int = 12
    scroll_gutter_x_max: int = 120
    scroll_gutter_y_min: int = 280
    scroll_gutter_y_max: int = 540
    # Chunky wheel notches (~a real mouse wheel): the scroll distance is
    # delivered as eased ticks of roughly this size.
    wheel_notch_min: int = 100
    wheel_notch_max: int = 150
    # Gap between eased wheel ticks (seconds).
    kinetic_gap_min: float = 0.012
    kinetic_gap_max: float = 0.05
    # PageDown variation + stall fallback.
    page_key_prob: float = 0.2
    page_key_presses_min: int = 1
    page_key_presses_max: int = 3


def kinetic_deltas(total: int, steps: int) -> list[int]:
    """Split a scroll ``total`` into ``steps`` eased wheel increments.

    Real wheel/trackpad scrolling isn't one big jump: the deltas ramp up then
    taper off. Each tick is weighted by a sine bump (peaking mid-gesture =
    ease-in-out), jittered so it isn't a perfect curve, with rounding drift
    absorbed by the final tick so the ticks sum exactly to ``total`` and
    preserve its sign.
    """
    steps = max(1, steps)
    sign = 1 if total >= 0 else -1
    mag = abs(total)
    weights = [
        math.sin(math.pi * (i + 0.5) / steps) * random.uniform(0.85, 1.15)
        for i in range(steps)
    ]
    wsum = sum(weights) or 1.0
    deltas = [int(round(mag * w / wsum)) for w in weights]
    deltas[-1] += mag - sum(deltas)  # absorb rounding drift
    return [sign * d for d in deltas if d]


def settle(base: float, *, humanize: bool = True, floor: float = 0.5) -> float:
    """A post-navigation settle — jittered around ``base`` when humanising."""
    if not humanize:
        return base
    return max(floor, random.uniform(base * 0.7, base * 1.6))


class HumanInput:
    """Human-like scroll / cursor motor skills over one browser session.

    Bind to a session (any object exposing async ``scroll`` / ``move`` /
    ``press_key``) once and call :meth:`scroll` / :meth:`cursor_drift` in the
    read loop. Actions the connected backend doesn't support self-disable for
    this instance after the first failure, so the loop keeps running.
    """

    def __init__(self, session: Any, cfg: Optional[HumanizeConfig] = None):
        self.session = session
        self.cfg = cfg or HumanizeConfig()
        self._move_supported = True
        self._page_key_supported = True
        # Prefer the native ``scroll_humanlike`` gesture (one round-trip, eased
        # inside the browser); self-disable to composed ticks on an older
        # agent-service without it.
        self._native_scroll_supported = True

    async def cursor_drift(self) -> None:
        """Occasionally glide the cursor to a random point (bezier motion).

        Best-effort flavour: pure idle "reading" motion, not required for the
        scroll to work. Self-disables if the backend has no ``move`` action.
        """
        cfg = self.cfg
        if not self._move_supported or random.random() > cfg.move_prob:
            return
        mover = getattr(self.session, "move", None)
        if mover is None:
            self._move_supported = False
            return
        x = random.randint(cfg.anchor_x_min, cfg.anchor_x_max)
        y = random.randint(cfg.anchor_y_min, cfg.anchor_y_max)
        try:
            await mover(x, y)
        except Exception:  # noqa: BLE001
            self._move_supported = False
            logger.debug("cursor drift unsupported; disabling for this session",
                         exc_info=True)

    async def keyboard_scroll(self) -> bool:
        """Scroll via ``PageDown`` key(s). Returns True if it ran.

        Key events target the *document*, so — unlike a wheel — a hovered
        ``<video>``/embedded scroller can't swallow them. Self-disables if the
        backend has no ``press_key`` action.
        """
        cfg = self.cfg
        if not self._page_key_supported:
            return False
        presser = getattr(self.session, "press_key", None)
        if presser is None:
            self._page_key_supported = False
            return False
        presses = random.randint(cfg.page_key_presses_min, cfg.page_key_presses_max)
        try:
            for _ in range(presses):
                await presser("PageDown")
                await asyncio.sleep(random.uniform(0.15, 0.5))
            return True
        except Exception:  # noqa: BLE001
            self._page_key_supported = False
            logger.debug("keyboard scroll unsupported; disabling for this session",
                         exc_info=True)
            return False

    async def scroll(self, *, prefer_keyboard: bool = False) -> None:
        """One human-ish scroll of ~1–1.7 viewports.

        Delivered as eased, chunky wheel ticks (see :func:`kinetic_deltas`) over
        a neutral gutter so a hovered video can't swallow them. Prefers the
        native ``scroll_humanlike`` gesture (one round-trip, ticks eased inside
        the browser with a single stability wait) and falls back to composed
        per-tick ``scroll`` calls on an older backend. ``prefer_keyboard`` (set
        by the caller when the previous scroll mounted nothing new) routes
        through ``PageDown`` instead, which the page can't intercept; ``PageDown``
        is also mixed in occasionally for variety.
        """
        cfg = self.cfg
        await self.cursor_drift()

        if prefer_keyboard or random.random() < cfg.page_key_prob:
            if await self.keyboard_scroll():
                await asyncio.sleep(random.uniform(cfg.dwell_min, cfg.dwell_max))
                return

        x = random.randint(cfg.scroll_gutter_x_min, cfg.scroll_gutter_x_max)
        y = random.randint(cfg.scroll_gutter_y_min, cfg.scroll_gutter_y_max)
        if random.random() < cfg.upscroll_prob:
            delta_y = -random.randint(cfg.delta_min // 3, cfg.delta_min // 2)
        else:
            delta_y = random.randint(cfg.delta_min, cfg.delta_max)
        notch = random.randint(cfg.wheel_notch_min, cfg.wheel_notch_max)
        steps = max(3, min(12, round(abs(delta_y) / notch)))
        # The ease-in-out *shape* is owned here (one place); the browser (native
        # path) or this loop (fallback) just plays the notches back.
        notches = kinetic_deltas(delta_y, steps)
        if not await self._native_scroll(x, y, notches):
            try:
                for tick in notches:
                    await self.session.scroll(x, y, 0, tick)
                    await asyncio.sleep(
                        random.uniform(cfg.kinetic_gap_min, cfg.kinetic_gap_max)
                    )
            except Exception:  # noqa: BLE001
                logger.debug("human scroll failed", exc_info=True)
        await asyncio.sleep(random.uniform(cfg.dwell_min, cfg.dwell_max))

    async def _native_scroll(self, x: int, y: int, notches: list[int]) -> bool:
        """Play the notches via the native ``scroll_humanlike`` gesture.

        Returns True if it ran (one round-trip, eased in the browser). Returns
        False — and self-disables for this instance — if the backend has no
        ``scroll_humanlike`` action, so the caller falls back to composed ticks.
        """
        if not self._native_scroll_supported or not notches:
            return False
        native = getattr(self.session, "scroll_humanlike", None)
        if native is None:
            self._native_scroll_supported = False
            return False
        cfg = self.cfg
        try:
            await native(
                x,
                y,
                notches,
                gap_min_ms=cfg.kinetic_gap_min * 1000.0,
                gap_max_ms=cfg.kinetic_gap_max * 1000.0,
            )
            return True
        except Exception:  # noqa: BLE001
            self._native_scroll_supported = False
            logger.debug("native scroll unsupported; using composed ticks",
                         exc_info=True)
            return False


__all__ = ["HumanizeConfig", "HumanInput", "kinetic_deltas", "settle"]
