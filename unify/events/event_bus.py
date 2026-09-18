"""In-process, asyncio-friendly publish/subscribe for typed events.

Every published event is validated against the payload model registered for
its type in ``unify.events.types``, appended to a bounded ring of recent
events that ``search`` reads, and offered to every registered callback.
Nothing leaves the process: the bus is telemetry and coordination for one
running assistant, and a restart starts it empty.
"""

from __future__ import annotations

import asyncio
import contextvars
import datetime as dt
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Union
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__ = ["Event", "EventBus", "EVENT_BUS", "RING_SIZE"]

# Root sequence number of the callback cascade currently executing. A
# callback scheduled from inside another callback inherits its root, so
# ``ajoin_callbacks`` can await a whole cascade while ignoring unrelated
# activity that starts afterwards.
_CURRENT_ROOT_SEQ: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_CURRENT_ROOT_SEQ",
    default=None,
)

# How many recent events the bus keeps for ``search``.
RING_SIZE = 1000


class Event(BaseModel):
    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique id of this event",
    )
    calling_id: str = Field(
        default="",
        description="Id of the call that produced the event; groups one call's events",
    )
    type: str = Field(description="Event type: a key of PAYLOAD_REGISTRY")
    timestamp: dt.datetime = Field(
        default_factory=lambda: dt.datetime.now(dt.UTC),
        description="When the event happened (UTC)",
    )
    payload: Any
    payload_cls: str = ""  # dotted path of the model the payload was validated as

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def _validate_and_coerce_payload(self):
        """Validate the payload against its type's model and store it as a dict.

        Consumers always see a plain dict (``payload["key"]`` / ``payload.get``).
        """
        from .types import PAYLOAD_REGISTRY

        if self.type not in PAYLOAD_REGISTRY:
            raise ValueError(
                f"Unknown event type '{self.type}'. "
                f"Known types: {list(PAYLOAD_REGISTRY.keys())}. "
                f"Define a Pydantic payload model in unify/events/types/.",
            )
        expected_model = PAYLOAD_REGISTRY[self.type]
        if isinstance(self.payload, BaseModel):
            model = self.payload
        elif isinstance(self.payload, dict):
            model = expected_model.model_validate(self.payload)
        else:
            raise ValueError(
                f"Payload for event type '{self.type}' must be a dict or "
                f"{expected_model.__name__}, got {type(self.payload).__name__}",
            )
        if not self.payload_cls:
            object.__setattr__(
                self,
                "payload_cls",
                f"{model.__class__.__module__}.{model.__class__.__name__}",
            )
        object.__setattr__(self, "payload", model.model_dump(mode="python"))
        return self


Callback = Callable[[List[Event]], Union[Awaitable[None], None]]


@dataclass
class _Subscription:
    """A callback fired every ``every_n`` events of ``event_type`` that pass ``filter``."""

    event_type: str
    callback: Callback
    filter: Optional[str]
    every_n: int
    subscription_id: str = field(default_factory=lambda: str(uuid4()))
    pending: int = 0  # matching events seen since the last trigger

    def matches(self, evt: Event) -> bool:
        return self.event_type == evt.type and EventBus._match_filter(evt, self.filter)

    def should_trigger(self) -> bool:
        self.pending += 1
        if self.pending < self.every_n:
            return False
        self.pending = 0
        return True


class EventBus:
    def __init__(self) -> None:
        self._ring: Deque[Event] = deque(maxlen=RING_SIZE)
        self._subscriptions: Dict[str, _Subscription] = {}
        # Callback tasks still running, each tagged with its sequence number
        # and the root of its cascade so the join methods can pick out the
        # ones that were pending when they were called.
        self._callback_futures: set[asyncio.Future] = set()
        self._callback_seq: int = 0

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    async def publish(self, event: Event) -> None:
        """Record *event* and fire every subscription it triggers."""
        self._ring.append(event)
        self._drop_futures_on_dead_loops()
        loop = asyncio.get_running_loop()
        for sub in list(self._subscriptions.values()):
            if not sub.matches(event) or not sub.should_trigger():
                continue
            self._callback_seq += 1
            seq = self._callback_seq
            root_seq = _CURRENT_ROOT_SEQ.get() or seq
            if asyncio.iscoroutinefunction(sub.callback):
                token = _CURRENT_ROOT_SEQ.set(root_seq)
                try:
                    fut: asyncio.Future = loop.create_task(sub.callback([event]))
                finally:
                    _CURRENT_ROOT_SEQ.reset(token)
            else:
                fut = loop.run_in_executor(None, sub.callback, [event])
            setattr(fut, "_eb_seq", seq)
            setattr(fut, "_eb_root_seq", root_seq)
            self._callback_futures.add(fut)
            fut.add_done_callback(self._callback_futures.discard)

    def join_published(self) -> None:
        """Every published event is visible to ``search`` once ``publish`` returns.

        Nothing is buffered between the two, so there is nothing to wait for;
        tests call this at the point where they expect the published events
        to be visible.
        """

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------
    def search(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Event]:
        """Return recent events that satisfy *filter*, newest first.

        *filter* is a Python expression evaluated per event with ``evt``,
        ``type`` (alias ``event_type``), ``payload`` and the other event
        fields in scope. The first *offset* matches are skipped and at most
        *limit* are returned.
        """
        matched = [
            evt for evt in reversed(self._ring) if self._match_filter(evt, filter)
        ]
        return matched[offset : offset + limit]

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------
    def register_callback(
        self,
        *,
        event_type: str,
        callback: Callback,
        filter: Optional[str] = None,
        every_n: int = 1,
    ) -> str:
        """Fire *callback* with each *every_n*-th *event_type* event passing *filter*.

        A coroutine callback runs as a task on the publishing loop; a plain
        function runs in the default executor. Returns the subscription id
        for ``unregister_callback``.
        """
        from .types import PAYLOAD_REGISTRY

        if event_type not in PAYLOAD_REGISTRY:
            raise ValueError(
                f"Unknown event type '{event_type}'. "
                f"Known types: {list(PAYLOAD_REGISTRY.keys())}.",
            )
        sub = _Subscription(
            event_type=event_type,
            callback=callback,
            filter=filter,
            every_n=every_n,
        )
        self._subscriptions[sub.subscription_id] = sub
        return sub.subscription_id

    def unregister_callback(self, subscription_id: str) -> None:
        del self._subscriptions[subscription_id]

    # ------------------------------------------------------------------
    # Joining callbacks
    # ------------------------------------------------------------------
    def _drop_futures_on_dead_loops(self) -> None:
        """Forget callbacks whose loop can never run them.

        The bus is process-global and outlives any one ConversationManager
        session. An in-process reboot on a fresh loop must not await the
        tasks a dead loop left behind: their done-callbacks never fire, so
        they would sit in the set forever and wedge every join.
        """
        for fut in list(self._callback_futures):
            fut_loop = fut.get_loop()
            if fut_loop.is_closed() or not fut_loop.is_running():
                self._callback_futures.discard(fut)

    async def ajoin_callbacks(self, *, cascade: bool = True) -> None:
        """Wait for the callbacks pending on the running loop when this was called.

        With *cascade* (the default) descendants those callbacks spawn are
        awaited too, while callbacks triggered by unrelated later events are
        not, so a busy bus cannot keep the join from returning.
        """
        self._drop_futures_on_dead_loops()
        loop = asyncio.get_running_loop()
        cutoff = self._callback_seq
        attr = "_eb_root_seq" if cascade else "_eb_seq"
        while True:
            to_await = [
                fut
                for fut in list(self._callback_futures)
                if fut.get_loop() is loop and getattr(fut, attr) <= cutoff
            ]
            if not to_await:
                return
            await asyncio.gather(*to_await, return_exceptions=True)
            # Let the done-callbacks scheduled via call_soon remove the
            # finished futures before the next pass.
            await asyncio.sleep(0)
            if not cascade:
                return

    def join_callbacks(self, *, cascade: bool = True) -> None:
        """Blocking ``ajoin_callbacks`` for callers off the event-loop thread.

        The callbacks run on the loop that published their events, so this
        schedules the join onto that loop and blocks the calling thread until
        it completes.
        """
        self._drop_futures_on_dead_loops()
        pending = [fut for fut in self._callback_futures if not fut.done()]
        if not pending:
            return
        asyncio.run_coroutine_threadsafe(
            self.ajoin_callbacks(cascade=cascade),
            pending[0].get_loop(),
        ).result()

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------
    def clear(self) -> None:
        """Forget every event, subscription and pending callback."""
        self._ring.clear()
        self._subscriptions.clear()
        self._callback_futures.clear()
        self._callback_seq = 0

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------
    @staticmethod
    def _match_filter(evt: Event, filter_expr: Optional[str]) -> bool:
        """Whether *evt* satisfies *filter_expr* (an empty expression matches all)."""
        if not filter_expr:
            return True
        ns: dict[str, Any] = {
            "evt": evt,
            "event_type": evt.type,
            **evt.model_dump(mode="python"),
        }
        return bool(eval(filter_expr, {"__builtins__": {}}, ns))


EVENT_BUS = EventBus()
