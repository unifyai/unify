"""Per-request state and the registry that tracks the concurrent requests
of one multi-handle async tool loop."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


class RequestStatus(Enum):
    """Status of a request within a multi-handle tool loop."""

    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class RequestState:
    """One request of a multi-handle loop: its 0-indexed id, lifecycle
    status, the future resolved on completion or cancellation, and the
    handle (a SteerableToolHandle, typed Any to avoid a circular import)."""

    request_id: int
    status: RequestStatus = RequestStatus.PENDING
    _result_future: asyncio.Future | None = field(default=None, repr=False)
    handle_ref: Any = None

    @property
    def result_future(self) -> asyncio.Future:
        """The result future, created on first access so it binds to the
        loop that is running by then."""
        if self._result_future is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # Outside a running loop (sync contexts), get_event_loop()
                # does not auto-create one on Python 3.12+.
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            self._result_future = loop.create_future()
        return self._result_future

    @property
    def is_pending(self) -> bool:
        return self.status == RequestStatus.PENDING

    @property
    def is_done(self) -> bool:
        return self.status != RequestStatus.PENDING


class RequestRegistry:
    """Auto-incrementing request ids, lifecycle transitions and the queries
    that decide when a multi-handle loop terminates."""

    def __init__(self) -> None:
        self._requests: dict[int, RequestState] = {}
        self._next_request_id: int = 0
        self._closed: bool = False

    def next_id(self) -> int:
        """The id the next registration would receive."""
        return self._next_request_id

    def register(self, handle_ref: Any = None) -> int:
        """Register a request and return its id; raises RuntimeError once
        the registry is closed."""
        if self._closed:
            raise RuntimeError(
                "Request registry is closed. Start a new loop via start_async_tool_loop().",
            )

        request_id = self._next_request_id
        self._next_request_id += 1

        state = RequestState(
            request_id=request_id,
            handle_ref=handle_ref,
        )
        self._requests[request_id] = state
        return request_id

    def get(self, request_id: int) -> RequestState | None:
        return self._requests.get(request_id)

    def complete(self, request_id: int, result: str) -> bool:
        """Complete a request and resolve its future with *result*; False
        when the id is invalid or the request is already done."""
        state = self._requests.get(request_id)
        if state is None:
            return False
        if state.is_done:
            return False

        state.status = RequestStatus.COMPLETED
        if not state.result_future.done():
            state.result_future.set_result(result)
        return True

    def cancel(self, request_id: int, reason: str | None = None) -> bool:
        """Cancel a request and resolve its future with a cancellation
        notice; False when the id is invalid or the request is already done."""
        state = self._requests.get(request_id)
        if state is None:
            return False
        if state.is_done:
            return False

        state.status = RequestStatus.CANCELLED
        if not state.result_future.done():
            cancel_msg = f"Request {request_id} cancelled"
            if reason:
                cancel_msg += f": {reason}"
            state.result_future.set_result(cancel_msg)
        return True

    def pending_count(self) -> int:
        return sum(1 for s in self._requests.values() if s.is_pending)

    def is_empty(self) -> bool:
        """True when no request is pending."""
        return self.pending_count() == 0

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Refuse new registrations from now on."""
        self._closed = True
