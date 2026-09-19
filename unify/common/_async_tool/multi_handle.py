"""Coordinator and per-request handles that let one async tool loop serve
several concurrent requests over shared context."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from .request_state import RequestRegistry
from .tagging import (
    tag_message_with_request,
    format_request_cancelled_notice,
    format_request_paused_notice,
    format_request_resumed_notice,
)


class MultiHandleCoordinator:
    """Shared state behind the per-request handles of one loop: routes
    messages and results to the right request by id."""

    def __init__(
        self,
        *,
        interject_queue: asyncio.Queue,
        clarification_channels: dict,
        persist: bool = False,
    ) -> None:
        self._registry = RequestRegistry()
        self._interject_queue = interject_queue
        # call_id -> (up_q, down_q), shared with the loop.
        self._clarification_channels = clarification_channels
        # Whether the loop persists after all requests complete.
        self._persist = persist
        self._request_clarification_queues: dict[int, asyncio.Queue] = {}
        self._request_notification_queues: dict[int, asyncio.Queue] = {}

    @property
    def registry(self) -> RequestRegistry:
        return self._registry

    def register_request(self, handle_ref: Any = None) -> int:
        """Register a new request, with its own event queues, and return its id."""
        request_id = self._registry.register(handle_ref)
        self._request_clarification_queues[request_id] = asyncio.Queue()
        self._request_notification_queues[request_id] = asyncio.Queue()
        return request_id

    def complete_request(self, request_id: int, result: str) -> bool:
        """Complete a request with its final answer; False when the id is
        invalid or the request is already done."""
        return self._registry.complete(request_id, result)

    def cancel_request(self, request_id: int, reason: str | None = None) -> bool:
        """Cancel a request; False when the id is invalid or the request is
        already done."""
        return self._registry.cancel(request_id, reason)

    def inject_interjection(self, request_id: int, message: str) -> None:
        """Queue *message* tagged with its request id."""
        tagged = tag_message_with_request(message, request_id)
        self._interject_queue.put_nowait(tagged)

    def inject_cancellation_notice(
        self,
        request_id: int,
        reason: str | None = None,
    ) -> None:
        """Tell the LLM that a request was cancelled."""
        notice = format_request_cancelled_notice(request_id, reason)
        self._interject_queue.put_nowait(notice)

    def inject_pause_notice(self, request_id: int) -> None:
        """Tell the LLM that a request was paused."""
        notice = format_request_paused_notice(request_id)
        self._interject_queue.put_nowait(notice)

    def inject_resume_notice(self, request_id: int) -> None:
        """Tell the LLM that a request was resumed."""
        notice = format_request_resumed_notice(request_id)
        self._interject_queue.put_nowait(notice)

    def route_clarification_to_request(
        self,
        request_id: int,
        clarification: dict,
    ) -> None:
        """Deliver a clarification event to its request's queue."""
        q = self._request_clarification_queues.get(request_id)
        if q is not None:
            q.put_nowait(clarification)

    def get_clarification_queue(self, request_id: int) -> asyncio.Queue | None:
        return self._request_clarification_queues.get(request_id)

    def get_notification_queue(self, request_id: int) -> asyncio.Queue | None:
        return self._request_notification_queues.get(request_id)

    def should_terminate(self) -> bool:
        """True once every request is done, unless the loop persists."""
        if self._persist:
            return False
        return self._registry.is_empty()

    def is_closed(self) -> bool:
        return self._registry.is_closed()

    def close(self) -> None:
        """Refuse new requests from now on."""
        self._registry.close()

    def get_request_future(self, request_id: int) -> asyncio.Future | None:
        state = self._registry.get(request_id)
        return state.result_future if state else None

    def is_request_done(self, request_id: int) -> bool:
        """True when the request is completed, cancelled or unknown."""
        state = self._registry.get(request_id)
        return state.is_done if state else True

    def validate_request_id(self, request_id: int) -> str | None:
        """An error message when the request id is unknown or already done,
        else None."""
        state = self._registry.get(request_id)
        if state is None:
            return f"Invalid request_id {request_id}: no such request exists."
        if state.is_done:
            return f"Invalid request_id {request_id}: request is already {state.status.value}."
        return None


class MultiRequestHandle:
    """Per-request handle of a multi-handle loop: wraps one request_id and
    routes every steering operation through the shared coordinator."""

    def __init__(
        self,
        request_id: int,
        coordinator: MultiHandleCoordinator,
        *,
        loop_id: str = "",
    ) -> None:
        self._request_id = request_id
        self._coordinator = coordinator
        self._loop_id = loop_id
        self._log_label = (
            f"{loop_id}[req:{request_id}]" if loop_id else f"req:{request_id}"
        )

    @property
    def request_id(self) -> int:
        """Return the request ID for this handle."""
        return self._request_id

    async def ask(
        self,
        question: str,
        *,
        _parent_chat_context: list[dict] | None = None,
    ) -> "MultiRequestHandle":
        """Ask about this request's status.

        In multi-handle mode, this injects a tagged question and returns self.
        """
        self._coordinator.inject_interjection(self._request_id, question)
        return self

    async def interject(
        self,
        message: str,
        *,
        _parent_chat_context_cont: list[dict] | None = None,
    ) -> None:
        """Inject additional context for this request."""
        self._coordinator.inject_interjection(self._request_id, message)

    async def stop(
        self,
        reason: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Stop/cancel this specific request.

        This marks the request as cancelled and notifies the LLM.
        Does NOT stop the entire loop unless this is the last pending request.
        """
        state = self._coordinator.registry.get(self._request_id)
        if state is None or state.is_done:
            return

        self._coordinator.inject_cancellation_notice(self._request_id, reason)
        self._coordinator.cancel_request(self._request_id, reason)

    async def pause(self) -> None:
        """Pause this specific request (soft pause via LLM notification)."""
        state = self._coordinator.registry.get(self._request_id)
        if state is None or state.is_done:
            return
        self._coordinator.inject_pause_notice(self._request_id)

    async def resume(self) -> None:
        """Resume this specific request (soft resume via LLM notification)."""
        state = self._coordinator.registry.get(self._request_id)
        if state is None or state.is_done:
            return
        self._coordinator.inject_resume_notice(self._request_id)

    def done(self) -> bool:
        """Check if this request has completed."""
        return self._coordinator.is_request_done(self._request_id)

    async def result(self) -> str:
        """Wait for this request's final answer."""
        future = self._coordinator.get_request_future(self._request_id)
        if future is None:
            return f"Request {self._request_id} not found"
        return await future

    async def next_clarification(self) -> dict:
        """Await the next clarification for this request."""
        q = self._coordinator.get_clarification_queue(self._request_id)
        if q is None:
            # An unknown request never produces events: block forever.
            await asyncio.Future()
        return await q.get()

    async def next_notification(self) -> dict:
        """Await the next notification for this request."""
        q = self._coordinator.get_notification_queue(self._request_id)
        if q is None:
            await asyncio.Future()
        return await q.get()

    async def answer_clarification(self, call_id: str, answer: str) -> None:
        """Answer a clarification question.

        Routes the answer through the shared clarification channels.
        """
        channels = self._coordinator._clarification_channels
        if call_id in channels:
            _, down_q = channels[call_id]
            await down_q.put(answer)

    def add_request(self, message: str) -> "MultiRequestHandle":
        """Add a new request to the same loop.

        Parameters
        ----------
        message : str
            The initial message for the new request.

        Returns
        -------
        MultiRequestHandle
            A new handle for the added request.

        Raises
        ------
        RuntimeError
            If the loop has been closed.
        """
        if self._coordinator.is_closed():
            raise RuntimeError(
                "Loop has terminated. Start a new loop via start_async_tool_loop().",
            )

        new_request_id = self._coordinator.register_request()
        self._coordinator.inject_interjection(new_request_id, message)
        new_handle = MultiRequestHandle(
            new_request_id,
            self._coordinator,
            loop_id=self._loop_id,
        )
        state = self._coordinator.registry.get(new_request_id)
        if state:
            state.handle_ref = new_handle

        return new_handle

    def get_history(self) -> list[dict]:
        """Returns empty list - full history is on the shared loop."""
        return []
