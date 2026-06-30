"""Google Cloud Pub/Sub implementation of ``IngressTransport``.

Extracted from the inline Pub/Sub subscriber code currently living in
``unify.conversation_manager.comms_manager.CommsManager`` (specifically
``handle_message`` and ``subscribe_to_topic``). This is Phase A.bis.3:
the new transport lands as standalone code while ``CommsManager``
keeps its inline copy. A.bis.4 then wires ``CommsManager`` to consume
this transport via constructor injection and deletes the inline copy.

Threading model
===============

Pub/Sub's ``SubscriberClient.subscribe()`` runs callbacks on a
background thread pool, not on the asyncio event loop. This
transport captures the loop at ``start()`` time and bridges every
callback through ``asyncio.run_coroutine_threadsafe`` so the
dispatcher executes on the main loop. For envelopes whose ``thread``
matches ``blocking_dispatch_threads`` (default: any thread containing
``"call"`` or ``"meet"``), the Pub/Sub callback blocks on
``future.result()`` until the dispatcher completes -- this preserves
the ordering guarantees today's code relies on for voice/meeting
events.

Topic and subscription naming are the caller's responsibility. The
hosted Unity convention is::

    topic        = f"unity-{agent_id}{env_suffix}"
    subscription = f"unity-{agent_id}{env_suffix}-sub"

(No ``assistant_topic()`` helper exists today; this convention is
inlined at the call sites in ``comms_manager.py`` and
``comms_utils.py``.) ``PubSubIngressTransport`` does not impose this
convention -- it just consumes whatever subscription ID is passed in.

Error handling
==============

JSON decoding failures, missing keys, and other transport-level
exceptions invoke the optional ``on_transport_error`` callback (which
receives the exception) and then ``ack`` the message so Pub/Sub does
not redeliver an envelope the transport cannot parse. Exceptions
raised by the dispatcher itself are logged via the
``streaming_pull_future``'s done-callback path and do not bring down
the subscriber; the dispatcher is responsible for its own ack/nack
semantics via the callables passed to it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from functools import partial
from typing import Any, Callable

from unify.gateway.ingress import (
    EnvelopeDispatcher,
    IngressTransport,
)

try:
    from google.cloud import pubsub_v1
except ImportError:  # pragma: no cover - exercised in pubsub-less installs
    pubsub_v1 = None

_log = logging.getLogger("unify.gateway.pubsub")


class PubSubIngressTransport(IngressTransport):
    """Receive envelopes from a Google Cloud Pub/Sub subscription."""

    def __init__(
        self,
        *,
        subscription_id: str,
        project_id: str,
        max_messages: int | None = None,
        credentials: Any = None,
        on_transport_error: Callable[[Exception], None] | None = None,
        blocking_dispatch_threads: tuple[str, ...] = ("call", "meet"),
    ) -> None:
        if not subscription_id:
            raise ValueError("subscription_id must be non-empty")
        if not project_id:
            raise ValueError("project_id must be non-empty")
        self._subscription_id = subscription_id
        self._project_id = project_id
        self._max_messages = max_messages
        self._credentials = credentials
        self._on_transport_error = on_transport_error
        self._blocking_threads = tuple(blocking_dispatch_threads)
        self._subscriber: Any = None
        self._streaming_pull_future: Any = None
        self._dispatcher: EnvelopeDispatcher | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def source_topic(self) -> str:
        """The topic name the subscription is bound to.

        Derived from the subscription ID by stripping the ``-sub``
        suffix, matching the hosted Unity convention. Passed as the
        ``source_topic`` argument on every dispatcher call.
        """
        return self._subscription_id.removesuffix("-sub")

    def _log_dispatch_future(self, future: Any) -> None:
        exc = future.exception()
        if exc is not None:
            _log.error("dispatcher raised on envelope dispatch: %s", exc)

    def _handle_message(self, message: Any) -> None:
        """Pub/Sub callback. Runs on Pub/Sub's background thread pool."""
        assert self._loop is not None and self._dispatcher is not None
        try:
            payload = json.loads(message.data.decode("utf-8"))
            thread = payload.get("thread", "")
            future = asyncio.run_coroutine_threadsafe(
                self._dispatcher(
                    payload,
                    source_topic=self.source_topic,
                    ack=message.ack,
                    nack=message.nack,
                ),
                self._loop,
            )
            future.add_done_callback(self._log_dispatch_future)
            if any(token in thread for token in self._blocking_threads):
                future.result()
        except Exception as exc:
            _log.error(
                "PubSubIngressTransport: transport-level error on subscription %s: %s",
                self._subscription_id,
                exc,
            )
            if self._on_transport_error is not None:
                try:
                    self._on_transport_error(exc)
                except Exception as cb_exc:
                    _log.error(
                        "on_transport_error callback raised: %s",
                        cb_exc,
                    )
            try:
                message.ack()
            except Exception as ack_exc:
                _log.error("ack() raised after transport error: %s", ack_exc)

    async def start(self, dispatcher: EnvelopeDispatcher) -> None:
        if self._streaming_pull_future is not None:
            raise RuntimeError(
                "PubSubIngressTransport.start: transport already started; "
                "call stop() first",
            )
        if pubsub_v1 is None:
            raise RuntimeError(
                "PubSubIngressTransport requires google-cloud-pubsub; "
                "install the package and retry, or use a different transport "
                "(e.g. InMemoryIngressTransport)",
            )

        self._dispatcher = dispatcher
        self._loop = asyncio.get_running_loop()

        if self._credentials:
            self._subscriber = pubsub_v1.SubscriberClient(credentials=self._credentials)
        else:
            self._subscriber = pubsub_v1.SubscriberClient()

        subscription_path = self._subscriber.subscription_path(
            self._project_id,
            self._subscription_id,
        )
        flow_control = (
            pubsub_v1.types.FlowControl(max_messages=self._max_messages)
            if self._max_messages
            else pubsub_v1.types.FlowControl()
        )
        callback = partial(self._handle_message)
        self._streaming_pull_future = self._subscriber.subscribe(
            subscription_path,
            callback=callback,
            flow_control=flow_control,
        )
        _log.info(
            "PubSubIngressTransport: subscribed to %s (max_messages=%s)",
            subscription_path,
            self._max_messages,
        )

    async def stop(self) -> None:
        if self._streaming_pull_future is not None:
            try:
                self._streaming_pull_future.cancel()
            except Exception as exc:
                _log.warning(
                    "PubSubIngressTransport.stop: cancel raised: %s",
                    exc,
                )
            self._streaming_pull_future = None
        if self._subscriber is not None:
            try:
                self._subscriber.close()
            except Exception as exc:
                _log.warning(
                    "PubSubIngressTransport.stop: subscriber.close raised: %s",
                    exc,
                )
            self._subscriber = None
        self._dispatcher = None
        self._loop = None


__all__ = ["PubSubIngressTransport"]
