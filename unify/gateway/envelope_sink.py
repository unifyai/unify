"""Delivery backends for normalized gateway envelopes."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import httpx

from unify.gateway.ingress import EnvelopeDispatcher
from unify.gateway.outbound import OutboundTransport
from unify.settings import SETTINGS


@runtime_checkable
class EnvelopeSink(Protocol):
    """Accept normalized inbound envelopes for a target assistant."""

    async def publish(
        self,
        assistant_id: str,
        envelope: dict,
        *,
        thread: str = "",
    ) -> str:
        """Deliver ``envelope`` and return a backend-specific message id."""


class MissingEnvelopeSink:
    """Sink that makes missing delivery configuration fail loudly."""

    async def publish(
        self,
        assistant_id: str,
        envelope: dict,
        *,
        thread: str = "",
    ) -> str:
        del assistant_id, envelope, thread
        raise RuntimeError(
            "No gateway envelope sink is configured. Configure a local sink "
            "or a Pub/Sub-backed sink before mounting adapter routes.",
        )


class DirectEnvelopeSink:
    """Deliver envelopes directly to a ``CommsManager``-style dispatcher."""

    def __init__(self, dispatcher: EnvelopeDispatcher) -> None:
        self._dispatcher = dispatcher

    async def publish(
        self,
        assistant_id: str,
        envelope: dict,
        *,
        thread: str = "",
    ) -> str:
        del assistant_id, thread
        await self._dispatcher(envelope, source_topic="", ack=None, nack=None)
        return ""


@dataclass(frozen=True)
class HttpEnvelopeSink:
    """Deliver envelopes to another HTTP ingress endpoint."""

    base_url: str
    path: str = "/local/comms/envelope"
    timeout: float = 10.0

    async def publish(
        self,
        assistant_id: str,
        envelope: dict,
        *,
        thread: str = "",
    ) -> str:
        del assistant_id, thread
        payload = dict(envelope)
        if "publish_timestamp" not in payload:
            payload["publish_timestamp"] = time.time()
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url.rstrip('/')}{self.path}",
                json=payload,
            )
        response.raise_for_status()
        return ""


@dataclass(frozen=True)
class OutboundTransportEnvelopeSink:
    """Publish inbound envelopes through an ``OutboundTransport`` backend."""

    transport: OutboundTransport
    project_env_suffix: str = ""

    def _topic_name(self, assistant_id: str) -> str:
        return f"unity-{assistant_id}{self.project_env_suffix}"

    async def publish(
        self,
        assistant_id: str,
        envelope: dict,
        *,
        thread: str = "",
    ) -> str:
        payload = dict(envelope)
        logical_thread = thread or str(payload.get("thread") or "")
        if "publish_timestamp" not in payload:
            payload["publish_timestamp"] = time.time()
        message = json.dumps(payload).encode("utf-8")
        return await asyncio.to_thread(
            self.transport.publish,
            self._topic_name(assistant_id),
            message,
            thread=logical_thread,
        )


def default_topic_suffix() -> str:
    """Return the deployment suffix used by per-assistant topics."""

    return (
        "-staging" if getattr(SETTINGS, "DEPLOY_ENV", "production") == "staging" else ""
    )


__all__ = [
    "DirectEnvelopeSink",
    "EnvelopeSink",
    "HttpEnvelopeSink",
    "MissingEnvelopeSink",
    "OutboundTransportEnvelopeSink",
    "default_topic_suffix",
]
