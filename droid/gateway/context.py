"""Dependency context for gateway route handlers."""

from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi import Request

from droid.gateway.credentials import CredentialStore, EnvCredentialStore
from droid.gateway.envelope_sink import (
    EnvelopeSink,
    HttpEnvelopeSink,
    MissingEnvelopeSink,
)
from droid.gateway.public_url import PublicUrlProvider, StaticPublicUrlProvider
from droid.gateway.runtime import LocalRuntimeActivator, RuntimeActivator
from droid.gateway.scheduler import LocalScheduler, Scheduler
from droid.gateway.storage import LocalDiskStorage, Storage


@dataclass
class GatewayContext:
    """Backend dependencies shared by gateway routers."""

    credentials: CredentialStore
    storage: Storage
    envelope_sink: EnvelopeSink
    runtime_activator: RuntimeActivator
    public_url_provider: PublicUrlProvider
    scheduler: Scheduler


def default_public_url_provider() -> StaticPublicUrlProvider:
    """Build the default URL provider from process environment."""

    comms_url = (
        os.environ.get("DROID_COMMS_URL")
        or os.environ.get("COMMS_URL")
        or "http://localhost:8001"
    )
    adapters_url = (
        os.environ.get("DROID_ADAPTERS_URL")
        or os.environ.get("LOCAL_ADAPTERS_URL")
        or os.environ.get("ADAPTERS_URL")
        or comms_url
    )
    return StaticPublicUrlProvider(
        comms_base_url=comms_url,
        adapters_base_url=adapters_url,
    )


def create_default_gateway_context() -> GatewayContext:
    """Create the stock local gateway dependency context."""

    local_ingress_url = os.environ.get("DROID_GATEWAY_LOCAL_INGRESS_URL", "").strip()
    envelope_sink = (
        HttpEnvelopeSink(local_ingress_url)
        if local_ingress_url
        else MissingEnvelopeSink()
    )
    # HTTP base where another service serves the gateway storage directory
    # (Orchestra's /v0/storage/local route over the shared compose volume).
    storage_public_url = os.environ.get("DROID_GATEWAY_STORAGE_PUBLIC_URL", "").strip()
    return GatewayContext(
        credentials=EnvCredentialStore(),
        storage=LocalDiskStorage(public_base_url=storage_public_url or None),
        envelope_sink=envelope_sink,
        runtime_activator=LocalRuntimeActivator(),
        public_url_provider=default_public_url_provider(),
        scheduler=LocalScheduler(),
    )


def get_gateway_context(request: Request) -> GatewayContext:
    """FastAPI dependency returning the app's gateway context."""

    context = getattr(request.app.state, "gateway_context", None)
    if context is None:
        context = create_default_gateway_context()
        request.app.state.gateway_context = context
    return context


__all__ = [
    "GatewayContext",
    "create_default_gateway_context",
    "default_public_url_provider",
    "get_gateway_context",
]
