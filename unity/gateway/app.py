"""FastAPI aggregator for ``unity.gateway`` channels.

Mirrors the channel-mounting topology from
``communication/main.py`` for the channels migrated in Phase B:

  social, phone, gmail, outlook, email, whatsapp, teams,
  sharepoint, unillm, discord, slack

Mount points and auth shapes are byte-for-byte identical to the
source so existing callers (Unity admin clients, the Twilio /
Microsoft / Discord webhooks, SDK consumers hitting unillm) see no
wire-level change when traffic is routed through this app.

Plugin composition
==================

The OSS path imports ``unity.gateway.app:app`` (or runs
``python -m unity.gateway``) and gets a stock app with the 10
channels mounted under the dependency shape the channels declared.

The deployed-SaaS path constructs its own app via
``create_app(extra_routers=..., extra_setup_hooks=...,
extra_lifespan_hooks=...)`` and mounts private SaaS pieces
(K8s job control routers, Prometheus instrumentation, GCP
client warm-up) on top of the same factory.

Plugins do NOT alter the wire behaviour of the built-in channels;
they only add new routes and new startup/shutdown side effects.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Callable, Sequence

from fastapi import FastAPI
from fastapi.params import Depends

from unity.gateway.channels.discord import router as discord_router
from unity.gateway.channels.drive import router as drive_router
from unity.gateway.channels.email import router as email_router
from unity.gateway.channels.gmail import router as gmail_router
from unity.gateway.channels.outlook import router as outlook_router
from unity.gateway.channels.phone import (
    auth_router as phone_auth_router,
)
from unity.gateway.channels.phone import (
    unauth_router as phone_unauth_router,
)
from unity.gateway.channels.sharepoint import router as sharepoint_router
from unity.gateway.channels.slack import auth_router as slack_auth_router
from unity.gateway.channels.social import router as social_router
from unity.gateway.channels.teams import router as teams_router
from unity.gateway.channels.unillm import router as unillm_router
from unity.gateway.channels.whatsapp import (
    auth_router as whatsapp_auth_router,
)
from unity.gateway.channels.whatsapp import (
    unauth_router as whatsapp_unauth_router,
)
from unity.gateway.common.auth import admin_auth_dependency
from unity.gateway.context import GatewayContext, create_default_gateway_context
from unity.gateway.adapters import (
    google_router,
    internal_router,
    microsoft_router,
    slack_adapter_router,
    twilio_router,
)

logger = logging.getLogger("unity.gateway.app")


# ---------------------------------------------------------------------------
# Plugin types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtraRouter:
    """One router to mount on the gateway app at a given prefix.

    Plugins declare the dependency shape per router (admin-auth,
    no-auth, custom) so the factory does not have to guess. ``router``
    is a FastAPI ``APIRouter``; ``dependencies`` is the same list-of-
    ``Depends`` shape ``app.include_router(..., dependencies=...)``
    accepts.
    """

    router: object
    prefix: str
    dependencies: Sequence[Depends] = field(default_factory=list)
    tags: Sequence[str] | None = None


SetupHook = Callable[[], None]
"""Sync callable run at lifespan startup via ``run_in_executor``.

Designed for SaaS warm-up paths that look like
``setup_kubernetes_client()`` -- blocking client construction that
the caller does not want to block the event loop on but does want to
fire-and-forget at boot time.
"""


LifespanHook = Callable[[FastAPI], AbstractAsyncContextManager]
"""Async context manager entered at startup and exited at shutdown.

Lets plugins manage long-lived background tasks (e.g. a periodic
maintenance loop) with normal try/finally cleanup semantics.
"""


# ---------------------------------------------------------------------------
# Built-in (OSS) lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _builtin_lifespan(app: FastAPI):
    """Discord pool sync + health check loop.

    Best-effort: a Discord/Orchestra outage at startup logs but does
    not prevent the app from serving the other channels.
    """
    health_task: asyncio.Task | None = None
    try:
        from unity.gateway.channels.discord.bot_manager import (
            start_health_check_loop,
            sync_from_orchestra,
        )

        try:
            await sync_from_orchestra()
        except Exception:
            logger.exception("Discord pool sync failed at startup")

        health_task = asyncio.create_task(start_health_check_loop())
        yield
    finally:
        if health_task is not None:
            health_task.cancel()


def _compose_lifespan(
    extra_setup_hooks: Sequence[SetupHook],
    extra_lifespan_hooks: Sequence[LifespanHook],
):
    """Build a single lifespan that runs the built-in + all plugin hooks.

    Order at startup:
      1. ``extra_setup_hooks`` scheduled via ``loop.run_in_executor``
         (fire-and-forget, matches the legacy ``communication/main.py``
         shape for ``setup_kubernetes_client`` /
         ``_get_pubsub_clients``).
      2. Built-in lifespan enters (Discord pool sync + health task).
      3. Each ``extra_lifespan_hooks`` context manager enters in
         declaration order.

    Order at shutdown is the reverse. Failures inside any plugin
    setup hook or lifespan hook surface immediately -- they're the
    plugin author's contract to handle gracefully if best-effort
    semantics are desired (see how the built-in Discord sync wraps
    its own try/except above).
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_event_loop()
        for setup_hook in extra_setup_hooks:
            loop.run_in_executor(None, setup_hook)

        async with _builtin_lifespan(app):
            async with AsyncExitStack() as stack:
                for lifespan_hook in extra_lifespan_hooks:
                    await stack.enter_async_context(lifespan_hook(app))
                yield

    return lifespan


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_app(
    *,
    gateway_context: GatewayContext | None = None,
    extra_routers: Sequence[ExtraRouter] | None = None,
    extra_setup_hooks: Sequence[SetupHook] | None = None,
    extra_lifespan_hooks: Sequence[LifespanHook] | None = None,
) -> FastAPI:
    """Construct the gateway FastAPI app.

    All three plugin parameters default to empty, in which case the
    returned app is byte-for-byte identical to the pre-plugin app
    (the OSS path). The deployed-SaaS path uses these parameters to
    mount private routers (e.g. K8s job control) and to warm up GCP
    clients at boot time.

    Factory rather than module-level singleton so callers can build
    multiple apps (tests, alternative compositions) and so importing
    this module has no side effects beyond the imports.
    """
    extra_routers = list(extra_routers or ())
    extra_setup_hooks = list(extra_setup_hooks or ())
    extra_lifespan_hooks = list(extra_lifespan_hooks or ())
    gateway_context = gateway_context or create_default_gateway_context()

    app = FastAPI(
        title="unity.gateway",
        description="Open-source communication channels for Unity.",
        lifespan=_compose_lifespan(extra_setup_hooks, extra_lifespan_hooks),
    )
    app.state.gateway_context = gateway_context

    # Built-in admin-authed channels.
    app.include_router(
        social_router,
        prefix="/social",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        phone_auth_router,
        prefix="/phone",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        gmail_router,
        prefix="/gmail",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        outlook_router,
        prefix="/outlook",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        email_router,
        prefix="/email",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        whatsapp_auth_router,
        prefix="/whatsapp",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        teams_router,
        prefix="/teams",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        sharepoint_router,
        prefix="/sharepoint",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        drive_router,
        prefix="/drive",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        discord_router,
        prefix="/discord",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        slack_auth_router,
        prefix="/slack",
        dependencies=admin_auth_dependency,
    )
    app.include_router(
        internal_router,
        dependencies=admin_auth_dependency,
        tags=["internal-adapters"],
    )

    # Built-in unauth channels (webhooks from third-parties that can't carry our bearer).
    app.include_router(phone_unauth_router, prefix="/phone")
    app.include_router(whatsapp_unauth_router, prefix="/whatsapp")
    app.include_router(google_router, tags=["google-adapters"])
    app.include_router(microsoft_router, tags=["microsoft-adapters"])
    app.include_router(slack_adapter_router, tags=["slack-adapters"])
    app.include_router(twilio_router, tags=["twilio-adapters"])

    # Built-in user-API-key authed channel (auth is enforced inside the route).
    app.include_router(unillm_router, prefix="/unillm")

    # Plugin routers from the caller, mounted with their declared
    # dependencies. Mounted after the built-ins so a plugin can layer
    # additional routes on existing prefixes without disturbing the
    # built-in routes.
    for extra in extra_routers:
        kwargs: dict = {"prefix": extra.prefix}
        if extra.dependencies:
            kwargs["dependencies"] = list(extra.dependencies)
        if extra.tags:
            kwargs["tags"] = list(extra.tags)
        app.include_router(extra.router, **kwargs)

    @app.get("/", include_in_schema=False)
    async def read_root() -> dict:
        return {"message": "success!"}

    @app.get("/health", include_in_schema=False)
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/features", include_in_schema=False)
    async def features() -> dict:
        """Per-channel availability for this deployment.

        Reports whether each contact channel has the provider credentials it
        needs to be provisioned/used, so upstream services (Orchestra → Console)
        can gate the corresponding UI instead of letting a user provision a
        channel that would fail at runtime. Deployment-level signal only: it
        reflects configured credentials, not per-assistant connection state, and
        carries no secrets (booleans only, no auth required like ``/health``).
        """

        def configured(*names: str) -> bool:
            return all(bool(os.environ.get(name, "").strip()) for name in names)

        twilio = configured("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN")
        twilio_wa = configured("TWILIO_WA_ACCOUNT_SID", "TWILIO_WA_AUTH_TOKEN")
        return {
            # Assistant phone (Twilio number provisioning + SMS/voice webhooks).
            "phone": twilio,
            # Assistant WhatsApp (Twilio WhatsApp sender).
            "whatsapp": twilio_wa,
            # Assistant Discord (interaction verification key; bot pool lives in
            # Orchestra, but without this key the deployment can't run Discord).
            "discord": configured("DISCORD_PUBLIC_KEY"),
            # Slack Events ingress.
            "slack": configured("SLACK_SIGNING_SECRET"),
            # User-side phone / WhatsApp verification codes (Twilio social verify).
            "social_verify_phone": twilio,
            "social_verify_whatsapp": twilio_wa,
        }

    return app


# Module-level app for ``uvicorn unity.gateway.app:app`` and the
# ``python -m unity.gateway`` launcher. Always stock (no plugins).
app = create_app()


# Backward-compatible re-export of the built-in lifespan as ``lifespan``
# for callers that imported it from a previous revision.
lifespan = _builtin_lifespan


__all__ = [
    "ExtraRouter",
    "LifespanHook",
    "SetupHook",
    "app",
    "create_app",
    "lifespan",
]
