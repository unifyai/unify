"""FastAPI aggregator for ``unity.gateway`` channels.

Mirrors the channel-mounting topology from
``communication/main.py`` for the 10 channels migrated in Phase B:

  social, phone, gmail, outlook, email, whatsapp, teams,
  sharepoint, unillm, discord

Mount points and auth shapes are byte-for-byte identical to the
source so existing callers (Unity admin clients, the Twilio /
Microsoft / Discord webhooks, SDK consumers hitting unillm) see no
wire-level change when traffic is eventually routed through this
app.

Out of scope for the open-source aggregator (and therefore not
mounted here):

* The private ``infra/`` routers from ``communication/main.py``
  (Kubernetes Job control + tunnel + VM-self endpoints). Those stay
  in the closed-source ``communication/`` package; the SaaS deploy
  composes them on top of this aggregator at runtime.
* The Prometheus / OTel metrics setup (also SaaS-deploy concern).
* The ``adapters/main.py`` service (4866 LOC of inbound webhooks):
  separate aggregator, separate Phase.

Lifespan hooks: only the open-source ones run from here. Right now
that's just the Discord pool sync + health-check loop (own
implementation lives in ``unity.gateway.channels.discord.bot_manager``).
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from unity.gateway.channels.discord import router as discord_router
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

logger = logging.getLogger("unity.gateway.app")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open-source lifespan hooks only.

    Currently:
    * Discord pool sync at startup + health-check loop while running.
      The sync is best-effort -- a Discord/Orchestra outage at startup
      logs but doesn't prevent the app from serving the other channels.
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


# ---------------------------------------------------------------------------
# App + router mount table
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Construct the gateway FastAPI app.

    Factory rather than module-level singleton so callers can build
    multiple apps (e.g. tests that want different lifespans) and so
    importing this module has no side effects beyond the imports.
    """
    app = FastAPI(
        title="unity.gateway",
        description="Open-source communication channels for Unity.",
        lifespan=lifespan,
    )

    # Admin-authed channels (most of them).
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
        discord_router,
        prefix="/discord",
        dependencies=admin_auth_dependency,
    )

    # Unauth channels (webhooks from Twilio that can't carry our bearer).
    app.include_router(phone_unauth_router, prefix="/phone")
    app.include_router(whatsapp_unauth_router, prefix="/whatsapp")

    # User-API-key authed channel (auth is enforced inside the route).
    app.include_router(unillm_router, prefix="/unillm")

    @app.get("/", include_in_schema=False)
    async def read_root() -> dict:
        return {"message": "success!"}

    @app.get("/health", include_in_schema=False)
    async def health() -> dict:
        return {"status": "ok"}

    return app


# Module-level app for ``uvicorn unity.gateway.app:app`` and the
# ``python -m unity.gateway`` launcher.
app = create_app()


__all__ = ["app", "create_app", "lifespan"]
