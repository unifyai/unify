"""Inbound provider and internal adapter routes for ``unity.gateway``."""

from unity.gateway.adapters.google import router as google_router
from unity.gateway.adapters.internal import router as internal_router
from unity.gateway.adapters.microsoft import router as microsoft_router
from unity.gateway.adapters.slack import router as slack_adapter_router
from unity.gateway.adapters.twilio import router as twilio_router

__all__ = [
    "google_router",
    "internal_router",
    "microsoft_router",
    "slack_adapter_router",
    "twilio_router",
]
