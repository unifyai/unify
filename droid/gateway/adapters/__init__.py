"""Inbound provider and internal adapter routes for ``droid.gateway``."""

from droid.gateway.adapters.google import router as google_router
from droid.gateway.adapters.internal import router as internal_router
from droid.gateway.adapters.microsoft import router as microsoft_router
from droid.gateway.adapters.slack import router as slack_adapter_router
from droid.gateway.adapters.twilio import router as twilio_router

__all__ = [
    "google_router",
    "internal_router",
    "microsoft_router",
    "slack_adapter_router",
    "twilio_router",
]
