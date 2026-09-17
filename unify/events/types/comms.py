"""Payload model for Comms events."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class CommsPayload(BaseModel):
    """Payload for Comms events (chat messages and related deliveries).

    Comms events have varied shapes depending on subtype. The payload_cls
    field on the Event identifies the
    specific subtype. This model captures common fields and allows extras.
    """

    model_config = ConfigDict(extra="allow")

    # Common fields across most Comms events
    content: Optional[str] = Field(
        default=None,
        description="Message content if applicable",
    )
    attachments: Optional[list[str]] = Field(
        default=None,
        description="Workspace paths of files sent with the message",
    )
    timestamp: Optional[datetime] = Field(
        default=None,
        description="Event timestamp",
    )
