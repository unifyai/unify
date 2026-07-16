"""Task trigger definitions for communication and provider-event automation."""

from __future__ import annotations

import json
from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, BeforeValidator, Field

from unify.conversation_manager.cm_types import Medium

ProviderEventTriggerState = Literal["draft", "enabled", "paused"]


class CommunicationTrigger(BaseModel):
    """Inbound communication event that should start the task."""

    kind: Literal["communication"] = Field(
        default="communication",
        description="Discriminator for communication-trigger tasks.",
    )
    medium: Medium = Field(
        ...,
        description="Which channel (SMS / email / call / …) must be observed",
    )
    from_contact_ids: Optional[List[int]] = Field(
        default=None,
        description=(
            "Only messages/calls from these contacts count. "
            "Leave unset to match any sender."
        ),
    )
    omit_contact_ids: Optional[List[int]] = Field(
        default=None,
        description="Explicitly ignore these contacts (overrides from_contact_ids)",
    )
    recurring: bool = Field(
        default=False,
        description=(
            "If True the task returns to the triggerable state after completion "
            "so future events re-activate it."
        ),
    )


class ProviderEventTrigger(BaseModel):
    """Third-party provider trigger that should start the task."""

    kind: Literal["provider_event"] = Field(
        default="provider_event",
        description="Discriminator for provider-event trigger tasks.",
    )
    state: ProviderEventTriggerState = Field(
        default="draft",
        description="Provider-trigger automation state independent from task.enabled.",
    )
    connection_id: str = Field(
        ...,
        description="Exact integration connection owned by the assistant.",
    )
    backend_id: str = Field(
        ...,
        description="Trigger provider backend identifier.",
    )
    canonical_app_slug: str = Field(
        ...,
        description="Connected app slug for the chosen integration.",
    )
    provider_trigger_slug: str = Field(
        ...,
        description="Provider-native trigger identifier from the staged catalog.",
    )
    trigger_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Provider trigger configuration from the staged catalog schema.",
    )


def _coerce_trigger_dict(data: Any) -> Any:
    """Normalize Orchestra/API trigger payloads into a discriminated dict."""

    if isinstance(data, str):
        data = json.loads(data)
    if isinstance(data, dict) and "medium" in data and "kind" not in data:
        return {**data, "kind": "communication"}
    return data


TaskTrigger = Annotated[
    Union[CommunicationTrigger, ProviderEventTrigger],
    Field(discriminator="kind"),
    BeforeValidator(_coerce_trigger_dict),
]

Trigger = CommunicationTrigger


def parse_task_trigger(
    value: Any,
) -> CommunicationTrigger | ProviderEventTrigger | None:
    """Parse one authored trigger payload into the discriminated union."""

    if value is None:
        return None
    if isinstance(value, (CommunicationTrigger, ProviderEventTrigger)):
        return value
    coerced = _coerce_trigger_dict(value)
    if isinstance(coerced, dict):
        kind = coerced.get("kind")
        if kind == "provider_event":
            return ProviderEventTrigger.model_validate(coerced)
        return CommunicationTrigger.model_validate(coerced)
    raise TypeError(f"Unsupported trigger payload type: {type(value)!r}")
