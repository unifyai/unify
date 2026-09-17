"""Task trigger definitions for communication-event automation."""

from __future__ import annotations

import json
from typing import Annotated, Any, List, Literal, Optional

from pydantic import BaseModel, BeforeValidator, Field

from unify.conversation_manager.cm_types import Medium


class CommunicationTrigger(BaseModel):
    """Inbound communication event that should start the task."""

    kind: Literal["communication"] = Field(
        default="communication",
        description="Discriminator for communication-trigger tasks.",
    )
    medium: Medium = Field(
        ...,
        description="Which channel must be observed",
    )
    from_contact_ids: Optional[List[int]] = Field(
        default=None,
        description=(
            "Only messages from these contacts count. "
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


def _coerce_trigger_dict(data: Any) -> Any:
    """Normalize stored trigger payloads into a discriminated dict."""

    if isinstance(data, str):
        data = json.loads(data)
    if isinstance(data, dict) and "medium" in data and "kind" not in data:
        return {**data, "kind": "communication"}
    return data


TaskTrigger = Annotated[
    CommunicationTrigger,
    BeforeValidator(_coerce_trigger_dict),
]

Trigger = CommunicationTrigger


def parse_task_trigger(value: Any) -> CommunicationTrigger | None:
    """Parse one authored trigger payload."""

    if value is None:
        return None
    if isinstance(value, CommunicationTrigger):
        return value
    coerced = _coerce_trigger_dict(value)
    if isinstance(coerced, dict):
        return CommunicationTrigger.model_validate(coerced)
    raise TypeError(f"Unsupported trigger payload type: {type(value)!r}")
