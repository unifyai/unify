from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from unify.common.prompt_helpers import PromptParts
from unify.conversation_manager.prompt_builders import build_system_prompt
from unify.session_details import SESSION_DETAILS

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager
    from unify.conversation_manager.domains.renderer import SnapshotState


@dataclass(frozen=True)
class BrainSpec:
    """
    Fully materialized inputs needed for a single Main CM Brain run.

    This is intentionally a small, explicit data structure so we can refactor
    how the brain is executed (plain generate vs async tool loop) without
    tangling prompt/model construction with execution and side effects.
    """

    system_prompt: PromptParts
    state_prompt: str

    def state_message(self) -> dict:
        # Mark this as a state snapshot so the async tool loop can treat it as
        # transient state (e.g., keep only the latest snapshot when generating).
        return {
            "role": "user",
            "content": self.state_prompt,
            "_cm_state_snapshot": True,
        }


def build_brain_spec(
    cm: "ConversationManager",
    snapshot_state: "SnapshotState",
) -> BrainSpec:
    """
    Build the prompt inputs for a single Main CM Brain run.

    The returned spec is *pure* (no side effects).

    Parameters
    ----------
    cm : ConversationManager
        The conversation manager instance.
    snapshot_state : SnapshotState
        Pre-rendered conversation state (caller computes this once and reuses it
        for both the BrainSpec and incremental-diff tracking).
    """
    prompt = snapshot_state.full_render

    boss_contact_id = SESSION_DETAILS.boss_contact_id
    boss_contact = cm.contact_index.get_contact(boss_contact_id) or {}

    assistant = SESSION_DETAILS.assistant
    bio_parts: list[str] = []
    job_title = assistant.job_title.strip()
    if job_title:
        bio_parts.append(f"Role / specialization: {job_title}.")
    if assistant.about:
        bio_parts.append(assistant.about)

    system_prompt = build_system_prompt(
        bio="\n".join(bio_parts),
        contact_id=boss_contact_id,
        first_name=boss_contact.get("first_name") or "",
        surname=boss_contact.get("surname") or "",
        phone_number=boss_contact.get("phone_number"),
        email_address=boss_contact.get("email_address"),
        assistant_has_phone=bool(assistant.number),
        assistant_has_email=bool(assistant.email),
    )

    # Validate we can JSON-encode state prompt early (helps catch accidental objects)
    json.dumps({"state_prompt": prompt})

    return BrainSpec(system_prompt=system_prompt, state_prompt=prompt)
