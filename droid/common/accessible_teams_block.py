"""Render shared-team membership guidance for runtime prompts."""

from __future__ import annotations

from droid.session_details import TeamSummary

ACCESSIBLE_TEAMS_MAX_DESCRIPTION_LENGTH = 1000
ACCESSIBLE_TEAMS_HEADER = "Accessible shared teams"
_NO_SHARED_TEAMS_TEXT = (
    "No shared teams are currently available. Use personal memory unless the "
    "user explicitly asks you to create or join a shared team."
)
_INTRO_TEXT = (
    "You are a member of the following shared teams. Each team's description "
    "names the team, domain, or scope of work it exists for; use it to decide "
    "whether a write belongs in that team versus personal memory."
)
_PERSONAL_BULLET = (
    "- personal: your private memory. This is the privacy floor; content here "
    "is visible only to you."
)
_ROUTING_RULES = """Routing rules:
- Use personal memory for private notes, ambiguous ownership, or anything that should not be visible to collaborators.
- Use a shared team only when the content clearly belongs to that team's named team, domain, or scope of work.
- If the user asks you to write something more broadly than personal memory and the right team is unclear, ask a brief clarifying question before writing.
- Never invent a shared team destination. Use only personal memory or one of the listed team ids.
- ``team:<id>`` tokens route memory and tasks only. Contact-addressed comms (``send_unify_message``, SMS, email, and similar) require an integer contact id; never pass a team token as ``contact_id``."""


def build_accessible_teams_block(team_summaries: list[TeamSummary]) -> str:
    """Render the shared-team guidance block for system prompts."""

    lines = [
        ACCESSIBLE_TEAMS_HEADER,
        "",
        _INTRO_TEXT,
        "",
        _PERSONAL_BULLET,
    ]
    if team_summaries:
        for summary in sorted(team_summaries, key=lambda item: item.team_id):
            description = _bounded_description(summary.description)
            lines.append(
                f'- team:{summary.team_id} "{summary.name}" - {description}',
            )
    else:
        lines.append(f"- shared: {_NO_SHARED_TEAMS_TEXT}")
    lines.extend(["", _ROUTING_RULES])
    return "\n".join(lines)


def _bounded_description(description: str) -> str:
    if len(description) <= ACCESSIBLE_TEAMS_MAX_DESCRIPTION_LENGTH:
        return description
    return f"{description[:ACCESSIBLE_TEAMS_MAX_DESCRIPTION_LENGTH - 3]}..."
