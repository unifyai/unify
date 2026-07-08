"""Render shared-team membership guidance for runtime prompts."""

from __future__ import annotations

from unify.session_details import SESSION_DETAILS, TeamSummary

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
_TEAM_OWNED_INTRO_TEXT = (
    "You are a team-owned assistant: your owning team's shared root is your "
    "home memory, visible to every member of that team. You have no private "
    "personal memory. Each team's description names the team, domain, or "
    "scope of work it exists for; use it to decide where a write belongs."
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
- ``team:<id>`` tokens route memory and tasks only. Contact-addressed comms (``send_unify_message``, SMS, email, and similar) require an integer contact id; never pass a team token as ``contact_id``.
- Team group chat is the one exception: a Unify message whose context carries a ``team_id`` was posted in that team's group chat (visible to every member, like a large email CC chain). Reply in the room by passing the same integer ``team_id`` to ``send_unify_message``; omit it to reply privately in the sender's 1:1 thread."""
_TEAM_OWNED_ROUTING_RULES = """Routing rules:
- Writes default to your owning team's shared root; you have no personal destination.
- Use another team's destination only when the content clearly belongs to that team's named team, domain, or scope of work.
- If the right team for a broader write is unclear, ask a brief clarifying question before writing.
- Never invent a shared team destination. Use only the listed team ids.
- ``team:<id>`` tokens route memory and tasks only. Contact-addressed comms (``send_unify_message``, SMS, email, and similar) require an integer contact id; never pass a team token as ``contact_id``.
- Team group chat is the one exception: a Unify message whose context carries a ``team_id`` was posted in that team's group chat (visible to every member, like a large email CC chain). Reply in the room by passing the same integer ``team_id`` to ``send_unify_message``; omit it to reply privately in the sender's 1:1 thread."""


def build_accessible_teams_block(team_summaries: list[TeamSummary]) -> str:
    """Render the shared-team guidance block for system prompts."""

    owner_team_id = SESSION_DETAILS.owner_team_id
    if owner_team_id is not None:
        return _build_team_owned_block(team_summaries, owner_team_id)

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


def _build_team_owned_block(
    team_summaries: list[TeamSummary],
    owner_team_id: int,
) -> str:
    lines = [
        ACCESSIBLE_TEAMS_HEADER,
        "",
        _TEAM_OWNED_INTRO_TEXT,
        "",
    ]
    summaries_by_id = {summary.team_id: summary for summary in team_summaries}
    owner_summary = summaries_by_id.get(owner_team_id)
    if owner_summary is not None:
        description = _bounded_description(owner_summary.description)
        lines.append(
            f'- team:{owner_team_id} "{owner_summary.name}" - your owning '
            f"team and default destination. {description}",
        )
    else:
        lines.append(
            f"- team:{owner_team_id}: your owning team and default destination.",
        )
    for summary in sorted(team_summaries, key=lambda item: item.team_id):
        if summary.team_id == owner_team_id:
            continue
        description = _bounded_description(summary.description)
        lines.append(
            f'- team:{summary.team_id} "{summary.name}" - {description}',
        )
    lines.extend(["", _TEAM_OWNED_ROUTING_RULES])
    return "\n".join(lines)


def _bounded_description(description: str) -> str:
    if len(description) <= ACCESSIBLE_TEAMS_MAX_DESCRIPTION_LENGTH:
        return description
    return f"{description[:ACCESSIBLE_TEAMS_MAX_DESCRIPTION_LENGTH - 3]}..."
