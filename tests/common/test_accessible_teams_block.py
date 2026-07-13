"""Tests for the shared-team prompt guidance block."""

from unify.common.accessible_teams_block import build_accessible_teams_block
from unify.session_details import TeamSummary


def test_renders_solo_assistant_block() -> None:
    """Solo assistants still receive explicit memory routing guidance."""

    block = build_accessible_teams_block([])

    assert (
        block
        == """Accessible shared teams

You are a member of the following shared teams. Each team's description names the team, domain, or scope of work it exists for; use it to decide whether a write belongs in that team versus personal memory.

- personal: your private memory. This is the privacy floor; content here is visible only to you.
- shared: No shared teams are currently available. Use personal memory unless the user explicitly asks you to create or join a shared team.

Routing rules:
- Use personal memory for private notes, ambiguous ownership, or anything that should not be visible to collaborators.
- Use a shared team only when the content clearly belongs to that team's named team, domain, or scope of work.
- If the user asks you to write something more broadly than personal memory and the right team is unclear, ask a brief clarifying question before writing.
- Never invent a shared team destination. Use only personal memory or one of the listed team ids.
- ``team:<id>`` tokens are shared destination roots for memory, tasks, dashboards/tiles, knowledge, guidance, and other team-scoped stores. Pass them as the ``destination`` argument on those APIs (for example ``primitives.dashboards.create_dashboard(..., destination="team:<id>")``). Contact-addressed comms (``send_unify_message``, SMS, email, and similar) require an integer contact id; never pass a team token as ``contact_id``.
- Team group chat is the one exception: a Unify message whose context carries a ``team_id`` was posted in that team's group chat (visible to every member, like a large email CC chain). Reply in the room by passing the same integer ``team_id`` to ``send_unify_message``; omit it to reply privately in the sender's 1:1 thread."""
    )


def test_renders_multi_membership_in_order() -> None:
    """Shared memberships are rendered by ascending team id."""

    block = build_accessible_teams_block(
        [
            TeamSummary(
                team_id=9,
                name="Marketing",
                description="Marketing campaigns and analytics for the brand team.",
            ),
            TeamSummary(
                team_id=3,
                name="Repairs",
                description="South-East repairs patch daily operations.",
            ),
        ],
    )

    assert '- team:3 "Repairs" - South-East repairs patch daily operations.' in block
    assert (
        '- team:9 "Marketing" - Marketing campaigns and analytics for the brand team.'
        in block
    )
    assert block.index("team:3") < block.index("team:9")


def test_renders_team_owned_block_without_personal_memory() -> None:
    """Team-owned assistants get owner-team-first guidance and no personal bullet."""
    from unify.session_details import SESSION_DETAILS

    original_owner = SESSION_DETAILS.owner_team_id
    SESSION_DETAILS.owner_team_id = 3
    try:
        block = build_accessible_teams_block(
            [
                TeamSummary(
                    team_id=9,
                    name="Marketing",
                    description="Marketing campaigns.",
                ),
                TeamSummary(
                    team_id=3,
                    name="Repairs",
                    description="Repairs operations.",
                ),
            ],
        )
    finally:
        SESSION_DETAILS.owner_team_id = original_owner

    assert "You are a team-owned assistant" in block
    assert "your owning team and default destination" in block
    assert '- team:3 "Repairs"' in block
    assert '- team:9 "Marketing"' in block
    assert block.index("team:3") < block.index("team:9")
    assert "- personal:" not in block
    assert "you have no personal destination" in block
