"""The roster the group-call etiquette is gated on.

``other_call_participant_names`` answers one question: is the assistant in a room
where a turn might belong to somebody else? Two or more names means yes, and the
slow brain gets the group-call section; fewer means the call is effectively 1:1
and must keep answering everything, as telephony always has.

The two shapes it reads are not interchangeable. A Unify Meet carries the org
roster, where members are tagged ``human`` or ``assistant`` and named by
``display_name``. A browser meet carries the meeting platform's own roster, keyed
``name``, with no marking of which entries are bots -- so there the assistant can
only exclude itself.
"""

from __future__ import annotations

import pytest

from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unify.session_details import SESSION_DETAILS

_OWN_FIRST, _OWN_LAST = "Lila", "Down"
_OWN_NAME = f"{_OWN_FIRST} {_OWN_LAST}"


@pytest.fixture
def call_manager(monkeypatch) -> LivekitCallManager:
    # ``name`` is derived from the two parts, so it is not settable directly.
    monkeypatch.setattr(SESSION_DETAILS.assistant, "first_name", _OWN_FIRST)
    monkeypatch.setattr(SESSION_DETAILS.assistant, "surname", _OWN_LAST)
    return LivekitCallManager(
        CallConfig(
            assistant_id="42",
            user_id="user-1",
            assistant_bio="bio",
            assistant_number="+15555550000",
            voice_provider="elevenlabs",
            voice_id="voice-1",
            assistant_name=_OWN_NAME,
            job_name="job-1",
        ),
        event_broker=None,
    )


def _meet(cm: LivekitCallManager, channel: str, names: list[str]) -> None:
    cm._call_channel = channel
    cm.meet_participants = [{"name": n} for n in names]


def _unify_meet(cm: LivekitCallManager, members: list[dict]) -> None:
    cm._call_channel = "unify_meet"
    cm.unify_meet_participants = members


class TestOffCallAndOneToOne:
    def test_no_live_call_names_nobody(self, call_manager):
        assert call_manager.other_call_participant_names == []

    @pytest.mark.parametrize("channel", ["phone_call", "whatsapp_call"])
    def test_telephony_names_nobody(self, call_manager, channel):
        """Not a gap: a phone call carries exactly one other person, so every
        turn on it is addressed to the assistant and the group rules must not
        engage."""
        call_manager._call_channel = channel
        call_manager.meet_participants = [{"name": "Ada"}]
        assert call_manager.other_call_participant_names == []


class TestBrowserMeetRoster:
    def test_the_assistant_is_not_one_of_the_others(self, call_manager):
        """Counting itself would make a 1:1 meet look like a group of two."""
        _meet(call_manager, "google_meet", [_OWN_NAME, "Ada"])
        assert call_manager.other_call_participant_names == ["Ada"]

    def test_the_name_the_bot_joined_under_wins_over_the_session_name(
        self,
        call_manager,
    ):
        """The two differ whenever a caller passes a name, or none is set.

        Subtracting the session name in that case leaves the bot in its own
        roster, which turns a 1:1 meet into an apparent group of two and stands
        the assistant down on a call where every turn was its own.
        """
        call_manager.meet_display_name = "Acme Notetaker"
        _meet(call_manager, "google_meet", ["Acme Notetaker", "Ada"])
        assert call_manager.other_call_participant_names == ["Ada"]

    def test_a_group_is_reported_in_full(self, call_manager):
        _meet(call_manager, "teams_meet", ["Ada", _OWN_NAME, "Bo"])
        assert call_manager.other_call_participant_names == ["Ada", "Bo"]

    def test_blank_and_missing_names_are_dropped(self, call_manager):
        """A roster crosses a wire; an unnamed entry must not become a member."""
        call_manager._call_channel = "google_meet"
        call_manager.meet_participants = [
            {"name": "Ada"},
            {"name": "   "},
            {"name": None},
            {},
        ]
        assert call_manager.other_call_participant_names == ["Ada"]


class TestUnifyMeetRoster:
    def test_only_humans_count_as_other_people(self, call_manager):
        """Peer assistants have their own etiquette; mixing them in here would
        make a 1:1 call with one AI teammate present read as a group."""
        _unify_meet(
            call_manager,
            [
                {"kind": "human", "display_name": "Ada"},
                {"kind": "assistant", "display_name": "A-DA"},
            ],
        )
        assert call_manager.other_call_participant_names == ["Ada"]

    def test_several_humans_are_a_group(self, call_manager):
        _unify_meet(
            call_manager,
            [
                {"kind": "human", "display_name": "Ada"},
                {"kind": "human", "display_name": "Bo"},
            ],
        )
        assert call_manager.other_call_participant_names == ["Ada", "Bo"]

    def test_a_nameless_member_is_dropped(self, call_manager):
        _unify_meet(
            call_manager,
            [
                {"kind": "human", "display_name": "Ada"},
                {"kind": "human", "display_name": ""},
                {"kind": "human"},
            ],
        )
        assert call_manager.other_call_participant_names == ["Ada"]


class TestRosterFollowsTheLiveCall:
    def test_a_browser_meet_does_not_read_the_org_roster(self, call_manager):
        """The two shapes use different keys, so crossing them reads as empty
        rather than wrong -- which would silently drop the etiquette."""
        call_manager._call_channel = "google_meet"
        call_manager.unify_meet_participants = [
            {"kind": "human", "display_name": "Ada"},
            {"kind": "human", "display_name": "Bo"},
        ]
        assert call_manager.other_call_participant_names == []

    @pytest.mark.asyncio
    async def test_the_roster_is_cleared_when_the_call_ends(self, call_manager):
        """A stale roster would put the next call on the wrong rules -- a 1:1
        phone call inheriting a meet's group of three."""
        _meet(call_manager, "google_meet", ["Ada", "Bo"])
        assert len(call_manager.other_call_participant_names) == 2

        await call_manager.cleanup_call_proc()
        assert call_manager.meet_participants == []
        assert call_manager.other_call_participant_names == []
