"""Whose desktop a call puts on its stage, and for how long.

The assistant's desktop has two audiences that were being answered by one
boolean. Somebody watching it -- from a call, or from the standalone Desktop tab
with no call in sight -- is what the brain needs to know, and it outlives any one
call. What a *call* mounts on every participant's screen is a different claim,
and it must not outlive the call that made it.

Collapsing the two left a viewer from an ended call, or a Desktop tab open
beside the call, deciding what a room full of people saw -- and, because the
broadcast fired only when the shared boolean moved, left no way to take it back
down. These pin the two apart.
"""

from __future__ import annotations

from unify.conversation_manager.conversation_manager import ConversationManager


class _Viewers:
    """The viewer bookkeeping under test, without standing up a CM.

    The methods are taken off the class unbound, so what runs here is the
    production implementation rather than a restatement of it.
    """

    def __init__(self, *keys: str) -> None:
        self._assistant_screen_share_viewers = set(keys)
        self.assistant_screen_share_active = bool(keys)

    # Re-wrapped: reading it off the class yields the plain function, which
    # rebinding here would turn back into a method and hand it ``self``.
    assistant_screen_share_viewer_key = staticmethod(
        ConversationManager.assistant_screen_share_viewer_key,
    )
    watched_from_call = ConversationManager.assistant_desktop_watched_from_call
    drop_stale = ConversationManager.drop_stale_call_screen_share_viewers
    note = ConversationManager.note_assistant_screen_share_viewer
    drop_source = ConversationManager.drop_assistant_screen_share_viewers


def test_desktop_tab_alone_stages_nothing() -> None:
    """A tab open beside the call is one person watching, not a presentation."""
    viewers = _Viewers("desktop_pane:u1")

    assert viewers.assistant_screen_share_active is True
    assert viewers.watched_from_call("c1") is False


def test_a_calls_own_viewer_stages_it() -> None:
    viewers = _Viewers("call:c1:room")

    assert viewers.watched_from_call("c1") is True


def test_another_calls_viewer_does_not_stage_this_one() -> None:
    """The bug, stated as the question that was never being asked."""
    viewers = _Viewers("call:c1:room")

    assert viewers.assistant_screen_share_active is True
    assert viewers.watched_from_call("c2") is False


def test_new_call_drops_viewers_of_calls_that_ended() -> None:
    """Nothing else can: the stop that would match names a departed call.

    Both resets that would otherwise have caught it are skippable in the same
    sequence -- the call-start one when a dispatch lands while its predecessor is
    still winding down, the call-end one when the stale-session guard drops the
    departed call's ``Ended`` event.
    """
    viewers = _Viewers("call:c1:room", "call:c2:room", "desktop_pane:u1")

    viewers.drop_stale("c2")

    assert viewers._assistant_screen_share_viewers == {
        "call:c2:room",
        "desktop_pane:u1",
    }
    assert viewers.assistant_screen_share_active is True


def test_dropping_stale_viewers_spares_the_desktop_tab() -> None:
    """A Desktop tab is nobody's call to close but its own."""
    viewers = _Viewers("call:c1:room", "desktop_pane:u1")

    viewers.drop_stale("c2")

    assert viewers._assistant_screen_share_viewers == {"desktop_pane:u1"}
    assert viewers.assistant_screen_share_active is True
    assert viewers.watched_from_call("c2") is False


def test_dropping_stale_viewers_keeps_a_share_started_early() -> None:
    """The Console can start a share before the runtime's own started event."""
    viewers = _Viewers("call:c2:room")

    viewers.drop_stale("c2")

    assert viewers._assistant_screen_share_viewers == {"call:c2:room"}


def test_no_call_id_drops_nothing() -> None:
    """A call with no session id says nothing about anyone else's viewers."""
    viewers = _Viewers("call:c1:room", "desktop_pane:u1")

    viewers.drop_stale("")

    assert viewers._assistant_screen_share_viewers == {
        "call:c1:room",
        "desktop_pane:u1",
    }


def test_last_call_viewer_leaving_clears_the_stage_under_an_open_tab() -> None:
    """The stop that used to do nothing.

    With a Desktop tab open the shared flag never moves, so an edge-triggered
    broadcast had no edge to fire on and the desktop stayed on every
    participant's stage for the rest of the call.
    """
    viewers = _Viewers("call:c1:room", "desktop_pane:u1")

    still_watched = viewers.note(
        user_id="u1",
        source="call:c1",
        watching=False,
    )

    assert still_watched is True
    assert viewers.watched_from_call("c1") is False


def test_call_boundary_still_leaves_the_desktop_tab_watching() -> None:
    """The distinction the viewer set was introduced to make, unchanged."""
    viewers = _Viewers("call:c1:room", "desktop_pane:u1")

    assert viewers.drop_source("call") is True
    assert viewers._assistant_screen_share_viewers == {"desktop_pane:u1"}


def test_anyone_on_the_call_can_put_a_desktop_up() -> None:
    """A call's share is one switch, so it does not matter who reaches for it."""
    viewers = _Viewers()

    viewers.note(user_id="u2", source="call:c1", watching=True)

    assert viewers.watched_from_call("c1") is True
    assert viewers._assistant_screen_share_viewers == {"call:c1:room"}


def test_one_participant_takes_down_what_another_put_up() -> None:
    """The whole reason a call keys its viewer to the room and not the person.

    Keyed per person, this stop would discard a key ``u2`` never held: the share
    would stay up and the button would do nothing -- the same dead control as
    before, reached by letting everyone press it.
    """
    viewers = _Viewers()
    viewers.note(user_id="u1", source="call:c1", watching=True)

    viewers.note(user_id="u2", source="call:c1", watching=False)

    assert viewers.watched_from_call("c1") is False
    assert viewers._assistant_screen_share_viewers == set()


def test_two_people_pressing_start_is_still_one_share() -> None:
    """Idempotent, so the second press is not a second thing to turn off."""
    viewers = _Viewers()

    viewers.note(user_id="u1", source="call:c1", watching=True)
    viewers.note(user_id="u2", source="call:c1", watching=True)
    viewers.note(user_id="u1", source="call:c1", watching=False)

    assert viewers.watched_from_call("c1") is False


def test_a_desktop_tab_is_still_one_viewer_per_person() -> None:
    """Unchanged where the tally is the point: two panes, two viewers."""
    viewers = _Viewers()

    viewers.note(user_id="u1", source="desktop_pane", watching=True)
    viewers.note(user_id="u2", source="desktop_pane", watching=True)
    still_watched = viewers.note(user_id="u1", source="desktop_pane", watching=False)

    assert still_watched is True
    assert viewers._assistant_screen_share_viewers == {"desktop_pane:u2"}
