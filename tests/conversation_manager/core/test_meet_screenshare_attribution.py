"""A shared screen in a meeting has to say whose it is.

Attribution is the reason frames come from Recall rather than from scraping the
meeting UI: the platform names the presenter, and without that name the model can
only describe "a screen" in a room full of people. Both brains render their own
label, so both are pinned here.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

from unify.common.prompt_helpers import PromptParts
from unify.conversation_manager.cm_types.screenshot import (
    ScreenshotEntry,
    generate_screenshot_path,
)
from unify.conversation_manager.domains.brain import BrainSpec
from unify.conversation_manager.medium_scripts.common import ScreenshotHistory


def _entry(source: str, attribution: str | None) -> ScreenshotEntry:
    return ScreenshotEntry(
        b64="ZmFrZS1qcGVn",
        utterance="what does that say?",
        timestamp=datetime(2026, 7, 30, 10, 0, tzinfo=timezone.utc),
        source=source,
        attribution=attribution,
    )


def _text_parts(content) -> str:
    return "\n".join(p["text"] for p in content if p.get("type") == "text")


def test_slow_brain_names_the_presenter() -> None:
    spec = BrainSpec(
        system_prompt=PromptParts(),
        state_prompt="state",
        screenshots=[_entry("google_meet", "Ada")],
        screenshot_paths=["Screenshots/GoogleMeet/x.jpg"],
    )
    text = _text_parts(spec.state_message()["content"])
    assert "shared by Ada" in text


def test_slow_brain_says_a_shared_screen_is_not_the_meeting_view() -> None:
    """The old wording promised participants' faces and the meeting UI.

    Frames are filtered to ``type == "screenshare"``, so that is all there is --
    and an assistant told it can see the gallery will answer questions about
    faces it has never seen.
    """
    spec = BrainSpec(
        system_prompt=PromptParts(),
        state_prompt="state",
        screenshots=[_entry("google_meet", "Ada")],
    )
    text = _text_parts(spec.state_message()["content"])
    assert "not the meeting's own gallery view" in text


def test_single_user_sources_stay_unattributed() -> None:
    """One user's own screen needs no name; appending one would read as a third party."""
    spec = BrainSpec(
        system_prompt=PromptParts(),
        state_prompt="state",
        screenshots=[_entry("user", None)],
    )
    text = _text_parts(spec.state_message()["content"])
    assert "shared by" not in text
    assert "User's Screen" in text


def test_fast_brain_names_the_presenter() -> None:
    history = ScreenshotHistory()
    entry = _entry("google_meet", "Ada")
    history.add(entry, generate_screenshot_path(entry))

    labels = [p for p in history.build_visual_context_content() if isinstance(p, str)]
    assert any("SHARED BY Ada" in label for label in labels)
    assert any("not yours" in label for label in labels)


def test_fast_brain_label_stays_fenced_when_attributed() -> None:
    """The ``===`` fencing is what separates sources in the prompt."""
    history = ScreenshotHistory()
    entry = _entry("teams_meet", "Grace")
    history.add(entry, generate_screenshot_path(entry))

    labels = [p for p in history.build_visual_context_content() if isinstance(p, str)]
    assert labels
    for label in labels:
        assert label.startswith("=== ") and label.endswith(" ===")


# ---------------------------------------------------------------------------
# Share state must follow the share, not the meeting
# ---------------------------------------------------------------------------


def _meet_state(*, google_meet: bool = True, sharing: bool) -> str:
    from unify.conversation_manager.domains.renderer import Renderer

    return Renderer.render_meet_interaction_state(
        google_meet_active=google_meet,
        meet_screen_share_active=sharing,
    )


def test_a_live_share_is_announced_as_live() -> None:
    state = _meet_state(sharing=True)
    assert "status='live'" in state
    assert "sharing their screen" in state


def test_no_share_says_so_rather_than_going_quiet() -> None:
    """Silence here was read as "the last screen I saw is still up".

    The old block was emitted for the whole meeting under a name and status that
    asserted an active visual, so the assistant kept describing a screen the
    presenter had already taken down. Saying "nobody is sharing" out loud is what
    replaces that.
    """
    state = _meet_state(sharing=False)
    assert "status='none'" in state
    assert "Nobody" in state
    assert "history" in state


def test_the_block_never_claims_a_visual_for_the_whole_meeting() -> None:
    """The regression guard: a meeting in progress is not a share in progress."""
    assert "google_meet_visual" not in _meet_state(sharing=False)
    assert "google_meet_visual" not in _meet_state(sharing=True)


def test_no_meeting_means_no_block_at_all() -> None:
    from unify.conversation_manager.domains.renderer import Renderer

    state = Renderer.render_meet_interaction_state(
        google_meet_active=False,
        teams_meet_active=False,
        meet_screen_share_active=False,
    )
    assert "meet_shared_screen" not in state


def test_unpaired_meet_frames_are_dropped_when_the_share_ends() -> None:
    """Otherwise the next turn describes a screen that has been taken down."""
    from unify.conversation_manager.conversation_manager import ConversationManager

    cm = SimpleNamespace(
        _screenshot_buffer=[
            _entry("google_meet", "Ada"),
            _entry("google_meet", "Ada")._replace(utterance=""),
            _entry("user", None)._replace(utterance=""),
        ],
    )
    # The first entry carries an utterance; the second does not.
    cm._screenshot_buffer[0] = cm._screenshot_buffer[0]._replace(
        utterance="what's that?",
    )

    drop = ConversationManager.drop_unpaired_screenshots.__get__(cm)
    assert drop("google_meet") == 1

    remaining = cm._screenshot_buffer
    assert [e.source for e in remaining] == ["google_meet", "user"]
    assert remaining[0].utterance == "what's that?"


def test_dropping_keeps_frames_tied_to_a_question() -> None:
    """A question asked about a screen stays answerable after the screen goes."""
    from unify.conversation_manager.conversation_manager import ConversationManager

    cm = SimpleNamespace(
        _screenshot_buffer=[
            _entry("google_meet", "Ada")._replace(utterance="read this"),
        ],
    )
    drop = ConversationManager.drop_unpaired_screenshots.__get__(cm)

    assert drop("google_meet") == 0
    assert len(cm._screenshot_buffer) == 1
