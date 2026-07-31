"""A shared screen in a meeting has to say whose it is.

Attribution is the reason frames come from Recall rather than from scraping the
meeting UI: the platform names the presenter, and without that name the model can
only describe "a screen" in a room full of people. Both brains render their own
label, so both are pinned here.
"""

from datetime import datetime, timezone

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
