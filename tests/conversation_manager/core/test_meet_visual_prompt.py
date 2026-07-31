"""What the fast brain is allowed to claim about seeing a browser meeting.

The voice-agent prompt is built once when the call starts and never rebuilt, so
it cannot describe live state. It used to assert "I **can** see the meeting" and
instruct the model to confirm on request, which produced a call where the fast
brain said "Yes — I can see the meeting view" while holding no image at all and
the slow brain contradicted it four seconds later.

It also named a label the emitter had stopped producing, so even once frames
arrived the model was hunting for a string that never appeared. Both properties
are pinned here because both failed silently.
"""

from datetime import datetime, timezone

import pytest

from unify.conversation_manager.cm_types.screenshot import (
    ScreenshotEntry,
    generate_screenshot_path,
    visual_source_label,
)
from unify.conversation_manager.medium_scripts.common import ScreenshotHistory
from unify.conversation_manager.prompt_builders import build_voice_agent_prompt

MEET_CHANNELS = ("google_meet", "teams_meet")


def _prompt(channel: str) -> str:
    return build_voice_agent_prompt(
        bio="An assistant.",
        boss_first_name="Julia",
        boss_surname="Goh",
        channel=channel,
    ).flatten()


def _emitted_labels(channel: str, attribution: str | None = None) -> list[str]:
    """The labels ``ScreenshotHistory`` actually puts beside an image."""
    history = ScreenshotHistory()
    entry = ScreenshotEntry(
        b64="ZmFrZS1qcGVn",
        utterance="",
        timestamp=datetime(2026, 7, 31, tzinfo=timezone.utc),
        source=channel,
        attribution=attribution,
    )
    history.add(entry, generate_screenshot_path(entry))
    return [p for p in history.build_visual_context_content() if isinstance(p, str)]


@pytest.mark.parametrize("channel", MEET_CHANNELS)
def test_the_prompt_names_the_label_the_emitter_produces(channel: str) -> None:
    """The one check that catches a reworded emitter.

    Two modules have to agree on this string: the prompt tells the model what to
    look for, and ``ScreenshotHistory`` decides what arrives. When they drifted,
    nothing failed -- the model just never recognised its own visual context.
    """
    emitted = _emitted_labels(channel)
    assert emitted, "the emitter produced no label at all"
    assert emitted[0] in _prompt(channel)


@pytest.mark.parametrize("channel", MEET_CHANNELS)
def test_the_prompt_does_not_claim_to_see_anything_unconditionally(
    channel: str,
) -> None:
    """Built before anyone shares, so it may not assert that it sees a screen."""
    prompt = _prompt(channel).lower()
    assert "i **can** see the meeting" not in prompt
    # The rule that replaced it: sight is conditional on an attached image.
    assert "i can only see what is actually attached" in prompt


@pytest.mark.parametrize("channel", MEET_CHANNELS)
def test_the_prompt_says_what_it_cannot_see(channel: str) -> None:
    """Frames are filtered to screenshares, so the gallery view is not available.

    The old text promised participant tiles, the chat panel and meeting controls
    -- none of which survive that filter. An assistant told it can see faces will
    answer questions about faces it has never seen.
    """
    prompt = _prompt(channel).lower()
    assert "cannot see participants' faces" in prompt
    assert "chat panel" in prompt


@pytest.mark.parametrize("channel", MEET_CHANNELS)
def test_the_prompt_defers_visual_questions(channel: str) -> None:
    """The slow brain holds the pixels, so it should answer, not the fast brain.

    Answering first and being corrected a moment later is the self-contradiction
    that made this look broken even when frames were flowing.
    """
    prompt = _prompt(channel).lower()
    assert "contentless" in prompt


def test_attribution_rides_the_label() -> None:
    """Whose screen it is has to survive into the label the model reads."""
    labels = _emitted_labels("google_meet", attribution="Ada")
    assert labels
    assert "SHARED BY Ada" in labels[0]
    assert labels[0] == visual_source_label("google_meet", "Ada")


def test_a_non_meet_channel_gets_no_meet_visual_block() -> None:
    """A phone call has no shared screen to describe."""
    assert "visual context" not in _prompt("phone").lower()
