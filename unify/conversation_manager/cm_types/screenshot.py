from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path
from typing import NamedTuple


class ScreenshotEntry(NamedTuple):
    """A single screenshot captured during screen sharing or webcam, paired with context."""

    b64: str
    utterance: str
    timestamp: datetime
    source: str  # "assistant" | "user" (screen share) | "webcam" | "google_meet" | "teams_meet"
    local_message_id: int | None = None
    filepath: str | None = None
    # Who the frame belongs to, when the source alone does not say. A meeting has
    # many people in it, so a shared screen is somebody's in particular; the
    # single-user sources leave this empty.
    attribution: str | None = None


_SOURCE_SUBFOLDER = {
    "assistant": "Assistant",
    "user": "User",
    "webcam": "Webcam",
    "google_meet": "GoogleMeet",
    "teams_meet": "TeamsMeet",
}

# How each visual source is announced to the fast brain, and the fenced form the
# label is emitted in.
#
# Canonical here rather than at the point of use because two modules must agree
# on the exact string: ``ScreenshotHistory`` emits it beside the image, and the
# voice-agent system prompt tells the model which label to look for. Restating it
# in the prompt is what broke this once already -- the emitter was reworded and
# the prompt kept naming a label that no longer appeared, so the model was
# hunting for a string that never arrived. Prompts interpolate these; they do not
# re-type them.
VISUAL_SOURCE_LABELS = {
    "assistant": "YOUR SCREEN (this is what YOUR machine currently shows)",
    "user": "USER'S SCREEN (this is THEIR machine, not yours)",
    "webcam": "USER'S WEBCAM",
    "google_meet": "SCREEN SHARED IN GOOGLE MEET (a participant's machine, not yours)",
    "teams_meet": "SCREEN SHARED IN TEAMS MEETING (a participant's machine, not yours)",
}

# Order the fast brain sees its visual sources in.
VISUAL_SOURCE_ORDER = ("assistant", "user", "webcam", "google_meet", "teams_meet")

_ATTRIBUTION_SUFFIX = " -- SHARED BY {name}"


def visual_source_label(source: str, attribution: str | None = None) -> str:
    """The fenced label emitted beside one image in the fast brain's context.

    ``attribution`` names the person whose screen it is, which only a meeting
    has: the single-user sources are unambiguous without it.
    """

    label = VISUAL_SOURCE_LABELS.get(source, "SCREENSHOT")
    if attribution:
        label += _ATTRIBUTION_SUFFIX.format(name=attribution)
    return f"=== {label} ==="


def generate_screenshot_path(entry: ScreenshotEntry) -> str:
    """Compute a deterministic filepath for a screenshot (no I/O)."""
    subfolder = _SOURCE_SUBFOLDER.get(entry.source, entry.source.title())
    directory = Path("Screenshots") / subfolder
    stem = entry.timestamp.strftime("%Y-%m-%dT%H-%M-%S.%f")
    return str(directory / f"{stem}.jpg")


def write_screenshot_to_disk(entry: ScreenshotEntry, path: str) -> None:
    """Write screenshot bytes to disk. Safe to call from a background task."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(base64.b64decode(entry.b64))
