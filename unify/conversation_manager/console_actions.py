"""Pairing a spoken line with the console moves it narrates.

The slow brain writes ``[[1]]``, ``[[2]]`` into the line it wants spoken and
lists the matching targets on ``show_in_console``. The markers never reach TTS:
they are removed here, and what is kept is each one's position in the clean
text, so Console can make the move on the words rather than on a timer.

Positions are measured in characters of the spoken text because that is what
Console can count as the synchronized transcript arrives. Anything time-based
would drift with speech rate and would keep firing after a barge-in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Doubled brackets: rare in speech, trivial to strip, and visibly wrong in a
# transcript if a line ever escapes with one still in it.
_MARKER = re.compile(r"\[\[(\d+)\]\]")


@dataclass(frozen=True)
class ConsoleActionStep:
    target: str
    after_chars: int


@dataclass(frozen=True)
class ParsedUtterance:
    """A line ready to speak, and the moves to make while it plays."""

    spoken_text: str
    steps: tuple[ConsoleActionStep, ...]
    #: Markers with no matching target, or targets with no marker. The line is
    #: still spoken; only the unmatched moves are dropped.
    dropped: tuple[str, ...]


def parse_console_actions(message: str, targets: list[str]) -> ParsedUtterance:
    """Strip markers from ``message`` and place ``targets`` at their positions.

    Marker ``[[n]]`` takes the nth target, one-indexed, matching how the tool
    documents itself. A marker without a target is dropped rather than shifting
    the rest along: a mismatch means the model miscounted, and moving the user
    somewhere it did not mean is worse than not moving them.
    """
    steps: list[ConsoleActionStep] = []
    dropped: list[str] = []
    used: set[int] = set()
    # Leading whitespace goes before any offset is measured; tidying it later
    # would shift every position already recorded.
    spoken = message.lstrip()
    out: list[str] = []
    cursor = 0

    for match in _MARKER.finditer(spoken):
        out.append(spoken[cursor : match.start()])
        cursor = match.end()
        remainder = spoken[cursor:]
        # A marker lifted from mid-sentence leaves either a doubled space or a
        # space before punctuation. Close the gap now, so the offset recorded
        # below indexes the text that will actually be spoken.
        if out and out[-1].endswith(" ") and re.match(r"[\s,.!?;:]", remainder):
            out[-1] = out[-1][:-1]
        elif not any(out):
            # Marker opened the line; drop the space it left in front. The
            # offset recorded below stays 0 either way, which is the start.
            cursor += len(remainder) - len(remainder.lstrip())

        index = int(match.group(1))
        if not 1 <= index <= len(targets):
            dropped.append(f"marker [[{index}]] has no target")
            continue
        used.add(index)
        steps.append(
            ConsoleActionStep(
                target=targets[index - 1],
                after_chars=sum(len(part) for part in out),
            ),
        )

    out.append(spoken[cursor:])
    # Trailing whitespace only; every recorded offset sits before it.
    spoken = "".join(out).rstrip()

    for index, target in enumerate(targets, start=1):
        if index not in used:
            dropped.append(f"target {target} has no [[{index}]] marker")

    return ParsedUtterance(
        spoken_text=spoken,
        steps=tuple(steps),
        dropped=tuple(dropped),
    )


def strip_markers(message: str) -> str:
    """Remove markers without pairing anything, for paths that only speak.

    Used when a line carries markers but the moves cannot run — no Console open,
    or ``show_in_console`` was never called. The line is still worth speaking;
    the markers are not worth reading aloud.
    """
    return parse_console_actions(message, []).spoken_text
