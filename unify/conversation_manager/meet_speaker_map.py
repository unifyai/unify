"""Correlating diarized voices with the meeting platform's own speaker names.

In a browser meet the fast brain learns two things from two places. The
transcriber reports finalised utterances tagged with an anonymous diarization id
(``S0``, ``S1``); the meeting platform reports, over the Recall relay, spans of
who was speaking. Neither names a voice on its own, and the pairing is what lets
a transcript row say "Ada" instead of "Speaker 1".

Matching is by **overlap of time spans**, not by asking who is speaking at the
moment a transcript finalises. A final lands after its speaker stopped talking,
by which point the platform has already sent ``speech_off`` and -- on a call
where people take turns -- nobody is speaking at all. Sampling the instantaneous
speaker therefore learns nothing on a well-behaved call, and fires only when
someone talks over the tail of the previous speaker, which is precisely when it
is most likely to name the wrong person.

Votes accumulate rather than binding on first sight: diarization ids are
per-call and can drift or split, so a single coincidental overlap must not name
a voice for the rest of the meeting.
"""

from __future__ import annotations

from dataclasses import dataclass

# A speaking span stays matchable slightly past its end. Platform speech
# boundaries and our own VAD boundaries are independent measurements of the same
# speech, so they disagree by a fraction of a second in both directions; without
# a little slack, short utterances land just outside every window and correlate
# to nothing.
_WINDOW_GRACE_S = 1.5

# Speaking spans kept per call. Enough for a long multi-party meeting, bounded so
# a four-hour call cannot grow this without limit.
_MAX_WINDOWS = 256

# A name must win repeatedly, and by a clear margin, before it labels a voice.
# One overlap is noise on a call where people interrupt each other.
MIN_VOTES = 2
MIN_SHARE = 0.6


@dataclass
class _Window:
    name: str
    start: float
    end: float | None = None


class MeetSpeakerWindows:
    """Spans of who the meeting platform said was speaking, and when.

    Times are epoch seconds from one clock -- the caller's. Mixing a monotonic
    reading with a wall-clock one silently yields zero overlap everywhere.
    """

    def __init__(self) -> None:
        self._windows: list[_Window] = []

    def speech_on(self, name: str, at: float) -> None:
        """Open a span for ``name``.

        A second ``speech_on`` with no intervening ``speech_off`` closes the
        previous span at the same instant: a dropped ``speech_off`` would
        otherwise leave a span open for the rest of the call, overlapping --
        and outvoting -- every later speaker.
        """
        if not name:
            return
        self._close(name, at)
        self._windows.append(_Window(name=name, start=at))
        if len(self._windows) > _MAX_WINDOWS:
            del self._windows[: len(self._windows) - _MAX_WINDOWS]

    def speech_off(self, name: str, at: float) -> None:
        """Close the open span for ``name``, if there is one."""
        self._close(name, at)

    def speaker_during(self, start: float, end: float) -> str | None:
        """Whoever was speaking for most of ``[start, end]``.

        Returns None when no span overlaps, which is the honest answer for a
        voice the platform never reported speaking -- a phone dial-in, or a
        relay that was down for that stretch.
        """
        if end < start:
            end = start
        best_name: str | None = None
        best_overlap = 0.0
        for window in self._windows:
            window_end = window.end if window.end is not None else end
            overlap = min(window_end + _WINDOW_GRACE_S, end) - max(window.start, start)
            # ``>=`` so that among equal overlaps the latest span wins: spans are
            # appended in order, and the most recent is the better guess for a
            # just-finalised utterance.
            if overlap > 0 and overlap >= best_overlap:
                best_overlap = overlap
                best_name = window.name
        return best_name

    def _close(self, name: str, at: float) -> None:
        for window in reversed(self._windows):
            if window.name == name and window.end is None:
                window.end = at
                return


class MeetSpeakerVotes:
    """Accumulated evidence linking a diarization id to a platform name."""

    def __init__(self) -> None:
        self._votes: dict[str, dict[str, int]] = {}

    def observe(self, speaker_id: str, name: str | None) -> None:
        """Record one utterance by ``speaker_id`` overlapping ``name``'s speech."""
        if not speaker_id or not name:
            return
        bucket = self._votes.setdefault(speaker_id, {})
        bucket[name] = bucket.get(name, 0) + 1

    def resolve(self, speaker_id: str | None) -> str | None:
        """The platform name for ``speaker_id``, once the evidence is decisive."""
        if not speaker_id:
            return None
        bucket = self._votes.get(speaker_id)
        if not bucket:
            return None
        top_name = max(bucket, key=lambda name: bucket[name])
        top_votes = bucket[top_name]
        if top_votes < MIN_VOTES:
            return None
        if top_votes / sum(bucket.values()) <= MIN_SHARE:
            return None
        return top_name
