"""Correlating diarized voices with platform speaker names.

The regression these guard is subtle: the correlation used to sample whoever the
platform said was speaking *at the moment a transcript finalised*. A final lands
after its speaker stopped, so on a call where people take turns the answer was
"nobody" every time and the map never filled -- attribution silently fell through
to the weaker signals below it, which is indistinguishable from working.
"""

from unify.conversation_manager.meet_speaker_map import (
    MIN_VOTES,
    MeetSpeakerVotes,
    MeetSpeakerWindows,
)


def test_a_final_arriving_after_speech_off_still_attributes():
    """The actual regression: transcription lag must not lose the speaker."""
    windows = MeetSpeakerWindows()
    windows.speech_on("Ada", 100.0)
    windows.speech_off("Ada", 103.0)

    # The turn was spoken over [100.2, 103.1]; the final lands at 103.6, well
    # after the platform said Ada stopped.
    assert windows.speaker_during(100.2, 103.1) == "Ada"


def test_sampling_the_instant_of_finalisation_is_what_used_to_fail():
    """Nobody is speaking when a final arrives -- hence span matching."""
    windows = MeetSpeakerWindows()
    windows.speech_on("Ada", 100.0)
    windows.speech_off("Ada", 103.0)

    # An instant well past the span and past the grace: correctly nobody.
    assert windows.speaker_during(110.0, 110.0) is None


def test_the_speaker_with_the_most_overlap_wins():
    windows = MeetSpeakerWindows()
    windows.speech_on("Ada", 100.0)
    windows.speech_off("Ada", 100.4)
    windows.speech_on("Grace", 100.4)
    windows.speech_off("Grace", 104.0)

    assert windows.speaker_during(100.0, 104.0) == "Grace"


def test_an_open_span_is_matchable_before_its_speech_off_arrives():
    """Attribution cannot wait for speech_off; it may be dropped entirely."""
    windows = MeetSpeakerWindows()
    windows.speech_on("Ada", 100.0)

    assert windows.speaker_during(100.5, 102.0) == "Ada"


def test_a_dropped_speech_off_does_not_capture_the_rest_of_the_call():
    """A span left open would otherwise outvote every later speaker forever."""
    windows = MeetSpeakerWindows()
    windows.speech_on("Ada", 100.0)
    # No speech_off for Ada -- her next speech_on has to close the first span.
    windows.speech_on("Ada", 200.0)
    windows.speech_off("Ada", 201.0)
    windows.speech_on("Grace", 300.0)
    windows.speech_off("Grace", 310.0)

    assert windows.speaker_during(300.0, 310.0) == "Grace"


def test_a_voice_the_platform_never_reported_resolves_to_nobody():
    """A phone dial-in, or a stretch where the relay was down."""
    windows = MeetSpeakerWindows()
    assert windows.speaker_during(100.0, 104.0) is None


def test_spans_are_bounded_over_a_long_call():
    """A four-hour meeting must not grow this without limit."""
    windows = MeetSpeakerWindows()
    for index in range(5000):
        windows.speech_on(f"Speaker {index}", float(index))
        windows.speech_off(f"Speaker {index}", float(index) + 0.5)

    # Recent history still resolves; the ancient beginning of the call is gone.
    assert windows.speaker_during(4999.0, 4999.4) == "Speaker 4999"
    assert windows.speaker_during(0.0, 0.4) is None


def test_one_overlap_is_not_enough_to_name_a_voice():
    """Diarization ids drift and split; a single coincidence must not stick."""
    votes = MeetSpeakerVotes()
    votes.observe("S0", "Ada")

    assert MIN_VOTES == 2
    assert votes.resolve("S0") is None


def test_a_repeated_name_names_the_voice():
    votes = MeetSpeakerVotes()
    votes.observe("S0", "Ada")
    votes.observe("S0", "Ada")

    assert votes.resolve("S0") == "Ada"


def test_a_contested_voice_stays_unnamed():
    """Two votes each is a coin flip, not evidence."""
    votes = MeetSpeakerVotes()
    votes.observe("S0", "Ada")
    votes.observe("S0", "Ada")
    votes.observe("S0", "Grace")
    votes.observe("S0", "Grace")

    assert votes.resolve("S0") is None


def test_a_clear_majority_wins_despite_a_stray_vote():
    votes = MeetSpeakerVotes()
    for _ in range(4):
        votes.observe("S0", "Ada")
    votes.observe("S0", "Grace")

    assert votes.resolve("S0") == "Ada"


def test_votes_are_per_diarization_id():
    votes = MeetSpeakerVotes()
    votes.observe("S0", "Ada")
    votes.observe("S0", "Ada")
    votes.observe("S1", "Grace")
    votes.observe("S1", "Grace")

    assert votes.resolve("S0") == "Ada"
    assert votes.resolve("S1") == "Grace"
    assert votes.resolve("S2") is None


def test_nothing_is_recorded_without_both_a_voice_and_a_name():
    votes = MeetSpeakerVotes()
    votes.observe("S0", None)
    votes.observe("S0", "")
    votes.observe("", "Ada")

    assert votes.resolve("S0") is None
    assert votes.resolve(None) is None
