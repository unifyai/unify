"""
tests/conversation_manager/voice/test_speaker_id.py
====================================================

Unit tests for the speaker-identification core (`speaker_id.py`): audio
helpers, ring buffer, centroid accumulation, the SpeakerTracker state
machine (enrolled-contact pinning, anonymous labelling, auto-enrollment
capture, and enrollment suggestion).

The tracker tests drive a stub embedder that derives deterministic vectors
from the audio content itself, so the full pipeline (ring buffer slice →
embedding → centroid → pinning/enrollment) is exercised without the ONNX
model. A separate real-model smoke test runs only when the model is already
cached locally.
"""

from __future__ import annotations

import asyncio
import os
import time

import numpy as np
import pytest

from unify.conversation_manager import speaker_id
from unify.conversation_manager.speaker_id import (
    AudioRingBuffer,
    CentroidAccumulator,
    SpeakerTracker,
    cosine_similarity,
    downmix_to_mono,
    pcm_to_wav_bytes,
    resample_pcm,
    wav_bytes_to_pcm,
)

SR = 16000


def _tone(amplitude: int, seconds: float, sr: int = SR) -> np.ndarray:
    """Constant-amplitude int16 'speech' used to key the stub embedder."""
    return np.full(int(seconds * sr), amplitude, dtype=np.int16)


class StubEmbedder:
    """Maps audio to a deterministic unit vector keyed on mean amplitude.

    Amplitudes below 5000 embed near axis 0, above near axis 1 — two cleanly
    separable "voices" for tracker tests.
    """

    async def embed(self, pcm: np.ndarray, sample_rate: int) -> np.ndarray:
        return self.embed_sync(pcm, sample_rate)

    def embed_sync(self, pcm: np.ndarray, sample_rate: int) -> np.ndarray:
        mean_amp = float(np.abs(pcm.astype(np.int32)).mean()) if len(pcm) else 0.0
        vec = np.array([1.0, 0.0], dtype=np.float32)
        if mean_amp >= 5000:
            vec = np.array([0.0, 1.0], dtype=np.float32)
        return vec


VOICE_A = [1.0, 0.0]  # stub embedding for quiet-amplitude audio
VOICE_B = [0.0, 1.0]  # stub embedding for loud-amplitude audio


# ─────────────────────────────────────────────────────────────────────────────
# Audio helpers
# ─────────────────────────────────────────────────────────────────────────────


class TestAudioHelpers:
    def test_wav_round_trip(self):
        pcm = (np.sin(np.linspace(0, 100, SR)) * 10000).astype(np.int16)
        wav = pcm_to_wav_bytes(pcm, SR)
        decoded, rate = wav_bytes_to_pcm(wav)
        assert rate == SR
        assert np.array_equal(decoded, pcm)

    def test_downmix_stereo(self):
        left = np.full(100, 1000, dtype=np.int16)
        right = np.full(100, 3000, dtype=np.int16)
        interleaved = np.empty(200, dtype=np.int16)
        interleaved[0::2] = left
        interleaved[1::2] = right
        mono = downmix_to_mono(interleaved, 2)
        assert len(mono) == 100
        assert int(mono[0]) == 2000

    def test_resample_integer_factor(self):
        pcm = _tone(1000, 1.0, sr=48000)
        out = resample_pcm(pcm, 48000, 16000)
        assert len(out) == 16000
        assert int(out[0]) == 1000

    def test_resample_non_integer_factor(self):
        pcm = _tone(1000, 1.0, sr=44100)
        out = resample_pcm(pcm, 44100, 16000)
        assert abs(len(out) - 16000) <= 1

    def test_cosine_similarity(self):
        a = np.array([1.0, 0.0])
        b = np.array([0.0, 1.0])
        assert cosine_similarity(a, a) == pytest.approx(1.0)
        assert cosine_similarity(a, b) == pytest.approx(0.0)
        assert cosine_similarity(a, np.zeros(2)) == 0.0


class TestCentroidAccumulator:
    def test_duration_weighted_centroid(self):
        acc = CentroidAccumulator()
        acc.add(np.array([1.0, 0.0], dtype=np.float32), 3.0)
        acc.add(np.array([0.0, 1.0], dtype=np.float32), 1.0)
        centroid = acc.centroid
        assert centroid is not None
        # Longer-duration vector dominates the direction.
        assert centroid[0] > centroid[1]
        assert np.linalg.norm(centroid) == pytest.approx(1.0)
        assert acc.total_duration_s == pytest.approx(4.0)
        assert acc.segments == 2

    def test_empty(self):
        assert CentroidAccumulator().centroid is None

    def test_similarity_seeds_empty_then_tracks_centroid(self):
        # An empty accumulator matches anything (so its first segment always
        # seeds it); once seeded, similarity tracks the running centroid.
        acc = CentroidAccumulator()
        assert acc.similarity(np.array([0.0, 1.0], dtype=np.float32)) == pytest.approx(
            1.0,
        )
        acc.add(np.array([1.0, 0.0], dtype=np.float32), 1.0)
        assert acc.similarity(np.array([1.0, 0.0], dtype=np.float32)) == pytest.approx(
            1.0,
        )
        assert acc.similarity(np.array([0.0, 1.0], dtype=np.float32)) == pytest.approx(
            0.0,
        )


class TestAudioRingBuffer:
    def test_slice_returns_window(self):
        ring = AudioRingBuffer()
        now = time.time()
        ring.append(_tone(1000, 10.0), SR, end_ts=now)
        pcm, rate = ring.slice(now - 4.0, now)
        assert rate == SR
        assert len(pcm) == pytest.approx(4 * SR, abs=2)

    def test_eviction_beyond_max_duration(self):
        ring = AudioRingBuffer(max_duration_s=5.0)
        now = time.time()
        for i in range(10):
            ring.append(_tone(1000, 1.0), SR, end_ts=now - 9 + i)
        # Only ~5 seconds retained.
        pcm, _ = ring.slice(now - 20, now)
        assert len(pcm) <= 6 * SR

    def test_empty_slice(self):
        ring = AudioRingBuffer()
        pcm, _ = ring.slice(0.0, 1.0)
        assert len(pcm) == 0


# ─────────────────────────────────────────────────────────────────────────────
# SpeakerTracker
# ─────────────────────────────────────────────────────────────────────────────


def _make_tracker(
    *,
    enrolled: dict[int, list[float]] | None = None,
    call_contact_id: int | None = 5,
    multi_party: bool = False,
    on_captured=None,
    on_suggested=None,
) -> SpeakerTracker:
    return SpeakerTracker(
        embedder=StubEmbedder(),
        enrolled_profiles=enrolled or {},
        call_contact_id=call_contact_id,
        multi_party=multi_party,
        enrollment_target_s=6.0,
        enrollment_min_s=2.0,
        on_enrollment_captured=on_captured,
        on_enrollment_suggested=on_suggested,
    )


class _Clock:
    """Synthetic wall-clock so consecutive segments never overlap in time."""

    def __init__(self) -> None:
        self.now = time.time()

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


def _feed_segment(
    tracker: SpeakerTracker,
    clock: _Clock,
    speaker_sid: str,
    amplitude: int,
    seconds: float,
) -> None:
    """Append one speech segment on the synthetic timeline and register its
    final transcript, mirroring the live flow (audio tee + STT final event)."""
    end_ts = clock.advance(seconds)
    tracker._ring.append(_tone(amplitude, seconds), SR, end_ts=end_ts)
    tracker.observe_final_transcript(speaker_sid, end_ts=end_ts)


@pytest.mark.asyncio
class TestSpeakerTracker:
    async def test_pins_enrolled_contact(self):
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.finalize()

        resolution = tracker.resolve("S0")
        assert resolution is not None
        assert resolution.contact_id == 5
        assert resolution.verified is True
        assert resolution.source == speaker_id.LABEL_SOURCE_VOICE_PIN

    async def test_anonymous_label_for_unmatched_voice(self):
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        # Boss (matches enrollment) and a second, louder voice.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=3.0)
        await tracker.finalize()

        boss = tracker.resolve("S0")
        other = tracker.resolve("S1")
        assert boss is not None and boss.contact_id == 5
        assert other is not None
        assert other.contact_id is None
        assert other.label == "Speaker 2"
        assert other.verified is False
        assert other.source == speaker_id.LABEL_SOURCE_ANONYMOUS
        # The enrolled voice is stamped as a verified pin, not a placeholder.
        assert boss.source == speaker_id.LABEL_SOURCE_VOICE_PIN

    async def test_no_anonymous_label_without_enrollment(self):
        # Contact NOT enrolled: unmatched voices must not get anonymous labels
        # (we cannot tell which voice is the contact).
        tracker = _make_tracker(enrolled={})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.finalize()

        resolution = tracker.resolve("S0")
        assert resolution is None

    async def test_auto_enrollment_after_target_duration(self):
        captured: list[tuple] = []

        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
        )
        clock = _Clock()
        # Single voice; 3 + 4 = 7s >= 6s target.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=4.0)
        await tracker.finalize()

        assert len(captured) == 1
        embedding, wav_path, duration_s = captured[0]
        assert duration_s >= 6.0
        assert cosine_similarity(np.asarray(embedding), np.array(VOICE_A)) > 0.9
        assert os.path.exists(wav_path)
        pcm, rate = wav_bytes_to_pcm(open(wav_path, "rb").read())
        assert rate == speaker_id.ENROLLMENT_SAMPLE_RATE
        assert len(pcm) / rate >= 6.0
        os.unlink(wav_path)

    async def test_partial_enrollment_fired_at_finalize(self):
        captured: list[tuple] = []
        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
        )
        clock = _Clock()
        # 3s of speech: above the 2s minimum but below the 6s target, so the
        # capture fires only on finalize (call end).
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await asyncio.gather(*list(tracker._pending_tasks), return_exceptions=True)
        assert not captured
        await tracker.finalize()
        assert len(captured) == 1
        os.unlink(captured[0][1])

    async def test_no_enrollment_below_minimum(self):
        captured: list[tuple] = []
        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=1.0)
        await tracker.finalize()
        assert not captured

    async def test_no_enrollment_with_two_speakers_and_suggestion(self):
        captured: list[tuple] = []
        suggested: list[int] = []
        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
            on_suggested=suggested.append,
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=4.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=4.0)
        await tracker.finalize()

        assert not captured
        assert suggested == [2]

    async def test_diarization_oversplit_same_voice_enrolls_without_suggestion(self):
        # Streaming STT often labels one talker as both S0 and S1. Matching
        # centroids must collapse to a single voice so auto-enrollment still
        # fires and the manual-recording suggestion does not.
        captured: list[tuple] = []
        suggested: list[int] = []
        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
            on_suggested=suggested.append,
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=2.0)
        _feed_segment(tracker, clock, "S1", amplitude=1000, seconds=2.0)
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=2.0)
        await tracker.finalize()

        assert not suggested
        assert len(captured) == 1
        # Speech was split across ids; combined duration still enrolls.
        assert captured[0][2] == pytest.approx(6.0, abs=0.05)
        os.unlink(captured[0][1])

    async def test_cross_id_merge_respects_similarity_threshold(self):
        # Centroids that only barely miss CROSS_ID_MERGE_SIM stay distinct so
        # a real second voice still triggers the enrollment suggestion.
        captured: list[tuple] = []
        suggested: list[int] = []

        class _AngledEmbedder(StubEmbedder):
            async def embed(self, pcm: np.ndarray, sample_rate: int) -> np.ndarray:
                mean_amp = (
                    float(np.abs(pcm.astype(np.int32)).mean()) if len(pcm) else 0.0
                )
                if mean_amp >= 5000:
                    # Cosine with [1,0] is 0.2 — below CROSS_ID_MERGE_SIM (0.3).
                    return np.array([0.2, np.sqrt(1.0 - 0.2**2)], dtype=np.float32)
                return np.array([1.0, 0.0], dtype=np.float32)

        tracker = SpeakerTracker(
            embedder=_AngledEmbedder(),
            enrolled_profiles={},
            call_contact_id=5,
            enrollment_target_s=6.0,
            enrollment_min_s=2.0,
            on_enrollment_captured=lambda emb, path, dur: captured.append(
                (emb, path, dur),
            ),
            on_enrollment_suggested=suggested.append,
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=4.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=4.0)
        await tracker.finalize()

        assert not captured
        assert suggested == [2]

    async def test_no_suggestion_when_contact_enrolled(self):
        suggested: list[int] = []
        tracker = _make_tracker(
            enrolled={5: VOICE_A},
            on_suggested=suggested.append,
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=3.0)
        await tracker.finalize()
        assert not suggested

    async def test_short_segments_ignored(self):
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        # Below SEGMENT_MIN_S and never topped up: buffered, then dropped at
        # finalize rather than embedded, so it contributes no cluster.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=0.2)
        await tracker.finalize()
        assert tracker.resolve("S0") is None
        assert tracker.diagnostics()["segments_dropped"] == 1

    async def test_short_segments_accumulate_until_they_can_be_embedded(self):
        """Backchannels are buffered per id, not discarded.

        Several sub-threshold finals from one speaker are concatenated and
        embedded once together, so the speaker is still attributed instead of
        the turns vanishing.
        """
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        for _ in range(3):
            _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=0.9)
        await tracker.finalize()

        resolution = tracker.resolve("S0")
        assert resolution is not None
        assert resolution.contact_id == 5
        stats = tracker.diagnostics()
        assert stats["segments_observed"] == 3
        assert stats["segments_buffered"] == 2  # first two held, third flushed
        assert stats["segments_embedded"] == 1  # one embedding for all three
        assert stats["clusters"] == 1

    async def test_buffered_audio_is_prepended_to_the_next_full_segment(self):
        """A short turn followed by a long one yields a single merged segment.

        The buffered audio must not be stranded: it belongs to the same voice,
        so it is carried into the next embedding rather than dropped.
        """
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=0.9)
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.finalize()

        stats = tracker.diagnostics()
        assert stats["segments_buffered"] == 1
        assert stats["segments_embedded"] == 1
        assert stats["segments_dropped"] == 0
        assert tracker._speakers["S0"].pending_duration_s == 0.0
        # Both turns' audio is behind the one cluster.
        cluster = tracker._speakers["S0"].clusters[0]
        assert cluster.accumulator.total_duration_s == pytest.approx(3.9, abs=0.05)

    async def test_short_segments_buffer_per_diarization_id(self):
        """One speaker's backchannels never top up another speaker's buffer."""
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=1.2)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=1.2)
        await tracker.await_pending()

        # Neither id has reached SEGMENT_MIN_S on its own.
        assert tracker.diagnostics()["segments_embedded"] == 0
        assert tracker._speakers["S0"].pending_duration_s == pytest.approx(
            1.2,
            abs=0.05,
        )
        assert tracker._speakers["S1"].pending_duration_s == pytest.approx(
            1.2,
            abs=0.05,
        )

    async def test_co_located_voices_split_into_clusters(self):
        # A single diarization id (S0) that actually carries two physically
        # co-located voices must be split into separate clusters, each
        # resolving to its own identity for the utterance that just spoke.
        tracker = _make_tracker(enrolled={5: VOICE_A})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.await_pending()
        first = tracker.resolve("S0")
        assert first.contact_id == 5
        assert first.verified is True
        assert first.provisional is False

        # A different, co-located voice speaks under the same id.
        _feed_segment(tracker, clock, "S0", amplitude=9000, seconds=3.0)
        await tracker.await_pending()
        second = tracker.resolve("S0")
        # Attribution follows the voice that just spoke — the second, unmatched
        # cluster with its own anonymous label — and the id is now provisional.
        assert second.contact_id is None
        assert second.label == "Speaker 2"
        assert second.provisional is True

        # The enrolled voice returns: attribution swings back to its pinned
        # cluster, so the contact is still named for routing — but the id now
        # carries two voices, so this utterance cannot be certified as theirs
        # and `verified` is withheld.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.await_pending()
        third = tracker.resolve("S0")
        assert third.contact_id == 5
        assert third.provisional is True
        assert third.verified is False

    async def test_co_located_second_voice_blocks_enrollment_and_suggests(self):
        # Two voices under one diarization id count as two speakers: the
        # contact's voiceprint must not be auto-captured, and enrollment is
        # suggested — even though the STT engine emitted only a single id.
        captured: list[tuple] = []
        suggested: list[int] = []
        tracker = _make_tracker(
            enrolled={},
            on_captured=lambda emb, path, dur: captured.append((emb, path, dur)),
            on_suggested=suggested.append,
        )
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=4.0)
        _feed_segment(tracker, clock, "S0", amplitude=9000, seconds=4.0)
        await tracker.finalize()

        assert not captured
        assert suggested == [2]

    async def test_multi_party_mints_anonymous_labels_without_enrollment(self):
        # A browser meet with nobody enrolled: every distinct voice must still
        # get a stable per-speaker label so the transcript is attributable. The
        # ordinal base is 1 (no primary caller is reserved).
        tracker = _make_tracker(enrolled={}, multi_party=True)
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=3.0)
        await tracker.finalize()

        first = tracker.resolve("S0")
        second = tracker.resolve("S1")
        assert first is not None and first.contact_id is None
        assert first.label == "Speaker 1"
        assert first.source == speaker_id.LABEL_SOURCE_ANONYMOUS
        assert second is not None and second.label == "Speaker 2"

    async def test_multi_party_labels_stable_across_turns(self):
        # Ordinals must not churn as speakers take turns: a voice keeps its label
        # when it speaks again, and co-located voices under one id stay split.
        tracker = _make_tracker(enrolled={}, multi_party=True)
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.await_pending()
        assert tracker.resolve("S0").label == "Speaker 1"

        # A second, co-located voice under the same diarization id.
        _feed_segment(tracker, clock, "S0", amplitude=9000, seconds=3.0)
        await tracker.await_pending()
        assert tracker.resolve("S0").label == "Speaker 2"

        # The first voice returns: its original ordinal is preserved.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.await_pending()
        assert tracker.resolve("S0").label == "Speaker 1"

    async def test_non_multi_party_unenrolled_mints_nothing(self):
        # A phone call (single primary, not multi-party) with an unenrolled
        # contact must not mint anonymous labels — we cannot tell which voice is
        # the contact, so a placeholder would be misleading.
        tracker = _make_tracker(enrolled={}, multi_party=False)
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=3.0)
        await tracker.finalize()

        assert tracker.resolve("S0") is None
        assert tracker.resolve("S1") is None

    async def test_enrolled_contact_reserves_speaker_one(self):
        # When the primary caller is enrolled, "Speaker 1" is reserved for them
        # (they resolve by name via the pin) and anonymous ordinals start at 2.
        tracker = _make_tracker(enrolled={5: VOICE_A}, multi_party=True)
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        _feed_segment(tracker, clock, "S1", amplitude=9000, seconds=3.0)
        await tracker.finalize()

        assert tracker.resolve("S0").contact_id == 5
        other = tracker.resolve("S1")
        assert other.contact_id is None
        assert other.label == "Speaker 2"


# ─────────────────────────────────────────────────────────────────────────────
# Mid-call profile refresh (late joiners)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestMidCallProfileRefresh:
    async def test_late_profile_pins_a_voice_already_heard(self):
        """Someone who joined and spoke before their profile arrived is pinned.

        Cluster centroids are re-scored on every segment, so the late joiner is
        picked up on their next utterance without replaying the call.
        """
        tracker = _make_tracker(enrolled={})
        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=9000, seconds=3.0)
        await tracker.await_pending()
        assert tracker.resolve("S0") is None  # nobody enrolled yet

        assert tracker.add_enrolled_profiles({9: VOICE_B}) == 1

        _feed_segment(tracker, clock, "S0", amplitude=9000, seconds=3.0)
        await tracker.await_pending()
        resolution = tracker.resolve("S0")
        assert resolution is not None
        assert resolution.contact_id == 9

    async def test_refresh_does_not_disturb_an_existing_profile(self):
        """A repeated roster push must not overwrite pins already in effect."""
        tracker = _make_tracker(enrolled={5: VOICE_A})
        assert tracker.add_enrolled_profiles({5: VOICE_B}) == 0

        clock = _Clock()
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=3.0)
        await tracker.await_pending()
        assert tracker.resolve("S0").contact_id == 5

    async def test_refresh_ignores_malformed_entries(self):
        tracker = _make_tracker(enrolled={})
        assert tracker.add_enrolled_profiles({"not-an-id": VOICE_A}) == 0
        assert tracker.add_enrolled_profiles({}) == 0
        assert tracker.add_enrolled_profiles({7: VOICE_A}) == 1
