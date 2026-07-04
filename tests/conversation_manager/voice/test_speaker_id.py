"""
tests/conversation_manager/voice/test_speaker_id.py
====================================================

Unit tests for the speaker-identification core (`speaker_id.py`): audio
helpers, ring buffer, centroid accumulation, and the SpeakerTracker state
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
    on_captured=None,
    on_suggested=None,
) -> SpeakerTracker:
    return SpeakerTracker(
        embedder=StubEmbedder(),
        enrolled_profiles=enrolled or {},
        call_contact_id=call_contact_id,
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
        # Below SEGMENT_MIN_S: no embedding scheduled.
        _feed_segment(tracker, clock, "S0", amplitude=1000, seconds=0.2)
        await tracker.finalize()
        assert tracker.resolve("S0") is None


# ─────────────────────────────────────────────────────────────────────────────
# Real model smoke test (runs only when the ONNX model is already cached)
# ─────────────────────────────────────────────────────────────────────────────


_MODEL_AVAILABLE = speaker_id.ensure_speaker_model(download=False) is not None


@pytest.mark.skipif(
    not _MODEL_AVAILABLE,
    reason="speaker embedding model not cached locally",
)
def test_real_model_same_audio_high_similarity():
    """The real extractor produces stable, unit-norm embeddings.

    Identical audio must embed identically; spectrally different audio must
    not. Uses synthetic harmonic 'voices' (different F0 + formant structure).
    """
    rng = np.random.default_rng(42)
    t = np.linspace(0, 5.0, 5 * SR, endpoint=False)

    def synth_voice(f0: float) -> np.ndarray:
        signal = np.zeros_like(t)
        for harmonic in range(1, 6):
            signal += np.sin(2 * np.pi * f0 * harmonic * t) / harmonic
        signal *= 1.0 + 0.3 * np.sin(2 * np.pi * 3.0 * t)  # AM "prosody"
        signal += 0.05 * rng.standard_normal(len(t))
        return (signal / np.abs(signal).max() * 20000).astype(np.int16)

    embedder = speaker_id.SpeakerEmbedder()
    low_voice = synth_voice(110.0)
    high_voice = synth_voice(220.0)

    emb_low = embedder.embed_sync(low_voice, SR)
    emb_low_again = embedder.embed_sync(low_voice, SR)
    emb_high = embedder.embed_sync(high_voice, SR)

    assert np.linalg.norm(emb_low) == pytest.approx(1.0, abs=1e-3)
    assert cosine_similarity(emb_low, emb_low_again) > 0.99
    assert cosine_similarity(emb_low, emb_high) < cosine_similarity(
        emb_low,
        emb_low_again,
    )
