"""Speaker identification: embeddings, matching, and per-call speaker tracking.

The live voice pipeline stays untouched; everything here runs off the hot
path. Audio frames are teed into a ring buffer while they stream to STT, and
each final diarized transcript triggers an embedding computation in a worker
thread. Embeddings pin Deepgram's per-call anonymous speaker ids (S0, S1, …)
to enrolled contacts, and accumulate auto-enrollments for single-speaker
calls.
"""

from __future__ import annotations

import asyncio
import io
import os
import tempfile
import time
import wave
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# Model management
# ─────────────────────────────────────────────────────────────────────────────

SPEAKER_MODEL_NAME = "wespeaker_en_voxceleb_CAM++.onnx"
SPEAKER_MODEL_URL = (
    "https://github.com/k2-fsa/sherpa-onnx/releases/download/"
    f"speaker-recongition-models/{SPEAKER_MODEL_NAME}"
)

# Cosine-similarity acceptance threshold for pinning an anonymous speaker id
# to an enrolled contact. CAM++ VoxCeleb embeddings of the same speaker across
# telephone-band audio typically score 0.6-0.8; different speakers < 0.4.
SPEAKER_MATCH_THRESHOLD = 0.55

# Auto-enrollment bounds (seconds of accumulated speech from a single voice).
ENROLLMENT_TARGET_S = 60.0
ENROLLMENT_MIN_S = 15.0

# Per-segment slicing bounds around a final transcript.
SEGMENT_MAX_S = 15.0
SEGMENT_MIN_S = 0.8

# Sample rate used for persisted enrollment audio and embedding input.
ENROLLMENT_SAMPLE_RATE = 16000

RING_BUFFER_S = 120.0


def speaker_model_path() -> Path:
    """Return the local path of the speaker-embedding model (may not exist)."""
    override = os.environ.get("UNIFY_SPEAKER_MODEL_PATH", "")
    if override:
        return Path(override)
    cache_root = Path(
        os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"),
    )
    return cache_root / "unify" / "speaker_id" / SPEAKER_MODEL_NAME


def ensure_speaker_model(*, download: bool = True) -> Path | None:
    """Return the model path, downloading it into the cache if needed.

    Returns None when the model is unavailable and cannot be downloaded, so
    callers can degrade gracefully (speaker attribution disabled).
    """
    path = speaker_model_path()
    if path.exists() and path.stat().st_size > 0:
        return path
    if not download:
        return None
    try:
        import urllib.request

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        urllib.request.urlretrieve(SPEAKER_MODEL_URL, tmp_path)
        os.replace(tmp_path, path)
        return path
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Audio helpers
# ─────────────────────────────────────────────────────────────────────────────


def downmix_to_mono(pcm: np.ndarray, num_channels: int) -> np.ndarray:
    """Average interleaved int16 channels down to mono."""
    if num_channels <= 1:
        return pcm
    usable = len(pcm) - (len(pcm) % num_channels)
    frames = pcm[:usable].reshape(-1, num_channels).astype(np.int32)
    return (frames.mean(axis=1)).astype(np.int16)


def resample_pcm(pcm: np.ndarray, src_rate: int, dst_rate: int) -> np.ndarray:
    """Resample int16 mono PCM.

    Integer downsample factors use boxcar averaging (cheap anti-aliasing);
    everything else falls back to linear interpolation, which is adequate for
    speaker embeddings and enrollment archival.
    """
    if src_rate == dst_rate or len(pcm) == 0:
        return pcm
    if src_rate % dst_rate == 0:
        factor = src_rate // dst_rate
        usable = len(pcm) - (len(pcm) % factor)
        if usable == 0:
            return np.zeros(0, dtype=np.int16)
        frames = pcm[:usable].reshape(-1, factor).astype(np.int32)
        return frames.mean(axis=1).astype(np.int16)
    duration = len(pcm) / src_rate
    dst_len = int(duration * dst_rate)
    src_t = np.linspace(0.0, duration, num=len(pcm), endpoint=False)
    dst_t = np.linspace(0.0, duration, num=dst_len, endpoint=False)
    return np.interp(dst_t, src_t, pcm.astype(np.float32)).astype(np.int16)


def pcm_to_wav_bytes(pcm: np.ndarray, sample_rate: int) -> bytes:
    """Encode int16 mono PCM as a WAV container."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm.tobytes())
    return buf.getvalue()


def wav_bytes_to_pcm(wav_bytes: bytes) -> tuple[np.ndarray, int]:
    """Decode a WAV container to int16 mono PCM + sample rate."""
    with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
        sample_rate = wf.getframerate()
        num_channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        raw = wf.readframes(wf.getnframes())
    if sampwidth != 2:
        raise ValueError(f"Only 16-bit WAV supported, got {sampwidth * 8}-bit")
    pcm = np.frombuffer(raw, dtype=np.int16)
    return downmix_to_mono(pcm, num_channels), sample_rate


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two vectors."""
    denom = float(np.linalg.norm(a)) * float(np.linalg.norm(b))
    if denom == 0.0:
        return 0.0
    return float(np.dot(a, b) / denom)


# ─────────────────────────────────────────────────────────────────────────────
# Embedding extraction
# ─────────────────────────────────────────────────────────────────────────────


class SpeakerEmbedder:
    """Thin wrapper around sherpa-onnx speaker-embedding extraction.

    All compute runs on a dedicated single worker thread so concurrent calls
    never contend inside the native extractor.
    """

    def __init__(self, model_path: str | Path | None = None) -> None:
        self._model_path = str(model_path) if model_path else None
        self._extractor = None
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="speaker-embed",
        )

    def _get_extractor(self):
        if self._extractor is None:
            import sherpa_onnx

            model = self._model_path or str(ensure_speaker_model())
            config = sherpa_onnx.SpeakerEmbeddingExtractorConfig(
                model=model,
                num_threads=1,
                provider="cpu",
            )
            self._extractor = sherpa_onnx.SpeakerEmbeddingExtractor(config)
        return self._extractor

    def embed_sync(self, pcm: np.ndarray, sample_rate: int) -> np.ndarray:
        """Compute a unit-normalized embedding for int16 mono PCM (blocking)."""
        extractor = self._get_extractor()
        samples = pcm.astype(np.float32) / 32768.0
        stream = extractor.create_stream()
        stream.accept_waveform(sample_rate, samples)
        stream.input_finished()
        embedding = np.asarray(extractor.compute(stream), dtype=np.float32)
        norm = float(np.linalg.norm(embedding))
        if norm > 0.0:
            embedding = embedding / norm
        return embedding

    async def embed(self, pcm: np.ndarray, sample_rate: int) -> np.ndarray:
        """Compute an embedding without blocking the event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self.embed_sync,
            pcm,
            sample_rate,
        )

    def embed_wav_sync(self, wav_bytes: bytes) -> np.ndarray:
        """Compute an embedding directly from WAV bytes (blocking)."""
        pcm, sample_rate = wav_bytes_to_pcm(wav_bytes)
        return self.embed_sync(pcm, sample_rate)


# ─────────────────────────────────────────────────────────────────────────────
# Centroid accumulation
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CentroidAccumulator:
    """Running duration-weighted centroid of embeddings for one speaker id."""

    _sum: np.ndarray | None = None
    total_duration_s: float = 0.0
    segments: int = 0

    def add(self, embedding: np.ndarray, duration_s: float) -> None:
        weighted = embedding * max(duration_s, 0.1)
        if self._sum is None:
            self._sum = weighted.copy()
        else:
            self._sum += weighted
        self.total_duration_s += duration_s
        self.segments += 1

    @property
    def centroid(self) -> np.ndarray | None:
        if self._sum is None:
            return None
        norm = float(np.linalg.norm(self._sum))
        if norm == 0.0:
            return None
        return self._sum / norm


# ─────────────────────────────────────────────────────────────────────────────
# Ring buffer
# ─────────────────────────────────────────────────────────────────────────────


class AudioRingBuffer:
    """Wall-clock-timestamped PCM ring buffer holding the last N seconds."""

    def __init__(self, max_duration_s: float = RING_BUFFER_S) -> None:
        self._max_duration_s = max_duration_s
        # Entries: (end_timestamp, pcm int16 mono, sample_rate)
        self._chunks: deque[tuple[float, np.ndarray, int]] = deque()
        self._duration_s = 0.0

    def append(
        self,
        pcm: np.ndarray,
        sample_rate: int,
        *,
        end_ts: float | None = None,
    ) -> None:
        if len(pcm) == 0:
            return
        end_ts = end_ts if end_ts is not None else time.time()
        self._chunks.append((end_ts, pcm, sample_rate))
        self._duration_s += len(pcm) / sample_rate
        while self._duration_s > self._max_duration_s and self._chunks:
            _, old, old_rate = self._chunks.popleft()
            self._duration_s -= len(old) / old_rate

    def slice(self, start_ts: float, end_ts: float) -> tuple[np.ndarray, int]:
        """Return concatenated mono PCM overlapping [start_ts, end_ts]."""
        parts: list[np.ndarray] = []
        sample_rate = ENROLLMENT_SAMPLE_RATE
        for chunk_end, pcm, rate in self._chunks:
            chunk_start = chunk_end - len(pcm) / rate
            if chunk_end <= start_ts or chunk_start >= end_ts:
                continue
            sample_rate = rate
            lo = max(0, int((start_ts - chunk_start) * rate))
            hi = min(len(pcm), int((end_ts - chunk_start) * rate))
            if hi > lo:
                parts.append(pcm[lo:hi])
        if not parts:
            return np.zeros(0, dtype=np.int16), sample_rate
        return np.concatenate(parts), sample_rate


# ─────────────────────────────────────────────────────────────────────────────
# Speaker tracker
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class SpeakerResolution:
    """Resolution of an anonymous diarization speaker id."""

    contact_id: Optional[int] = None
    label: Optional[str] = None
    verified: bool = False


@dataclass
class _SpeakerState:
    accumulator: CentroidAccumulator = field(default_factory=CentroidAccumulator)
    pinned_contact_id: Optional[int] = None
    anonymous_label: Optional[str] = None
    enrollment_audio: list[np.ndarray] = field(default_factory=list)
    enrollment_duration_s: float = 0.0
    enrollment_sample_rate: int = ENROLLMENT_SAMPLE_RATE


class SpeakerTracker:
    """Per-call speaker attribution and auto-enrollment.

    Feed it raw audio (`add_audio`) and final diarized transcripts
    (`observe_final_transcript`); query it with `resolve(speaker_id)`.

    Callbacks fire at most once per call:
    - ``on_enrollment_captured(embedding, wav_path, duration_s)`` when a
      single-voice call has accumulated enough speech to enroll the contact.
    - ``on_enrollment_suggested(num_speakers)`` when multiple voices are heard
      but the call contact has no enrollment to disambiguate them.
    """

    def __init__(
        self,
        *,
        embedder: SpeakerEmbedder,
        enrolled_profiles: dict[int, np.ndarray],
        call_contact_id: int | None,
        match_threshold: float = SPEAKER_MATCH_THRESHOLD,
        enrollment_target_s: float = ENROLLMENT_TARGET_S,
        enrollment_min_s: float = ENROLLMENT_MIN_S,
        on_enrollment_captured: Callable[[np.ndarray, str, float], None] | None = None,
        on_enrollment_suggested: Callable[[int], None] | None = None,
    ) -> None:
        self._embedder = embedder
        self._enrolled = {
            int(cid): np.asarray(vec, dtype=np.float32)
            for cid, vec in (enrolled_profiles or {}).items()
        }
        self._call_contact_id = (
            int(call_contact_id) if call_contact_id is not None else None
        )
        self._match_threshold = match_threshold
        self._enrollment_target_s = enrollment_target_s
        self._enrollment_min_s = enrollment_min_s
        self._on_enrollment_captured = on_enrollment_captured
        self._on_enrollment_suggested = on_enrollment_suggested

        self._ring = AudioRingBuffer()
        self._speakers: dict[str, _SpeakerState] = {}
        self._last_final_ts: float = 0.0
        self._next_anonymous_index = 2
        self._enrollment_fired = False
        self._suggestion_fired = False
        self._pending_tasks: set[asyncio.Task] = set()

    # ── audio ingestion ──────────────────────────────────────────────────

    def add_audio(
        self,
        data: bytes | np.ndarray,
        sample_rate: int,
        num_channels: int = 1,
    ) -> None:
        pcm = (
            np.frombuffer(data, dtype=np.int16)
            if isinstance(data, (bytes, bytearray, memoryview))
            else np.asarray(data, dtype=np.int16)
        )
        pcm = downmix_to_mono(pcm, num_channels)
        self._ring.append(pcm, sample_rate)

    # ── transcript observation ───────────────────────────────────────────

    def observe_final_transcript(
        self,
        speaker_id: str | None,
        *,
        end_ts: float | None = None,
    ) -> None:
        """Register a final diarized transcript; schedules embedding work."""
        end_ts = end_ts if end_ts is not None else time.time()
        window_start = max(self._last_final_ts, end_ts - SEGMENT_MAX_S)
        self._last_final_ts = end_ts
        if not speaker_id:
            return
        pcm, sample_rate = self._ring.slice(window_start, end_ts)
        duration_s = len(pcm) / sample_rate if sample_rate else 0.0
        if duration_s < SEGMENT_MIN_S:
            return
        task = asyncio.create_task(
            self._process_segment(speaker_id, pcm, sample_rate, duration_s),
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _process_segment(
        self,
        speaker_id: str,
        pcm: np.ndarray,
        sample_rate: int,
        duration_s: float,
    ) -> None:
        embedding = await self._embedder.embed(pcm, sample_rate)
        state = self._speakers.setdefault(speaker_id, _SpeakerState())
        state.accumulator.add(embedding, duration_s)
        self._try_pin(state)
        self._accumulate_enrollment(state, pcm, sample_rate, duration_s)
        self._check_enrollment_progress()
        self._check_suggestion()

    def _try_pin(self, state: _SpeakerState) -> None:
        if state.pinned_contact_id is not None or not self._enrolled:
            return
        centroid = state.accumulator.centroid
        if centroid is None:
            return
        best_cid, best_score = None, 0.0
        for cid, profile in self._enrolled.items():
            score = cosine_similarity(centroid, profile)
            if score > best_score:
                best_cid, best_score = cid, score
        if best_cid is not None and best_score >= self._match_threshold:
            state.pinned_contact_id = best_cid
            state.anonymous_label = None
        elif state.anonymous_label is None and self._call_contact_enrolled:
            # The call contact is enrolled but this voice does not match:
            # give it a stable session-scoped anonymous identity.
            state.anonymous_label = f"Speaker {self._next_anonymous_index}"
            self._next_anonymous_index += 1

    @property
    def _call_contact_enrolled(self) -> bool:
        return (
            self._call_contact_id is not None
            and self._call_contact_id in self._enrolled
        )

    # ── auto-enrollment ──────────────────────────────────────────────────

    def _accumulate_enrollment(
        self,
        state: _SpeakerState,
        pcm: np.ndarray,
        sample_rate: int,
        duration_s: float,
    ) -> None:
        if (
            self._enrollment_fired
            or self._call_contact_id is None
            or self._call_contact_enrolled
        ):
            return
        if state.enrollment_duration_s >= self._enrollment_target_s:
            return
        state.enrollment_audio.append(
            resample_pcm(pcm, sample_rate, ENROLLMENT_SAMPLE_RATE),
        )
        state.enrollment_sample_rate = ENROLLMENT_SAMPLE_RATE
        state.enrollment_duration_s += duration_s

    def _check_enrollment_progress(self) -> None:
        if len(self._speakers) != 1:
            return
        state = next(iter(self._speakers.values()))
        if state.enrollment_duration_s >= self._enrollment_target_s:
            self._fire_enrollment(state)

    def _fire_enrollment(self, state: _SpeakerState) -> None:
        if (
            self._enrollment_fired
            or self._on_enrollment_captured is None
            or self._call_contact_id is None
            or self._call_contact_enrolled
            or not state.enrollment_audio
        ):
            return
        self._enrollment_fired = True
        pcm = np.concatenate(state.enrollment_audio)
        sample_rate = state.enrollment_sample_rate
        task = asyncio.create_task(self._emit_enrollment(pcm, sample_rate))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _emit_enrollment(self, pcm: np.ndarray, sample_rate: int) -> None:
        embedding = await self._embedder.embed(pcm, sample_rate)
        wav_bytes = pcm_to_wav_bytes(pcm, sample_rate)
        fd, wav_path = tempfile.mkstemp(prefix="voice_enroll_", suffix=".wav")
        with os.fdopen(fd, "wb") as f:
            f.write(wav_bytes)
        duration_s = len(pcm) / sample_rate
        self._on_enrollment_captured(embedding, wav_path, duration_s)

    def _check_suggestion(self) -> None:
        if (
            self._suggestion_fired
            or self._on_enrollment_suggested is None
            or self._call_contact_id is None
            or self._call_contact_enrolled
            or len(self._speakers) < 2
        ):
            return
        self._suggestion_fired = True
        self._on_enrollment_suggested(len(self._speakers))

    async def finalize(self) -> None:
        """Call-end hook: flush pending work and fire a partial enrollment."""
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)
        if not self._enrollment_fired and len(self._speakers) == 1:
            state = next(iter(self._speakers.values()))
            if state.enrollment_duration_s >= self._enrollment_min_s:
                self._fire_enrollment(state)
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    # ── resolution ───────────────────────────────────────────────────────

    def resolve(self, speaker_id: str | None) -> SpeakerResolution | None:
        """Resolve a diarization speaker id to a contact or anonymous label."""
        if not speaker_id:
            return None
        state = self._speakers.get(speaker_id)
        if state is None:
            return None
        if state.pinned_contact_id is not None:
            return SpeakerResolution(
                contact_id=state.pinned_contact_id,
                verified=True,
            )
        if state.anonymous_label:
            return SpeakerResolution(label=state.anonymous_label)
        return None
