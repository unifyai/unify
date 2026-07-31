"""Speaker identification: embeddings, matching, and per-call speaker tracking.

The live voice pipeline stays untouched; everything here runs off the hot
path. Audio frames are teed into a ring buffer while they stream to STT, and
each final diarized transcript triggers an embedding computation in a worker
thread. Embeddings pin Deepgram's per-call anonymous speaker ids (S0, S1, …)
to enrolled contacts, and accumulate auto-enrollments for single-speaker
calls.

Attribution only: labels and enrollment never gate the conversation loop —
every voice on a call ends turns, triggers replies, and may interrupt.
"""

from __future__ import annotations

import asyncio
import io
import logging
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

# Plain stdlib logger: this module is imported by the LiveKit voice-agent child
# process, where the heavyweight ``unify.logger`` import chain is best avoided.
# Handlers are inherited from whichever process hosts it.
_log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Model management
# ─────────────────────────────────────────────────────────────────────────────

# Carries ``{contact_id: embedding}`` to the legacy per-call subprocess, which
# has no LiveKit job metadata to ride along in. JSON-encoded, same shape as the
# ``voice_profiles`` metadata key on the worker path.
VOICE_PROFILES_ENV = "VOICE_PROFILES"

SPEAKER_MODEL_NAME = "wespeaker_en_voxceleb_CAM++.onnx"
SPEAKER_MODEL_URL = (
    "https://github.com/k2-fsa/sherpa-onnx/releases/download/"
    f"speaker-recongition-models/{SPEAKER_MODEL_NAME}"
)

# Cosine-similarity acceptance threshold for pinning an anonymous speaker id
# to an enrolled contact. CAM++ VoxCeleb embeddings of the same speaker across
# telephone-band audio typically score 0.6-0.8; different speakers < 0.4.
SPEAKER_MATCH_THRESHOLD = 0.55

# Cosine-similarity threshold for a segment to join an existing within-id voice
# cluster rather than seed a new one. A single diarization id can carry more
# than one physically co-located voice (the STT engine under-splits); each such
# voice becomes its own cluster. Set below SPEAKER_MATCH_THRESHOLD so ordinary
# within-speaker variation (typically > 0.6) always merges, but well above the
# different-speaker floor (~0.4) so a genuinely different voice spawns a cluster.
CLUSTER_JOIN_SIM = 0.5

# Cosine-similarity threshold for treating clusters under *different*
# diarization ids as the same physical voice when gating auto-enrollment.
# Streaming diarization often over-splits one talker into S0/S1/…; without this
# merge those ids each count as a distinct voice and spuriously block
# enrollment.
#
# This asks the same question as ``CLUSTER_JOIN_SIM`` — "is this the same
# voice?" — only across diarization ids rather than within one, so the two are
# deliberately tied together. They previously diverged: this sat at 0.3, below
# any plausible different-speaker score, which made the merge unable to keep
# *anyone* apart. Since the merge is what gates auto-enrollment, a two-person
# call read as one voice and enrolled a blended voiceprint.
#
# PROVISIONAL: the same-speaker (0.6-0.8) and different-speaker (<0.4) bands
# these thresholds assume are inherited, not measured. Validate against a real
# multi-speaker corpus via ``$UNIFY_SPEAKER_TEST_CORPUS`` before treating the
# value as settled — see tests/conversation_manager/voice/speaker_corpus.py.
CROSS_ID_MERGE_SIM = CLUSTER_JOIN_SIM

# Minimum accumulated speech before a voice cluster counts as a distinct
# person for enrollment/suggestion gating. Without a floor, one noisy or
# clipped segment seeds a cluster that reads as a whole extra speaker —
# blocking auto-enrollment and firing the "multiple voices" suggestion on what
# is really a single-speaker call.
MIN_VOICE_DURATION_S = 2.0

# Auto-enrollment bounds (seconds of accumulated speech from a single voice).
ENROLLMENT_TARGET_S = 60.0
ENROLLMENT_MIN_S = 15.0

# Per-segment slicing bounds around a final transcript.
SEGMENT_MAX_S = 15.0

# Minimum audio behind one embedding. CAM++ pools frame statistics, so a short
# slice does not merely embed *noisily* — it embeds somewhere else entirely.
# Measured against the real extractor, a slice scored against its own speaker's
# profile: 0.8s -> 0.20, 1.0s -> 0.31, 1.5s -> 0.41, 2.0s -> 0.60, 3.0s -> 0.74.
# Below ~2s the result is unusable and averaging does not rescue it (a centroid
# of fifteen 0.8s segments plateaus near 0.32, never reaching the match
# threshold). Finals shorter than this are buffered per diarization id rather
# than discarded, so backchannels still contribute instead of seeding phantom
# clusters. See tests/conversation_manager/voice/test_speaker_id_real_model.py.
SEGMENT_MIN_S = 2.0

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
    """Running duration-weighted centroid of embeddings for one voice cluster.

    A cluster holds a single acoustic identity: cross-voice separation is done
    one level up (a diarization id owns a *set* of these), so this accumulator
    stays a plain running mean with no rejection logic.
    """

    _sum: np.ndarray | None = None
    total_duration_s: float = 0.0
    segments: int = 0

    def similarity(self, embedding: np.ndarray) -> float:
        """Cosine similarity of an embedding to the current centroid.

        An empty accumulator returns 1.0 so its first segment always seeds it.
        """
        centroid = self.centroid
        if centroid is None:
            return 1.0
        return cosine_similarity(embedding, centroid)

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


# Provenance of a resolved speaker display label. A transcript row carries its
# source so it is self-describing -- "is voice fingerprinting actually working?"
# is answerable from the row alone.
#
# Authority ordering is expressed by the order ``_resolve_speaker`` in the call
# script consumes these, highest first: voice pin, then platform participant,
# then org-call roster, then the anonymous placeholder. It is deliberately not
# also stated as a table here; two statements of one ordering drift apart, and
# the one nothing executes is the one that goes stale.
#
# Stored rows from earlier runtimes carry sources no longer produced --
# ``dom_meet_map`` and ``dom_active_speaker`` (scraped from a meeting UI in a
# browser we no longer run) and ``google_meet_transcript``. Readers should treat
# an unrecognised source as a real name of unknown provenance, ranked below the
# ones above.
LABEL_SOURCE_VOICE_PIN = "voice_pin"
# The meeting platform's own name for a participant, reported by the meeting
# backend rather than read off the screen. Ranked above ``meet_roster`` because
# it is the same kind of claim from a better source, and below ``voice_pin``
# because an enrolled-voice match additionally resolves to a contact id, which a
# display name does not.
LABEL_SOURCE_RECALL_PARTICIPANT = "recall_participant"
LABEL_SOURCE_MEET_ROSTER = "meet_roster"
# The only source that is a placeholder ("Speaker N") rather than a real name --
# consumers key off it instead of "no voice match".
LABEL_SOURCE_ANONYMOUS = "anonymous"


@dataclass
class SpeakerResolution:
    """Resolution of a single diarized utterance.

    Attribution is per *voice cluster*, not per diarization id: when one id
    carries several co-located voices, the resolution names the cluster the
    current utterance's audio actually joined. ``provisional`` marks that the
    id spans more than one voice cluster, so downstream consumers know the
    diarization id alone is not a reliable speaker key. ``source`` records how
    the identity was derived (``LABEL_SOURCE_VOICE_PIN`` for an embedding match,
    ``LABEL_SOURCE_ANONYMOUS`` for a minted "Speaker N"); the platform-reported
    sources are stamped one level up in the call script, not here.
    """

    contact_id: Optional[int] = None
    label: Optional[str] = None
    verified: bool = False
    provisional: bool = False
    source: Optional[str] = None


@dataclass
class _VoiceCluster:
    """One acoustic identity within a diarization id.

    A diarization id under-split by the STT engine can hold several of these,
    one per physically co-located voice. Each carries its own centroid and its
    own resolved identity (an enrolled-contact pin or a minted anonymous label).
    """

    accumulator: CentroidAccumulator = field(default_factory=CentroidAccumulator)
    pinned_contact_id: Optional[int] = None
    anonymous_label: Optional[str] = None


@dataclass
class _SpeakerState:
    """Per-diarization-id state: a set of voice clusters plus enrollment audio.

    ``last_cluster`` is the cluster the most recently processed segment joined;
    ``resolve`` reports on it so the answer tracks the voice that just spoke.
    Enrollment audio is accumulated at the id level but only while the id holds
    a single cluster, so a co-located second voice never poisons the voiceprint.
    """

    clusters: list[_VoiceCluster] = field(default_factory=list)
    last_cluster: Optional[_VoiceCluster] = None
    enrollment_audio: list[np.ndarray] = field(default_factory=list)
    enrollment_duration_s: float = 0.0
    enrollment_sample_rate: int = ENROLLMENT_SAMPLE_RATE
    # Finals too short to embed on their own, held at ENROLLMENT_SAMPLE_RATE
    # until they add up to SEGMENT_MIN_S. Bounded: flushed as soon as they do.
    pending_audio: list[np.ndarray] = field(default_factory=list)
    pending_duration_s: float = 0.0


class SpeakerTracker:
    """Per-call speaker attribution and auto-enrollment.

    Feed it raw audio (`add_audio`) and final diarized transcripts
    (`observe_final_transcript`); query it with `resolve(speaker_id)`.

    Callbacks fire at most once per call:
    - ``on_enrollment_captured(embedding, wav_path, duration_s)`` when a
      single-voice call has accumulated enough speech to enroll the contact.
    - ``on_enrollment_suggested(num_speakers)`` when multiple voices are heard
      but the call contact has no enrollment to disambiguate them.

    ``multi_party`` marks a call with many concurrent speakers and no single
    primary (a meet). It only affects *labeling*: every distinct voice cluster
    gets a stable anonymous "Speaker N" identity even when nobody is enrolled,
    so meet transcripts are per-speaker attributable from the voice track alone.
    Auto-enrollment stays single-voice-gated regardless, so a meet never enrolls
    an arbitrary participant as the call contact.

    Enrollment gating counts *physically* distinct voices: clusters under
    different diarization ids whose centroids clear ``cross_id_merge_sim``
    collapse into one voice, so STT over-splits do not block auto-enrollment.
    """

    def __init__(
        self,
        *,
        embedder: SpeakerEmbedder,
        enrolled_profiles: dict[int, np.ndarray],
        call_contact_id: int | None,
        multi_party: bool = False,
        match_threshold: float = SPEAKER_MATCH_THRESHOLD,
        cross_id_merge_sim: float = CROSS_ID_MERGE_SIM,
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
        self._multi_party = multi_party
        self._match_threshold = match_threshold
        self._cross_id_merge_sim = cross_id_merge_sim
        self._enrollment_target_s = enrollment_target_s
        self._enrollment_min_s = enrollment_min_s
        self._on_enrollment_captured = on_enrollment_captured
        self._on_enrollment_suggested = on_enrollment_suggested

        self._ring = AudioRingBuffer()
        self._speakers: dict[str, _SpeakerState] = {}
        self._last_final_ts: float = 0.0
        # "Speaker 1" is reserved for the enrolled primary caller only when there
        # is one (a phone contact who resolves by name, never by ordinal). With
        # no reserved primary — an unenrolled multi-party meet — the sequence
        # starts at 1 so the first unidentified voice is "Speaker 1", not a
        # gap-leaving "Speaker 2".
        self._next_anonymous_index = 2 if self._call_contact_enrolled else 1
        self._enrollment_fired = False
        self._suggestion_fired = False
        self._pending_tasks: set[asyncio.Task] = set()

        # Attribution runs entirely off fire-and-forget tasks whose exceptions
        # nothing retrieves, and every upstream failure (absent model, empty
        # profile map) degrades silently — so the feature can be dead in
        # production with no signal at all. These counters plus ``diagnostics``
        # are that signal.
        self._segments_observed = 0
        self._segments_buffered = 0
        self._segments_dropped = 0
        self._segments_embedded = 0
        self._embed_failures = 0

        _log.info(
            "SpeakerTracker: %d enrolled profile(s), call_contact=%s "
            "(enrolled=%s), multi_party=%s",
            len(self._enrolled),
            self._call_contact_id,
            self._call_contact_enrolled,
            self._multi_party,
        )

    def add_enrolled_profiles(self, profiles: dict[int, list[float]]) -> int:
        """Merge in profiles that were not known when the call started.

        Enrolled profiles are otherwise a snapshot taken at dispatch, so anyone
        who joins a multi-party call later cannot be voice-pinned however good
        their enrollment is. Returns the number newly added.

        Only unknown contacts are added: an existing profile is left alone so a
        late roster push cannot disturb pins already made on this call. Voices
        already clustered are re-scored against the enlarged set on their next
        segment, so a late joiner who has already spoken is picked up without
        replaying anything.
        """
        added = 0
        for contact_id, vector in (profiles or {}).items():
            try:
                cid = int(contact_id)
            except (TypeError, ValueError):
                continue
            if cid in self._enrolled:
                continue
            self._enrolled[cid] = np.asarray(vector, dtype=np.float32)
            added += 1
        if added:
            _log.info(
                "SpeakerTracker: +%d enrolled profile(s) mid-call (%d total)",
                added,
                len(self._enrolled),
            )
        return added

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
        """Register a final diarized transcript; schedules embedding work.

        Finals carrying less than ``SEGMENT_MIN_S`` of audio — backchannels
        like "yeah" or "mm-hm", which are a large share of real call turns —
        are accumulated against their diarization id instead of embedded on
        their own, and flushed once they add up. Embedding them individually
        produced vectors unrelated to the speaker, which then seeded phantom
        voice clusters and mislabelled the caller's own short turns.
        """
        end_ts = end_ts if end_ts is not None else time.time()
        window_start = max(self._last_final_ts, end_ts - SEGMENT_MAX_S)
        self._last_final_ts = end_ts
        if not speaker_id:
            return
        self._segments_observed += 1
        pcm, sample_rate = self._ring.slice(window_start, end_ts)
        duration_s = len(pcm) / sample_rate if sample_rate else 0.0
        if duration_s <= 0.0:
            return

        state = self._speakers.setdefault(speaker_id, _SpeakerState())
        if state.pending_audio or duration_s < SEGMENT_MIN_S:
            # Normalized to one rate so buffered pieces can be concatenated;
            # the extractor resamples internally either way.
            state.pending_audio.append(
                resample_pcm(pcm, sample_rate, ENROLLMENT_SAMPLE_RATE),
            )
            state.pending_duration_s += duration_s
            if state.pending_duration_s < SEGMENT_MIN_S:
                self._segments_buffered += 1
                return
            pcm = np.concatenate(state.pending_audio)
            sample_rate = ENROLLMENT_SAMPLE_RATE
            duration_s = state.pending_duration_s
            state.pending_audio = []
            state.pending_duration_s = 0.0

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
        try:
            embedding = await self._embedder.embed(pcm, sample_rate)
        except Exception:
            # Nothing awaits this task's result, so without an explicit log an
            # unusable extractor (missing model, bad audio) silently disables
            # attribution for the whole call.
            self._embed_failures += 1
            _log.exception(
                "speaker embedding failed for %s (%.1fs @ %dHz); "
                "attribution degraded for this segment",
                speaker_id,
                duration_s,
                sample_rate,
            )
            return
        self._segments_embedded += 1
        state = self._speakers.setdefault(speaker_id, _SpeakerState())
        cluster = self._assign_cluster(state, embedding, duration_s)
        self._try_pin(cluster)
        state.last_cluster = cluster
        if len(state.clusters) == 1:
            # A second cluster means a co-located voice shares this id; only the
            # sole-voice case is safe to feed into the enrolled voiceprint.
            self._accumulate_enrollment(state, pcm, sample_rate, duration_s)
        self._check_enrollment_progress()
        self._check_suggestion()

    def _assign_cluster(
        self,
        state: _SpeakerState,
        embedding: np.ndarray,
        duration_s: float,
    ) -> _VoiceCluster:
        """Join the segment to its nearest within-id cluster, or seed a new one.

        A segment merges into the closest cluster when their cosine similarity
        clears ``CLUSTER_JOIN_SIM``; otherwise it is a different co-located voice
        and gets its own cluster. This is the core fix for a diarization id that
        the STT engine failed to split into distinct speakers.
        """
        best, best_sim = None, 0.0
        for cluster in state.clusters:
            sim = cluster.accumulator.similarity(embedding)
            if sim > best_sim:
                best, best_sim = cluster, sim
        if best is not None and best_sim >= CLUSTER_JOIN_SIM:
            best.accumulator.add(embedding, duration_s)
            return best
        cluster = _VoiceCluster()
        cluster.accumulator.add(embedding, duration_s)
        state.clusters.append(cluster)
        return cluster

    def _try_pin(self, cluster: _VoiceCluster) -> None:
        # With no enrolled profiles there is nothing to pin against, but a
        # multi-party call still needs a per-cluster anonymous label, so fall
        # through to the minting branch below instead of bailing.
        if not self._enrolled and not self._multi_party:
            return
        centroid = cluster.accumulator.centroid
        if centroid is None:
            return
        best_cid, best_score = None, 0.0
        for cid, profile in self._enrolled.items():
            score = cosine_similarity(centroid, profile)
            if score > best_score:
                best_cid, best_score = cid, score
        if best_cid is not None and best_score >= self._match_threshold:
            # The cluster centroid is re-scored on every segment, so pinning is
            # not a one-way latch: a pin is revoked below if the cluster's voice
            # later drifts away from every enrolled profile.
            cluster.pinned_contact_id = best_cid
        else:
            cluster.pinned_contact_id = None
            if cluster.anonymous_label is None and (
                self._call_contact_enrolled or self._multi_party
            ):
                # This voice does not match an enrolled profile: mint a stable
                # session-scoped anonymous identity for the cluster. Warranted
                # either because the call contact is enrolled (so a non-matching
                # voice is confidently *someone else*) or because this is a
                # multi-party call (a meet) where every distinct voice needs a
                # per-speaker label even with nobody enrolled. The label is kept
                # even if the cluster is later pinned (resolution prefers the
                # pin), so the ordinal never churns.
                cluster.anonymous_label = f"Speaker {self._next_anonymous_index}"
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

    def _distinct_voice_count(self) -> int:
        """Physically distinct voices after merging cross-id near-duplicates.

        Within an id, ``CLUSTER_JOIN_SIM`` already splits co-located voices into
        separate clusters. Across ids, streaming diarization may over-split one
        talker (S0 vs S1); centroids that clear ``cross_id_merge_sim`` collapse
        into a single voice for enrollment / suggestion gating.

        Two properties this count has to have, because auto-enrollment is gated
        on it and a wrong answer writes a permanent voiceprint:

        *Deterministic.* Groups are seeded from the longest-established voice
        down, so the answer does not depend on ``dict`` iteration order.

        *No chaining.* A centroid joins a group only if it clears the threshold
        against **every** member, not just the nearest one. Single-linkage would
        let A~B and B~C collapse A and C together even when they are plainly
        different people, which is precisely the merge that must not happen.

        Clusters below ``MIN_VOICE_DURATION_S`` are ignored: too little audio to
        assert a distinct person, and counting them turns one clipped segment
        into a phantom speaker that blocks enrollment.
        """
        established = sorted(
            (
                (cluster.accumulator.centroid, cluster.accumulator.total_duration_s)
                for state in self._speakers.values()
                for cluster in state.clusters
                if cluster.accumulator.centroid is not None
                and cluster.accumulator.total_duration_s >= MIN_VOICE_DURATION_S
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        groups: list[list[np.ndarray]] = []
        for centroid, _duration in established:
            for group in groups:
                if all(
                    cosine_similarity(centroid, member) >= self._cross_id_merge_sim
                    for member in group
                ):
                    group.append(centroid)
                    break
            else:
                groups.append([centroid])
        return len(groups)

    def _single_voice_enrollment_audio(
        self,
    ) -> tuple[list[np.ndarray], int, float] | None:
        """Enrollment PCM across all ids when exactly one physical voice exists.

        Speech may be split across over-split diarization ids; combine their
        enrollment buffers so duration thresholds see the full single-voice
        total rather than a per-id fragment.
        """
        if self._distinct_voice_count() != 1:
            return None
        audio_parts: list[np.ndarray] = []
        sample_rate = ENROLLMENT_SAMPLE_RATE
        duration_s = 0.0
        for state in self._speakers.values():
            if not state.enrollment_audio:
                continue
            audio_parts.extend(state.enrollment_audio)
            sample_rate = state.enrollment_sample_rate
            duration_s += state.enrollment_duration_s
        if not audio_parts:
            return None
        return audio_parts, sample_rate, duration_s

    def _check_enrollment_progress(self) -> None:
        material = self._single_voice_enrollment_audio()
        if material is None:
            return
        audio_parts, sample_rate, duration_s = material
        if duration_s >= self._enrollment_target_s:
            self._fire_enrollment(audio_parts, sample_rate)

    def _fire_enrollment(
        self,
        audio_parts: list[np.ndarray],
        sample_rate: int,
    ) -> None:
        if (
            self._enrollment_fired
            or self._on_enrollment_captured is None
            or self._call_contact_id is None
            or self._call_contact_enrolled
            or not audio_parts
        ):
            return
        self._enrollment_fired = True
        pcm = np.concatenate(audio_parts)
        task = asyncio.create_task(self._emit_enrollment(pcm, sample_rate))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _emit_enrollment(self, pcm: np.ndarray, sample_rate: int) -> None:
        duration_s = len(pcm) / sample_rate
        try:
            embedding = await self._embedder.embed(pcm, sample_rate)
            wav_bytes = pcm_to_wav_bytes(pcm, sample_rate)
            fd, wav_path = tempfile.mkstemp(prefix="voice_enroll_", suffix=".wav")
            with os.fdopen(fd, "wb") as f:
                f.write(wav_bytes)
            self._on_enrollment_captured(embedding, wav_path, duration_s)
        except Exception:
            # Fire-and-forget like the segment path: log or the enrollment is
            # lost with no trace, and the contact stays silently unenrolled.
            _log.exception(
                "voice enrollment capture failed for contact %s (%.0fs)",
                self._call_contact_id,
                duration_s,
            )
            return
        _log.info(
            "voice enrollment captured for contact %s (%.0fs of speech)",
            self._call_contact_id,
            duration_s,
        )

    def _check_suggestion(self) -> None:
        distinct = self._distinct_voice_count()
        if (
            self._suggestion_fired
            or self._on_enrollment_suggested is None
            or self._call_contact_id is None
            or self._call_contact_enrolled
            or distinct < 2
        ):
            return
        self._suggestion_fired = True
        self._on_enrollment_suggested(distinct)

    async def await_pending(self) -> None:
        """Flush in-flight embedding work.

        Callers that need per-utterance attribution (``resolve`` reports on the
        last *processed* segment) await this after a final transcript so the
        current utterance's segment has been clustered before they resolve.
        """
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    def diagnostics(self) -> dict:
        """Per-call attribution counters, for the end-of-call summary log.

        Answers "did voice fingerprinting actually do anything on this call?"
        without needing the transcript: no enrolled profiles, every segment
        rejected as too short, or embeddings failing all present as zeros here.
        """
        pins = sum(
            1
            for state in self._speakers.values()
            for cluster in state.clusters
            if cluster.pinned_contact_id is not None
        )
        return {
            "enrolled_profiles": len(self._enrolled),
            "diarization_ids": len(self._speakers),
            "clusters": sum(len(s.clusters) for s in self._speakers.values()),
            "distinct_voices": self._distinct_voice_count(),
            "pinned_clusters": pins,
            "segments_observed": self._segments_observed,
            "segments_buffered": self._segments_buffered,
            "segments_dropped": self._segments_dropped,
            "segments_embedded": self._segments_embedded,
            "embed_failures": self._embed_failures,
            "enrollment_fired": self._enrollment_fired,
            "suggestion_fired": self._suggestion_fired,
        }

    def _flush_pending_segments(self) -> None:
        """Embed any buffered short finals that reached the duration floor.

        Anything still under it is dropped rather than embedded: a call ending
        mid-backchannel must not contribute an unusable vector to the last
        speaker's cluster.
        """
        for speaker_id, state in self._speakers.items():
            if not state.pending_audio:
                continue
            pcm = np.concatenate(state.pending_audio)
            duration_s = state.pending_duration_s
            state.pending_audio = []
            state.pending_duration_s = 0.0
            if duration_s < SEGMENT_MIN_S:
                self._segments_dropped += 1
                continue
            task = asyncio.create_task(
                self._process_segment(
                    speaker_id,
                    pcm,
                    ENROLLMENT_SAMPLE_RATE,
                    duration_s,
                ),
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def finalize(self) -> None:
        """Call-end hook: flush pending work and fire a partial enrollment."""
        await self.await_pending()
        self._flush_pending_segments()
        await self.await_pending()
        if not self._enrollment_fired:
            material = self._single_voice_enrollment_audio()
            if material is not None:
                audio_parts, sample_rate, duration_s = material
                if duration_s >= self._enrollment_min_s:
                    self._fire_enrollment(audio_parts, sample_rate)
        await self.await_pending()
        stats = self.diagnostics()
        _log.info(
            "speaker attribution summary: %s",
            " ".join(f"{k}={v}" for k, v in stats.items()),
        )

    # ── resolution ───────────────────────────────────────────────────────

    def resolve(self, speaker_id: str | None) -> SpeakerResolution | None:
        """Resolve a diarization id to the identity of the voice that just spoke.

        Attribution is to ``last_cluster`` — the cluster the most recently
        processed segment joined — so when an id carries several co-located
        voices the answer names the specific one, not a blurred average.
        ``provisional`` is set whenever the id spans more than one cluster.

        A provisional pin is still returned — it is the best guess available
        for routing, and dropping it would lose attribution entirely — but it
        is **not** reported as verified. ``verified`` is a claim that this
        utterance's voice was positively matched, and once an id is known to
        carry more than one voice the tracker cannot make that claim about any
        single utterance under it.
        """
        if not speaker_id:
            return None
        state = self._speakers.get(speaker_id)
        if state is None or state.last_cluster is None:
            return None
        cluster = state.last_cluster
        provisional = len(state.clusters) > 1
        if cluster.pinned_contact_id is not None:
            return SpeakerResolution(
                contact_id=cluster.pinned_contact_id,
                verified=not provisional,
                provisional=provisional,
                source=LABEL_SOURCE_VOICE_PIN,
            )
        if cluster.anonymous_label:
            return SpeakerResolution(
                label=cluster.anonymous_label,
                provisional=provisional,
                source=LABEL_SOURCE_ANONYMOUS,
            )
        return None
