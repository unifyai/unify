"""
tests/conversation_manager/voice/test_speaker_id_real_model.py
==============================================================

Tuning tests for speaker identification, run against the real CAM++ extractor.

``test_speaker_id.py`` drives a stub embedder that returns orthogonal 2-d unit
vectors keyed on mean amplitude. That proves the tracker's *wiring* — ring
buffer, clustering, pinning, enrollment gating — but it cannot prove its
*tuning*: under the stub every similarity threshold trivially separates the
two "voices", and embeddings do not depend on segment length at all. This
module covers what the stub cannot.

Two classes of test, with very different corpus requirements:

*Self-comparison* — how much audio the model needs before an embedding of a
speaker resembles another embedding of that same speaker (``SEGMENT_MIN_S``).
Synthesiser artifacts are constant within one voice, so these are sound on
any corpus, including the generated one.

*Cross-speaker* — whether each threshold separates different people
(``SPEAKER_MATCH_THRESHOLD``, ``CLUSTER_JOIN_SIM``, ``CROSS_ID_MERGE_SIM``)
and whether the tracker keeps two speakers apart. These are only meaningful on
a corpus the model can actually separate, so they sit behind the measured
``separable_corpus`` gate rather than an assumption. Screening the macOS
``say`` voices found 16 of 21 ranking some *other* voice above themselves —
CAM++ largely measures the synthesiser there, not the speaker — so on a
generated corpus these skip. Supply human recordings via
``$UNIFY_SPEAKER_TEST_CORPUS`` to run them.

The remaining ``xfail(strict=True)`` markers are all cross-speaker, covering
defects that cannot be confirmed or fixed without a separable corpus. They
record the gap rather than hiding it; strict means a fix turns them into
failures until the marker is removed, which is the intended signal. The
duration defects they used to sit alongside are fixed, so those tests now
assert the corrected behaviour directly.

Marked ``slow``: real inference, tens of embeddings.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pytest

from unify.conversation_manager import speaker_id
from unify.conversation_manager.speaker_id import (
    CLUSTER_JOIN_SIM,
    CROSS_ID_MERGE_SIM,
    SEGMENT_MIN_S,
    SPEAKER_MATCH_THRESHOLD,
    CentroidAccumulator,
    SpeakerEmbedder,
    SpeakerTracker,
    cosine_similarity,
)

from .speaker_corpus import (
    CORPUS_ENV,
    SAMPLE_RATE,
    is_real_corpus,
    load_corpus,
    unavailable_reason,
)

_log = logging.getLogger(__name__)

_MODEL_PATH = speaker_id.ensure_speaker_model(download=False)

# Only look for (and, on macOS, generate) the corpus once the model is known to
# be present — otherwise collection pays for ~20 `say` renders it cannot use.
_CORPUS_REASON = unavailable_reason() if _MODEL_PATH is not None else None

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        _MODEL_PATH is None,
        reason="speaker embedding model not cached locally",
    ),
    pytest.mark.skipif(_CORPUS_REASON is not None, reason=_CORPUS_REASON or ""),
]

# A "profile" is one whole passage, standing in for an enrollment; segments are
# sliced out of the *other* passage, so a speaker is never scored against their
# own identical audio. Scoring against different words is the realistic case
# and is markedly harder than re-scoring the same recording.
_PROFILE_PASSAGE = "a"
_SEGMENT_PASSAGE = "b"

# A segment at the accepted floor must beat a half-length one by at least this
# much for the duration effect to count as real rather than noise.
_MONOTONIC_MARGIN = 0.15

# At ``SEGMENT_MIN_S`` most slices of a speaker must match that speaker's own
# profile; at half the floor almost none may. These bracket the regime change
# the floor is placed at — measured across every slice position of every corpus
# voice: 0.8s -> 0% clearing threshold, 1.0s -> 0%, 1.5s -> 12%, 2.0s -> 65%,
# 4.0s -> 83%. Deliberately loose: individual slices vary a lot with which
# words they happen to contain, so only the distribution is meaningful.
_USABLE_FRACTION_AT_FLOOR = 0.5
_UNUSABLE_FRACTION_BELOW_FLOOR = 0.15


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def embedder() -> SpeakerEmbedder:
    return SpeakerEmbedder(_MODEL_PATH)


@pytest.fixture(scope="module")
def corpus() -> dict[str, dict[str, np.ndarray]]:
    return load_corpus()


@pytest.fixture(scope="module")
def profiles(embedder, corpus) -> dict[str, np.ndarray]:
    """Full-passage embedding per speaker — the stand-in for an enrollment."""
    return {
        name: embedder.embed_sync(passages[_PROFILE_PASSAGE], SAMPLE_RATE)
        for name, passages in corpus.items()
    }


@dataclass(frozen=True)
class _CorpusQuality:
    """How well the model separates this corpus's voices.

    ``rank1`` is the fraction of speakers whose own profile is the closest
    match to their other passage — the property cross-speaker tests depend on.
    ``gap`` is the margin between the same-speaker floor and the
    different-speaker ceiling; a non-positive gap means the two distributions
    overlap and no threshold can separate them.
    """

    rank1: float
    same_min: float
    same_mean: float
    diff_max: float
    diff_mean: float

    @property
    def gap(self) -> float:
        return self.same_min - self.diff_max

    @property
    def is_separable(self) -> bool:
        return self.rank1 == 1.0 and self.gap > 0.0


@pytest.fixture(scope="module")
def quality(embedder, corpus, profiles) -> _CorpusQuality:
    names = sorted(corpus)
    probes = {
        name: embedder.embed_sync(corpus[name][_SEGMENT_PASSAGE], SAMPLE_RATE)
        for name in names
    }
    correct = sum(
        max(names, key=lambda m: cosine_similarity(probes[n], profiles[m])) == n
        for n in names
    )
    same = [cosine_similarity(probes[n], profiles[n]) for n in names]
    diff = [
        cosine_similarity(probes[a], profiles[b])
        for i, a in enumerate(names)
        for b in names[i + 1 :]
    ]
    result = _CorpusQuality(
        rank1=correct / len(names),
        same_min=min(same),
        same_mean=float(np.mean(same)),
        diff_max=max(diff),
        diff_mean=float(np.mean(diff)),
    )
    _log.info(
        "corpus=%s voices=%d rank-1=%.0f%% | same-speaker min=%.3f mean=%.3f "
        "| different-speaker max=%.3f mean=%.3f | gap=%+.3f",
        "real" if is_real_corpus() else "generated",
        len(names),
        result.rank1 * 100,
        result.same_min,
        result.same_mean,
        result.diff_max,
        result.diff_mean,
        result.gap,
    )
    return result


@pytest.fixture(scope="module")
def separable_corpus(corpus, quality) -> dict[str, dict[str, np.ndarray]]:
    """The corpus, but only for tests that need speakers told apart."""
    if not quality.is_separable:
        pytest.skip(
            f"corpus cannot separate speakers (rank-1 {quality.rank1:.0%}, "
            f"gap {quality.gap:+.3f}): cross-speaker thresholds cannot be "
            f"measured against it. Point ${CORPUS_ENV} at human recordings.",
        )
    return corpus


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _slice(pcm: np.ndarray, seconds: float, index: int = 0) -> np.ndarray:
    """A ``seconds``-long slice taken ``index`` slices into the utterance.

    Offset from the start so slices land on voiced speech rather than the
    leading silence every render begins with.
    """
    n = int(seconds * SAMPLE_RATE)
    start = int(1.0 * SAMPLE_RATE) + index * n
    if start + n > len(pcm):
        raise ValueError("corpus utterance too short for this slice")
    return pcm[start : start + n]


class _CallSim:
    """Feeds a SpeakerTracker on a synthetic timeline, as the live flow does.

    Mirrors ``_feed_segment`` in ``test_speaker_id.py``: audio is appended to
    the ring with an explicit ``end_ts`` and the matching final transcript is
    registered, so no wall clock is involved.
    """

    def __init__(self, tracker: SpeakerTracker) -> None:
        self._tracker = tracker
        self._now = 1_000.0
        self._offsets: dict[str, int] = {}

    async def utterance(
        self,
        diarization_id: str,
        pcm: np.ndarray,
        seconds: float,
        *,
        gap_s: float = 0.4,
    ) -> None:
        """Play ``seconds`` of ``pcm``, advancing through it on repeat calls."""
        n = int(seconds * SAMPLE_RATE)
        offset = self._offsets.get(diarization_id, 0)
        if offset + n > len(pcm):
            offset = 0
        chunk = pcm[offset : offset + n]
        self._offsets[diarization_id] = offset + n

        self._now += seconds
        self._tracker._ring.append(chunk, SAMPLE_RATE, end_ts=self._now)
        self._tracker.observe_final_transcript(diarization_id, end_ts=self._now)
        await self._tracker.await_pending()
        self._now += gap_s


def _make_tracker(
    embedder: SpeakerEmbedder,
    *,
    enrolled: dict[int, np.ndarray] | None = None,
    contact_id: int | None = 42,
    on_captured=None,
    on_suggested=None,
) -> SpeakerTracker:
    return SpeakerTracker(
        embedder=embedder,
        enrolled_profiles=enrolled or {},
        call_contact_id=contact_id,
        on_enrollment_captured=on_captured,
        on_enrollment_suggested=on_suggested,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sanity and corpus reporting
# ─────────────────────────────────────────────────────────────────────────────


def test_corpus_embeddings_are_unit_norm_and_deterministic(embedder, corpus):
    name = sorted(corpus)[0]
    pcm = corpus[name][_PROFILE_PASSAGE]
    first = embedder.embed_sync(pcm, SAMPLE_RATE)
    again = embedder.embed_sync(pcm, SAMPLE_RATE)

    assert first.ndim == 1 and first.size > 0
    assert float(np.linalg.norm(first)) == pytest.approx(1.0, abs=1e-3)
    assert cosine_similarity(first, again) > 0.999


def test_corpus_quality_is_measured_not_assumed(quality):
    """Records the corpus's separability; the cross-speaker gate reads it.

    A generated corpus is expected to fail ``is_separable`` — the assertion
    here is only that the measurement is well-formed, so the report always
    lands in the log for whoever is calibrating.
    """
    assert 0.0 <= quality.rank1 <= 1.0
    assert quality.diff_max < 0.999, "corpus voices are duplicates"


# ─────────────────────────────────────────────────────────────────────────────
# Segment duration: what the model needs before an embedding means anything
#
# Self-comparison only — sound on any corpus.
# ─────────────────────────────────────────────────────────────────────────────


def _match_fraction(embedder, corpus, profiles, seconds: float) -> float:
    """Fraction of all ``seconds``-long slices that match their own speaker.

    Averaged over every slice position of every voice: a single slice's score
    swings wildly with which words it happens to contain, so only the
    distribution says anything about duration.
    """
    scores: list[float] = []
    for name, passages in corpus.items():
        pcm = passages[_SEGMENT_PASSAGE]
        width = int(seconds * SAMPLE_RATE)
        for start in range(SAMPLE_RATE, len(pcm) - width, width):
            scores.append(
                cosine_similarity(
                    embedder.embed_sync(pcm[start : start + width], SAMPLE_RATE),
                    profiles[name],
                ),
            )
    assert scores, f"corpus too short to slice at {seconds}s"
    return sum(s >= SPEAKER_MATCH_THRESHOLD for s in scores) / len(scores)


def test_segment_floor_sits_above_the_unusable_regime(embedder, corpus, profiles):
    """``SEGMENT_MIN_S`` must be on the usable side of the duration cliff.

    CAM++ pools frame statistics, so below roughly two seconds an embedding is
    not a noisy version of the speaker — it is unrelated to them. This pins the
    floor to the regime change rather than to a hand-picked number.
    """
    at_floor = _match_fraction(embedder, corpus, profiles, SEGMENT_MIN_S)
    below = _match_fraction(embedder, corpus, profiles, SEGMENT_MIN_S / 2)

    assert at_floor >= _USABLE_FRACTION_AT_FLOOR, (
        f"only {at_floor:.0%} of {SEGMENT_MIN_S}s slices match their own "
        f"speaker; SEGMENT_MIN_S is too low"
    )
    assert below <= _UNUSABLE_FRACTION_BELOW_FLOOR, (
        f"{below:.0%} of {SEGMENT_MIN_S / 2}s slices already match — the "
        f"cliff has moved, so SEGMENT_MIN_S may be higher than it needs to be"
    )


def test_longer_segments_resemble_their_speaker_more_than_short_ones(
    embedder,
    corpus,
    profiles,
):
    """Per-speaker form of the same effect: the floor beats half the floor.

    Holds within each voice, so unlike the cross-speaker tests it does not
    depend on the corpus separating different people.
    """
    for name, passages in corpus.items():
        pcm = passages[_SEGMENT_PASSAGE]
        short = cosine_similarity(
            embedder.embed_sync(_slice(pcm, SEGMENT_MIN_S / 2), SAMPLE_RATE),
            profiles[name],
        )
        longer = cosine_similarity(
            embedder.embed_sync(_slice(pcm, SEGMENT_MIN_S), SAMPLE_RATE),
            profiles[name],
        )
        assert longer - short >= _MONOTONIC_MARGIN, (
            f"{name}: {SEGMENT_MIN_S / 2}s scored {short:.3f}, "
            f"{SEGMENT_MIN_S}s scored {longer:.3f} — expected at least "
            f"{_MONOTONIC_MARGIN} better"
        )


def test_centroid_of_shortest_segments_converges_on_its_speaker(
    embedder,
    corpus,
    profiles,
):
    name = sorted(corpus)[0]
    pcm = corpus[name][_SEGMENT_PASSAGE]
    accumulator = CentroidAccumulator()
    count = int((len(pcm) / SAMPLE_RATE - 1.0) // SEGMENT_MIN_S)
    for index in range(count):
        segment = _slice(pcm, SEGMENT_MIN_S, index)
        accumulator.add(embedder.embed_sync(segment, SAMPLE_RATE), SEGMENT_MIN_S)

    score = cosine_similarity(accumulator.centroid, profiles[name])
    assert score >= SPEAKER_MATCH_THRESHOLD, (
        f"{name}: centroid of {count} x {SEGMENT_MIN_S}s segments scored "
        f"{score:.3f} against its own profile"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Whole-tracker behaviour, single speaker
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_single_speaker_long_utterances_is_one_pinned_voice(
    embedder,
    corpus,
    profiles,
):
    """The happy path: one enrolled speaker, ordinary sentence-length turns."""
    name = sorted(corpus)[0]
    tracker = _make_tracker(embedder, enrolled={42: profiles[name]})
    sim = _CallSim(tracker)
    pcm = corpus[name][_SEGMENT_PASSAGE]

    resolutions = []
    for _ in range(4):
        await sim.utterance("S0", pcm, 2.5)
        resolutions.append(tracker.resolve("S0"))
    await tracker.finalize()

    assert len(tracker._speakers["S0"].clusters) == 1
    assert tracker._distinct_voice_count() == 1
    assert all(r is not None and r.contact_id == 42 for r in resolutions)
    assert not any(r.provisional for r in resolutions)


@pytest.mark.asyncio
async def test_single_speaker_with_backchannels_stays_one_voice(
    embedder,
    corpus,
    profiles,
):
    """A real caller mixes sentences with short acknowledgements.

    Before short finals were buffered, each "yeah" embedded as noise, missed
    ``CLUSTER_JOIN_SIM`` against the speaker's own cluster, and seeded a
    phantom second one — so the caller's own short turns came back labelled
    "Speaker 2" and ``provisional`` latched on for the rest of the call.
    """
    name = sorted(corpus)[0]
    tracker = _make_tracker(embedder, enrolled={42: profiles[name]})
    sim = _CallSim(tracker)
    pcm = corpus[name][_SEGMENT_PASSAGE]

    resolutions = []
    for seconds in (2.5, 0.9, 2.5, 1.0, 2.5):
        await sim.utterance("S0", pcm, seconds)
        resolutions.append(tracker.resolve("S0"))
    await tracker.finalize()

    assert len(tracker._speakers["S0"].clusters) == 1, (
        "one speaker split into "
        f"{len(tracker._speakers['S0'].clusters)} voice clusters"
    )
    assert all(
        r is not None and r.contact_id == 42 for r in resolutions
    ), f"attributions: {[(r.contact_id, r.label) for r in resolutions]}"


# ─────────────────────────────────────────────────────────────────────────────
# Cross-speaker: threshold placement and two-speaker calls
#
# Gated on `separable_corpus` — meaningless unless the model can tell this
# corpus's voices apart in the first place.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,value",
    [
        ("SPEAKER_MATCH_THRESHOLD", SPEAKER_MATCH_THRESHOLD),
        ("CLUSTER_JOIN_SIM", CLUSTER_JOIN_SIM),
        ("CROSS_ID_MERGE_SIM", CROSS_ID_MERGE_SIM),
    ],
)
def test_threshold_sits_between_the_distributions(
    separable_corpus,
    quality,
    name,
    value,
):
    """Every threshold must fall in the gap that separates the distributions."""
    assert quality.diff_max < value <= quality.same_min, (
        f"{name}={value} is outside the separating gap "
        f"({quality.diff_max:.3f}, {quality.same_min:.3f}]"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F1: CROSS_ID_MERGE_SIM=0.3 is far below any plausible "
        "different-speaker score, so _distinct_voice_count() collapses "
        "genuinely different people into one voice. That is the gate "
        "protecting auto-enrollment. Phase 1 raises it into the measured gap."
    ),
)
def test_cross_id_merge_sim_does_not_merge_different_speakers(
    separable_corpus,
    quality,
):
    assert CROSS_ID_MERGE_SIM > quality.diff_max, (
        f"CROSS_ID_MERGE_SIM={CROSS_ID_MERGE_SIM} merges different speakers "
        f"scoring up to {quality.diff_max:.3f}"
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason=(
        "F1: CROSS_ID_MERGE_SIM=0.3 collapses two genuinely different "
        "speakers on separate diarization ids into a single voice, so the "
        "count that gates auto-enrollment reads 1 instead of 2."
    ),
)
async def test_two_speakers_count_as_two_distinct_voices(
    embedder,
    separable_corpus,
    profiles,
):
    first, second = sorted(separable_corpus)[:2]
    tracker = _make_tracker(embedder, enrolled={42: profiles[first]})
    sim = _CallSim(tracker)

    for _ in range(3):
        await sim.utterance("S0", separable_corpus[first][_SEGMENT_PASSAGE], 2.5)
        await sim.utterance("S1", separable_corpus[second][_SEGMENT_PASSAGE], 2.5)
    await tracker.finalize()

    assert tracker._distinct_voice_count() == 2


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason=(
        "F1: with the voice count collapsed to 1, a two-person call passes "
        "the single-voice gate and auto-enrolls a voiceprint blended from "
        "both speakers. The stranger then matches the contact's stored "
        "profile above SPEAKER_MATCH_THRESHOLD on every future call, and "
        "auto-enrollment is write-once so it never self-corrects."
    ),
)
async def test_two_speakers_do_not_contaminate_auto_enrollment(
    embedder,
    separable_corpus,
    profiles,
):
    """An unenrolled contact must not be enrolled from a shared-room call."""
    first, second = sorted(separable_corpus)[:2]
    captured: dict = {}
    tracker = _make_tracker(
        embedder,
        enrolled={},  # contact not yet enrolled: auto-enrollment is armed
        on_captured=lambda emb, wav, dur: captured.update(embedding=emb),
    )
    sim = _CallSim(tracker)

    for _ in range(9):
        await sim.utterance("S0", separable_corpus[first][_SEGMENT_PASSAGE], 2.5)
        await sim.utterance("S1", separable_corpus[second][_SEGMENT_PASSAGE], 2.5)
    await tracker.finalize()

    if captured:
        stranger = cosine_similarity(captured["embedding"], profiles[second])
        assert (
            stranger < SPEAKER_MATCH_THRESHOLD
        ), f"stored voiceprint matches the other speaker at {stranger:.3f}"


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason=(
        "F2: the manual-enrollment nudge reads the same collapsed voice "
        "count as the enrollment gate (it needs >=2, enrollment needs ==1), "
        "so it never fires and the Console fallback recorder is unreachable."
    ),
)
async def test_multiple_voices_trigger_the_enrollment_suggestion(
    embedder,
    separable_corpus,
):
    first, second = sorted(separable_corpus)[:2]
    suggested: dict = {}
    tracker = _make_tracker(
        embedder,
        enrolled={},
        on_suggested=lambda n: suggested.update(count=n),
    )
    sim = _CallSim(tracker)

    for _ in range(3):
        await sim.utterance("S0", separable_corpus[first][_SEGMENT_PASSAGE], 2.5)
        await sim.utterance("S1", separable_corpus[second][_SEGMENT_PASSAGE], 2.5)
    await tracker.finalize()

    assert suggested.get("count", 0) >= 2
