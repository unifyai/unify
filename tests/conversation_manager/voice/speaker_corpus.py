"""Multi-speaker audio corpus for real-model speaker-identification tests.

The stub embedder in ``test_speaker_id.py`` maps audio to orthogonal 2-d unit
vectors, so every similarity threshold trivially passes and segment duration
cannot matter. That harness proves the tracker's *wiring*; it cannot prove its
*tuning*. This module supplies the audio needed to measure the tuning against
the real CAM++ extractor.

Two corpus sources, in priority order:

1. ``$UNIFY_SPEAKER_TEST_CORPUS`` — a directory of ``{speaker}_{passage}.wav``
   files (16-bit PCM WAV). **Real human recordings belong here.** Threshold
   calibration requires them; see the caveat below.
2. A synthetic corpus generated with macOS ``say`` and cached under
   ``~/.cache/unify/speaker_id/test_corpus/``. Generated once, then reused.

Caveat on the synthetic corpus: ``say`` voices come from one synthesiser and
share its artifacts, so different-speaker similarity is *inflated* relative to
real humans. That makes it sound for tests asserting a threshold is set too
low (a real corpus would only widen the gap) but unsound for calibrating where
a threshold should sit. Tests needing the latter gate on a real corpus.

``say`` also silently substitutes the default voice for any voice that is not
installed, which yields byte-identical files for several requested names, so
the builder de-duplicates on content hash.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np

from unify.conversation_manager.speaker_id import wav_bytes_to_pcm

CORPUS_ENV = "UNIFY_SPEAKER_TEST_CORPUS"
SAMPLE_RATE = 16000

# Two passages so the same speaker can be scored against *different words*,
# which is the real-world same-speaker case (never the identical utterance).
PASSAGES = {
    "a": (
        "I have seen things you people would not believe. Attack ships on fire "
        "off the shoulder of Orion. I watched C beams glitter in the dark near "
        "the Tannhauser Gate. All those moments will be lost in time, like "
        "tears in rain."
    ),
    "b": (
        "The quick brown fox jumps over the lazy dog while the sun sets over "
        "the quiet harbour and the boats return home for the evening tide. "
        "Nothing else stirred along the whole length of the empty road."
    ),
}

# Requested generously; whatever this machine actually installs survives the
# de-duplication pass below.
_SAY_VOICES = (
    "Samantha",
    "Daniel",
    "Karen",
    "Moira",
    "Rishi",
    "Alex",
    "Fiona",
    "Tessa",
    "Veena",
    "Nicky",
)

_MIN_SPEAKERS = 4


def _cache_dir() -> Path:
    # ``UNIFY_REAL_HOME`` is the pre-isolation home that tests/conftest.py
    # records before pointing HOME at a temp dir. Preferring it keeps the
    # generated corpus in one place instead of re-rendering it into a fresh
    # temp home on every pytest session.
    real_home = os.environ.get("UNIFY_REAL_HOME")
    if real_home:
        root = Path(real_home) / ".cache"
    else:
        root = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return root / "unify" / "speaker_id" / "test_corpus"


def _generate_with_say(dest: Path) -> None:
    """Render every passage in every available ``say`` voice into ``dest``."""
    dest.mkdir(parents=True, exist_ok=True)
    for voice in _SAY_VOICES:
        for passage_key, text in PASSAGES.items():
            out = dest / f"{voice}_{passage_key}.wav"
            if out.exists():
                continue
            try:
                subprocess.run(
                    [
                        "say",
                        "-v",
                        voice,
                        "--data-format=LEI16@16000",
                        "-o",
                        str(out),
                        text,
                    ],
                    check=True,
                    capture_output=True,
                    timeout=60,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                # Voice unavailable on this machine; the others still suffice.
                out.unlink(missing_ok=True)


def _read_pcm(path: Path) -> np.ndarray:
    pcm, rate = wav_bytes_to_pcm(path.read_bytes())
    if rate != SAMPLE_RATE:
        raise ValueError(f"{path.name}: expected {SAMPLE_RATE} Hz, got {rate}")
    return pcm


def _source_dir() -> Path | None:
    """The directory holding the corpus, generating it first if it can."""
    override = os.environ.get(CORPUS_ENV, "")
    if override:
        return Path(override)
    cache = _cache_dir()
    if sys.platform == "darwin" and shutil.which("say"):
        _generate_with_say(cache)
    return cache if cache.exists() else None


def _speaker_files() -> dict[str, dict[str, Path]]:
    """``{speaker: {passage: path}}`` for complete, distinct voices.

    De-duplication hashes the file bytes rather than decoded audio so
    availability can be checked without decoding the whole corpus. ``say``
    substitutes the default voice for any name that is not installed, which
    yields byte-identical renders under several different speaker names.
    """
    source = _source_dir()
    if source is None:
        return {}
    by_speaker: dict[str, dict[str, Path]] = {}
    for wav in sorted(source.glob("*.wav")):
        if "_" not in wav.stem:
            continue
        speaker, passage = wav.stem.rsplit("_", 1)
        by_speaker.setdefault(speaker, {})[passage] = wav

    complete = {
        name: passages
        for name, passages in by_speaker.items()
        if set(passages) >= set(PASSAGES)
    }
    seen: set[str] = set()
    unique: dict[str, dict[str, Path]] = {}
    for name in sorted(complete):
        digest = hashlib.sha256(complete[name]["a"].read_bytes()).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        unique[name] = complete[name]
    return unique


def load_corpus() -> dict[str, dict[str, np.ndarray]]:
    """Return ``{speaker: {passage: int16 mono PCM @ 16 kHz}}``."""
    return {
        name: {passage: _read_pcm(path) for passage, path in passages.items()}
        for name, passages in _speaker_files().items()
    }


def is_real_corpus() -> bool:
    """Whether the corpus is human recordings rather than TTS renders.

    Only a real corpus can say where a threshold *should* sit; the synthetic
    one can only say when a threshold is set too low. See the module docstring.
    """
    return bool(os.environ.get(CORPUS_ENV, ""))


def unavailable_reason() -> str | None:
    """``None`` when the corpus is usable, else a human-readable skip reason.

    Deliberately avoids decoding the audio: this runs at import time to drive
    the module-level ``skipif``.
    """
    corpus = _speaker_files()
    if not corpus:
        return (
            f"no speaker corpus: set ${CORPUS_ENV} to a directory of "
            "{speaker}_{passage}.wav files, or run on macOS so it can be "
            "generated with `say`"
        )
    if len(corpus) < _MIN_SPEAKERS:
        return (
            f"speaker corpus has {len(corpus)} distinct voice(s), "
            f"need >= {_MIN_SPEAKERS}"
        )
    return None
