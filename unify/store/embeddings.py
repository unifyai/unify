"""Text embeddings for vector columns and semantic search.

Two providers:

- ``local`` (default): a small ONNX sentence-embedding model run in process via
  ``fastembed``. Deterministic, offline after the one-time model download, and
  needs no key, so the runtime's only credential stays the LLM key.
- ``openrouter``: ``openai/text-embedding-3-small`` through OpenRouter's
  embeddings endpoint, authenticated with ``OPENROUTER_API_KEY``.

``UNIFY_EMBED_MODEL`` selects the model: a bare name is a fastembed model id,
``<name>@openrouter`` routes to OpenRouter. Every vector is cached by
``(model, text)`` in a SQLite file (``UNIFY_EMBED_CACHE``, defaulting to
``embeddings.sqlite`` beside the store) so re-embedding the same text is a
lookup, across processes and runs.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import struct
import threading
from pathlib import Path
from typing import Callable, Iterable

DEFAULT_LOCAL_MODEL = "BAAI/bge-small-en-v1.5"
OPENROUTER_SUFFIX = "@openrouter"

_lock = threading.RLock()
_memory: dict[tuple[str, str], list[float]] = {}
_providers: dict[str, "Provider"] = {}
_cache_conn: sqlite3.Connection | None = None
_cache_path: str | None = None

Provider = Callable[[list[str]], list[list[float]]]


def configured_model() -> str:
    return os.environ.get("UNIFY_EMBED_MODEL", "").strip() or DEFAULT_LOCAL_MODEL


def _cache_file() -> str:
    explicit = os.environ.get("UNIFY_EMBED_CACHE", "").strip()
    if explicit:
        return explicit
    from .engine import store_home

    return str(store_home() / "embeddings.sqlite")


def _cache() -> sqlite3.Connection:
    global _cache_conn, _cache_path
    path = _cache_file()
    if _cache_conn is not None and _cache_path == path:
        return _cache_conn
    if _cache_conn is not None:
        _cache_conn.close()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS embeddings ("
        " model TEXT NOT NULL, text_hash TEXT NOT NULL, dim INTEGER NOT NULL,"
        " vector BLOB NOT NULL, PRIMARY KEY (model, text_hash))",
    )
    _cache_conn, _cache_path = conn, path
    return conn


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _pack(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)


def _unpack(blob: bytes, dim: int) -> list[float]:
    return list(struct.unpack(f"<{dim}f", blob))


def _openrouter_provider(model: str) -> Provider:
    import requests

    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            f"UNIFY_EMBED_MODEL={model!r} routes embeddings through OpenRouter, "
            "which needs OPENROUTER_API_KEY.",
        )
    name = model[: -len(OPENROUTER_SUFFIX)]

    def run(texts: list[str]) -> list[list[float]]:
        response = requests.post(
            "https://openrouter.ai/api/v1/embeddings",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": name, "input": texts},
            timeout=120,
        )
        response.raise_for_status()
        rows = sorted(response.json()["data"], key=lambda d: d["index"])
        return [[float(x) for x in row["embedding"]] for row in rows]

    return run


def _local_provider(model: str) -> Provider:
    from fastembed import TextEmbedding

    engine = TextEmbedding(model_name=model)

    def run(texts: list[str]) -> list[list[float]]:
        return [[float(x) for x in vec] for vec in engine.embed(texts, batch_size=64)]

    return run


def _provider(model: str) -> Provider:
    provider = _providers.get(model)
    if provider is None:
        if model.endswith(OPENROUTER_SUFFIX):
            provider = _openrouter_provider(model)
        else:
            provider = _local_provider(model)
        _providers[model] = provider
    return provider


def embed_many(texts: Iterable[str], model: str | None = None) -> list[list[float]]:
    """Embed several texts, serving cached vectors and batching the rest."""
    model = model or configured_model()
    texts = [str(t) for t in texts]
    results: list[list[float] | None] = [None] * len(texts)
    missing: dict[str, list[int]] = {}
    with _lock:
        conn = _cache()
        for index, text in enumerate(texts):
            key = (model, _hash(text))
            vector = _memory.get(key)
            if vector is None:
                row = conn.execute(
                    "SELECT dim, vector FROM embeddings WHERE model = ? AND text_hash = ?",
                    key,
                ).fetchone()
                if row is not None:
                    vector = _unpack(row[1], row[0])
                    _memory[key] = vector
            if vector is None:
                missing.setdefault(text, []).append(index)
            else:
                results[index] = vector
        if missing:
            ordered = list(missing)
            vectors = _provider(model)(ordered)
            for text, vector in zip(ordered, vectors):
                key = (model, _hash(text))
                _memory[key] = vector
                conn.execute(
                    "INSERT OR REPLACE INTO embeddings (model, text_hash, dim, vector)"
                    " VALUES (?, ?, ?, ?)",
                    (model, key[1], len(vector), _pack(vector)),
                )
                for index in missing[text]:
                    results[index] = vector
    return [vector for vector in results if vector is not None]


def embed(text: str, model: str | None = None) -> list[float]:
    """Embed one text."""
    return embed_many([text], model=model)[0]


def reset() -> None:
    """Drop in-memory state so a new cache path or model takes effect."""
    global _cache_conn, _cache_path
    with _lock:
        _memory.clear()
        _providers.clear()
        if _cache_conn is not None:
            _cache_conn.close()
        _cache_conn, _cache_path = None, None


def _json_safe(vector: list[float]) -> str:
    return json.dumps(vector)
