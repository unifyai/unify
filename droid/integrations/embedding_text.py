"""Deterministic Layer 1 embedding-text normalization for integration catalog rows.

This is integration-scoped only: it normalizes the ``embedding_text`` produced by
the integration app/tool row builders and must NOT be imported by the shared
semantic-search infrastructure or any other state manager's embedding text.

The transform is deterministic, label-free, and uses no LLM. It cleans and
recombines content that already exists in the canonical catalog fields so the
pooled embedding vector is dominated by signal rather than scaffolding noise
(dotted identifiers, argspecs, raw JSON, scope URLs, constant ``Field:`` labels).

This module is duplicated byte-for-byte in the orchestra integration layer
(``orchestra/orchestra/services/integration_embedding_text.py``) so that the
manifest (Orchestra) and self-host (droid) paths emit identical vectors for the
same input row.
"""

from __future__ import annotations

import re

_URL_RE = re.compile(r"https?://\S+")
_STRUCTURAL_RE = re.compile(r"[{}\[\]\"`|]")
_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_UNDERSCORE_SLASH_RE = re.compile(r"(?<=\w)[_/](?=\w)")
_INNER_DOT_RE = re.compile(r"(?<=[A-Za-z0-9])\.(?=[A-Za-z0-9])")
_WHITESPACE_RE = re.compile(r"\s+")

_AUTH_MODE_LABELS = {
    "api_key": "API key",  # pragma: allowlist secret
    "oauth": "OAuth",
    "custom": "Custom",
}


def _split_identifier_token(token: str) -> str:
    """Split a single token if it is an identifier; otherwise return it unchanged.

    Splits on internal ``_`` / ``/`` separators, inner ``.`` between alphanumerics,
    and camelCase boundaries. Trailing sentence punctuation (e.g. ``action.``) and
    prose hyphens (e.g. ``commission-free``) are preserved. Identifier tokens are
    lowercased; ordinary words and acronyms (``API``, ``OAuth``) are left intact.
    """

    core = _UNDERSCORE_SLASH_RE.sub(" ", token)
    core = _INNER_DOT_RE.sub(" ", core)
    core = _CAMEL_BOUNDARY_RE.sub(" ", core)
    if core != token:
        return core.lower()
    return token


def _normalize_line(text: str) -> str:
    text = _URL_RE.sub(" ", text)
    text = _STRUCTURAL_RE.sub(" ", text)
    tokens = (_split_identifier_token(token) for token in text.split(" ") if token)
    return _WHITESPACE_RE.sub(" ", " ".join(tokens)).strip()


def normalize_embedding_text(parts: list[str]) -> str:
    """Normalize and recombine field values into clean embedding text.

    Each part is URL/structural-noise stripped, identifier-split, and
    whitespace-collapsed. Empty parts are dropped and exact duplicate lines are
    removed (preserving first-occurrence order) so the pooled vector is not
    diluted by repeated scaffolding.
    """

    lines: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        for raw_line in str(part).split("\n"):
            line = _normalize_line(raw_line)
            if not line or line in seen:
                continue
            seen.add(line)
            lines.append(line)
    return "\n".join(lines)


def humanize_auth_modes(auth_modes: list[str] | None) -> str:
    """Render canonical auth-mode ids as a human, embedding-friendly string."""

    labels: list[str] = []
    for mode in auth_modes or []:
        key = str(mode).strip().lower()
        label = _AUTH_MODE_LABELS.get(key, key.replace("_", " ").title())
        if label and label not in labels:
            labels.append(label)
    return ", ".join(labels)
