from __future__ import annotations

"""
Token counting utilities shared by prompt budgeting and context compression.
"""

from typing import Optional

from unify.logger import LOGGER
from unify.common.hierarchical_logger import DEFAULT_ICON

_TIKTOKEN_AVAILABLE = True
try:
    import tiktoken  # type: ignore
except Exception:  # pragma: no cover
    _TIKTOKEN_AVAILABLE = False
    tiktoken = None  # type: ignore

_AVG_CHARS_PER_TOKEN = 4.0
_WARNED_ON_FALLBACK = False


def _warn_once() -> None:
    global _WARNED_ON_FALLBACK
    if not _WARNED_ON_FALLBACK:
        try:
            LOGGER.warning(
                f"{DEFAULT_ICON} tiktoken not available – using a conservative char→token heuristic. "
                "Install `tiktoken` for precise accounting.",
            )
        except Exception:
            pass
        _WARNED_ON_FALLBACK = True


def get_encoding_for(model_or_encoding: Optional[str] = None):
    """
    Return a tiktoken Encoding for a **model name** or **encoding name**.
    Preference: encoding_for_model(model) → get_encoding(name) → default.
    """
    if not _TIKTOKEN_AVAILABLE:
        _warn_once()
        return None

    if model_or_encoding:
        try:
            return tiktoken.encoding_for_model(model_or_encoding)
        except Exception:
            try:
                return tiktoken.get_encoding(model_or_encoding)
            except Exception:
                pass

        if "gpt-4o" in model_or_encoding or "o4-mini" in model_or_encoding:
            return tiktoken.get_encoding("o200k_base")
        return tiktoken.get_encoding("cl100k_base")

    return tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str, model_or_encoding: Optional[str] = None) -> int:
    """Return number of tokens in *text* for the given model/encoding."""
    if not _TIKTOKEN_AVAILABLE:
        _warn_once()
        return int(len(text) / _AVG_CHARS_PER_TOKEN * 1.1)
    enc = get_encoding_for(model_or_encoding)
    try:
        return len(enc.encode(text))  # type: ignore[attr-defined]
    except Exception:
        return int(len(text) / _AVG_CHARS_PER_TOKEN * 1.1)
