"""Plain-text helpers for assistant-authored outbound messages."""

from __future__ import annotations

import re

_INLINE_WS = re.compile(r"[ \t]+")


def normalize_outbound_plain_text(text: str) -> str:
    """Collapse hard line wraps while preserving paragraph breaks.

    LLMs often hard-wrap prose near 80 columns. Mail clients and chat surfaces
    reflow continuous text naturally, but they honor every ``\\n`` in plain text
    as a fixed break — which produces ragged edges on wide screens. This helper
    joins lines within a paragraph into one flowing line and keeps blank lines
    (two or more consecutive newlines) as ``\\n\\n`` paragraph separators.
    """
    if not text:
        return text
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return ""
    paragraphs: list[str] = []
    for block in re.split(r"\n{2,}", normalized):
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        paragraphs.append(_INLINE_WS.sub(" ", " ".join(lines)))
    return "\n\n".join(paragraphs)
