"""Build OpenAI-style image content blocks for vision LLM calls."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, Protocol

import unify


class _BytesReader(Protocol):
    def open_bytes(self, path: str) -> bytes: ...


def sniff_image_mime(image_bytes: bytes) -> str:
    """Return a MIME type for JPEG/PNG payloads, else octet-stream."""

    head = image_bytes[:10]
    if head.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    return "application/octet-stream"


def bytes_to_data_url(image_bytes: bytes) -> str:
    """Encode raw image bytes as a ``data:image/...;base64,...`` URL."""

    mime = sniff_image_mime(image_bytes)
    b64_data = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime};base64,{b64_data}"


def to_image_content_block(
    image: str | bytes,
    *,
    adapter: _BytesReader | None = None,
) -> dict[str, Any]:
    """Return an OpenAI-style image content block for a vision LLM message.

    Accepts local file paths, raw bytes, ``http(s)://`` URLs, ``gs://`` URIs,
    and ``data:image/...`` data URLs. Local paths are read via ``adapter`` when
    provided, otherwise via ``pathlib.Path.read_bytes()``.
    """

    if isinstance(image, bytes):
        url = bytes_to_data_url(image)
    elif image.startswith("data:image/"):
        url = image
    elif image.startswith("http://") or image.startswith("https://"):
        url = image
    elif image.startswith("gs://"):
        url = unify.get_signed_url(image, expiration_minutes=60)
    else:
        if adapter is not None:
            image_bytes = adapter.open_bytes(image)
        else:
            image_bytes = Path(image).read_bytes()
        url = bytes_to_data_url(image_bytes)

    return {"type": "image_url", "image_url": {"url": url}}
