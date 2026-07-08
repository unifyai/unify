"""Build OpenAI-style image content blocks for vision LLM calls."""

from __future__ import annotations

import base64
import io
import re
from pathlib import Path
from typing import Any, Protocol

import unisdk


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


_DATA_URL_RE = re.compile(r"^data:image/[^;]+;base64,(?P<payload>.+)$", re.DOTALL)


def _image_bytes_from_payload(
    image: str | bytes,
    *,
    adapter: _BytesReader | None = None,
) -> bytes | None:
    """Return raster bytes for local payloads; ``None`` for remote URLs."""

    if isinstance(image, bytes):
        return image
    if image.startswith("data:image/"):
        match = _DATA_URL_RE.match(image)
        if match is None:
            raise ValueError("Invalid data:image URL")
        return base64.b64decode(match.group("payload"))
    if image.startswith(("http://", "https://", "gs://")):
        return None
    if adapter is not None:
        return adapter.open_bytes(image)
    return Path(image).read_bytes()


def scale_image_payload_for_observation(
    image: str | bytes,
    *,
    model: str | None = None,
    adapter: _BytesReader | None = None,
) -> str | bytes:
    """Fit raster image payloads to the model-aware observation space.

    Remote ``http(s)://`` and ``gs://`` URLs are returned unchanged. Local
    paths, raw bytes, and ``data:image/...`` payloads are decoded, resized
    with the same policy as ``display()``, and re-encoded as PNG bytes.
    """
    from PIL import Image

    from unify.common.observation_scaling import fit_image_to_observation_space

    raw = _image_bytes_from_payload(image, adapter=adapter)
    if raw is None:
        return image

    with Image.open(io.BytesIO(raw)) as pil_img:
        fitted = fit_image_to_observation_space(pil_img, model=model)
        buf = io.BytesIO()
        fitted.save(buf, format="PNG")
        return buf.getvalue()


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
        url = unisdk.get_signed_url(image, expiration_minutes=60)
    else:
        if adapter is not None:
            image_bytes = adapter.open_bytes(image)
        else:
            image_bytes = Path(image).read_bytes()
        url = bytes_to_data_url(image_bytes)

    return {"type": "image_url", "image_url": {"url": url}}
