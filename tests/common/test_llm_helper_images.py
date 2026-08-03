"""Image promotion out of tool payloads is gated on real base64 image data.

Payloads use "image" keys for plenty of non-image strings (asset URNs, URLs,
schema fragments). Promoting those into image_url content blocks ships
garbage that providers reject with a 400, killing the whole tool loop, so
only strings whose decoded head carries a PNG/JPEG magic may be collected —
everything else stays in the textual payload.
"""

from __future__ import annotations

import base64

from unify.common.llm_helpers import (
    _collect_images,
    _is_b64_image,
    _strip_image_keys,
)

PNG_B64 = base64.b64encode(
    b"\x89PNG\r\n\x1a\n" + b"\x00" * 64,
).decode()
JPEG_B64 = base64.b64encode(b"\xff\xd8\xff\xe0" + b"\x00" * 64).decode()


def test_is_b64_image_accepts_png_and_jpeg():
    assert _is_b64_image(PNG_B64)
    assert _is_b64_image(JPEG_B64)


def test_is_b64_image_rejects_non_image_strings():
    assert not _is_b64_image("urn:li:image:C4D00AAAAbBCDEFGhiJ")
    assert not _is_b64_image("https://example.com/picture.png")
    assert not _is_b64_image(f"data:image/png;base64,{PNG_B64}")
    assert not _is_b64_image(base64.b64encode(b"just some text bytes").decode())
    assert not _is_b64_image("")


def test_collect_images_skips_non_image_values():
    acc: list[str] = []
    _collect_images(
        {
            "examples": [{"image": "urn:li:image:C4D00AAAAbBCDEFGhiJ"}],
            "nested": {"image": PNG_B64},
            "schema": {"image": {"type": "string"}},
        },
        acc,
    )
    assert acc == [PNG_B64]


def test_strip_image_keys_only_removes_real_images():
    stripped = _strip_image_keys(
        {
            "examples": [{"image": "urn:li:image:C4D00AAAAbBCDEFGhiJ"}],
            "shot": {"image": PNG_B64},
        },
    )
    assert stripped["examples"][0]["image"] == "urn:li:image:C4D00AAAAbBCDEFGhiJ"
    assert "image" not in stripped["shot"]
