"""Verify CodeActActor routes image Q&A through query_llm(images=...) in execute_code."""

from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from tests.actor.state_managers.utils import (
    extract_code_act_execute_code_snippets,
    get_code_act_tool_calls,
    make_code_act_actor,
)

pytestmark = [pytest.mark.eval, pytest.mark.llm_call]

_VISIBLE_TEXT = "V1S10N42"


def _write_labeled_image(path: Path, text: str) -> None:
    img = Image.new("RGB", (240, 100), color=(20, 40, 120))
    draw = ImageDraw.Draw(img)
    draw.text((30, 35), text, fill=(255, 255, 255))
    img.save(path, format="JPEG")


@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_code_act_uses_query_llm_images_for_screenshot_text(
    tmp_path: Path,
):
    img_path = tmp_path / "screenshot.jpg"
    _write_labeled_image(img_path, _VISIBLE_TEXT)

    request = (
        f"What text is visible in the image at {img_path}? "
        "Write Python in execute_code to answer programmatically. "
        "Use the sandbox LLM helpers for image analysis rather than "
        "primitives.files.ask_about_file. "
        "Do not ask clarifying questions. Return only the visible text."
    )

    async with make_code_act_actor(impl="simulated") as (actor, _primitives, calls):
        handle = await actor.act(
            request,
            clarification_enabled=False,
        )
        result = await handle.result()

    assert result is not None
    assert "execute_code" in set(get_code_act_tool_calls(handle))

    snippets = extract_code_act_execute_code_snippets(handle)
    assert any(
        "query_llm(" in snippet and "images" in snippet for snippet in snippets
    ), snippets
    assert not any("ask_about_file" in call for call in calls), calls

    assert _VISIBLE_TEXT in str(result).upper()
