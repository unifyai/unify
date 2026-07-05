"""
Screenshot → CM brain → act(): routing integration test.

Validates that a buffered user screen-share screenshot causes the slow brain
to delegate filesystem work to act() with a Screenshots/ filepath reference.
Full CodeActActor crop/output behavior is covered elsewhere; this test focuses
on CM routing under screen-share + vision context.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.conftest import BOSS
from tests.conversation_manager.actions.integration.helpers import (
    assert_no_errors,
    get_actor_started_event,
)
from unify.conversation_manager.events import UnifyMessageReceived
from unify.conversation_manager.cm_types import ScreenshotEntry
from unify.file_manager.settings import get_local_root

pytestmark = [pytest.mark.integration, pytest.mark.eval, pytest.mark.llm_call]

GRUB_IMAGE_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent / "images" / "grub_screen.jpg"
)


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_screenshot_crop_via_act(initialized_cm_codeact):
    """Screen-shared screenshot work routes to act() with a Screenshots/ path."""
    cm = initialized_cm_codeact
    cm.cm.vm_ready = True
    cm.cm.file_sync_complete = True
    local_root = Path(get_local_root())

    (local_root / "Screenshots" / "User").mkdir(parents=True, exist_ok=True)
    (local_root / "Screenshots" / "Assistant").mkdir(parents=True, exist_ok=True)
    (local_root / "Outputs").mkdir(parents=True, exist_ok=True)

    assert GRUB_IMAGE_PATH.exists(), f"Test image not found: {GRUB_IMAGE_PATH}"
    grub_b64 = base64.b64encode(GRUB_IMAGE_PATH.read_bytes()).decode()

    cm.cm.user_screen_share_active = True
    entry = ScreenshotEntry(
        b64=grub_b64,
        utterance="I'm stuck on this screen, can you help?",
        timestamp=datetime.now(timezone.utc),
        source="user",
    )
    cm.cm._screenshot_buffer.append(entry)

    result = await cm.step_until_wait(
        UnifyMessageReceived(
            contact=BOSS,
            content=(
                "Can you crop the menu options area from my screen share "
                "screenshot and save it to the Outputs folder as menu_crop.png? "
                "Then send it back to me."
            ),
        ),
    )

    actor_event = get_actor_started_event(result)
    act_query = actor_event.query.lower()
    assert "screenshots" in act_query, (
        f"Expected the act() query to reference a Screenshots/ path. "
        f"Got: {actor_event.query}"
    )
    assert_no_errors(result)
