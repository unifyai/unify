"""File attachment upload followed by a grounded question about file contents."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from tests.flows.harness import FlowHarness


@pytest.mark.asyncio
async def test_file_upload_question_uses_file_content(
    flow_session: FlowHarness,
    tmp_path: Path,
) -> None:
    """Attachment + question -> CM act -> primitives.files -> grounded reply."""

    secret = "amber-quartz-7742"  # pragma: allowlist secret
    attachment_path = tmp_path / "inventory.txt"
    attachment_path.write_text(
        textwrap.dedent(
            f"""\
            Warehouse snapshot
            SKU: WX-12
            Secret code: {secret}
            """,
        ),
        encoding="utf-8",
    )

    await flow_session.inject_unify_message(
        "I attached a file. What is the secret code in it? Reply with just the code.",
        attachments=[attachment_path],
    )
    reply = await flow_session.wait_for_unify_reply(timeout=240.0)
    content = str(reply.content or "")
    assert secret in content
