"""File attachment upload followed by a grounded question about file contents."""

from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from tests.flows.harness import FlowHarness, assert_primitive_invoked
from tests.helpers import capture_events


def _write_secret_pdf(path: Path, secret: str) -> None:
    """Write a one-page PDF whose only meaningful content is ``secret``.

    A PDF (rather than a plain ``.txt``) is deliberate: the brain cannot answer
    by reading the bytes off disk, so it has to route through the document
    understanding surface (``primitives.files.ask_about_file``) to recover the
    code. That is the behavior the assertion below pins.
    """

    doc = fitz.open()
    try:
        page = doc.new_page()
        page.insert_textbox(
            fitz.Rect(72, 72, 500, 300),
            f"Warehouse snapshot\nSKU: WX-12\nSecret code: {secret}\n",
            fontsize=12,
        )
        doc.save(str(path))
    finally:
        doc.close()


@pytest.mark.asyncio
async def test_file_upload_question_uses_file_content(
    flow_session: FlowHarness,
    tmp_path: Path,
) -> None:
    """Attachment + question -> CM act -> primitives.files.ask_about_file -> reply.

    Asserts both that the grounded secret comes back AND that the brain answered
    by querying the file through ``primitives.files.ask_about_file`` rather than
    parsing the document itself in ``execute_code``, which would reach the same
    answer while bypassing the document-understanding primitive.
    """

    secret = "amber-quartz-7742"  # pragma: allowlist secret
    attachment_path = tmp_path / "inventory.pdf"
    _write_secret_pdf(attachment_path, secret)

    async with capture_events("ManagerMethod") as events:
        await flow_session.inject_unify_message(
            "I attached a file. What is the secret code in it? "
            "Reply with just the code.",
            attachments=[attachment_path],
        )
        reply = await flow_session.wait_for_unify_reply(timeout=240.0)
    content = str(reply.content or "")
    assert secret in content
    assert_primitive_invoked(events, "FileManager", "ask_about_file")
