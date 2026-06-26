"""File attachment upload followed by a grounded question about file contents."""

from __future__ import annotations

import uuid
from pathlib import Path

import fitz
import pytest

from tests.flows.harness import FlowHarness


def _write_secret_pdf(path: Path, secret: str) -> None:
    """Write a one-page PDF whose only meaningful content is ``secret``.

    A PDF (rather than a plain ``.txt``) is deliberate: the secret is only
    recoverable by actually parsing the uploaded document, so a ``cat``-style
    shortcut over the raw bytes yields nothing usable. Recovering the code at
    all requires the upload -> ingest -> parse path the test exercises.
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
    """Attachment + grounded question: the reply must quote the uploaded file.

    The secret is a fresh per-run token embedded only inside the PDF, so the
    brain can recover it only by reading the actual ingested attachment through
    the file-primitive surface (``render_pdf`` / ``ask_about_file``); it cannot
    be guessed, recalled from memory, or lifted from the prompt. That grounding
    is the load-bearing guarantee here.

    A method-level primitive assertion is intentionally omitted: the brain
    legitimately varies which file primitive it reaches for, and the rendering
    primitives it prefers for PDFs are not on the ``ManagerMethod`` telemetry
    stream, so pinning a single method name would be brittle without proving
    anything the random-token grounding does not already prove.
    """

    secret = f"amber-quartz-{uuid.uuid4().hex[:8]}"  # pragma: allowlist secret
    attachment_path = tmp_path / "inventory.pdf"
    _write_secret_pdf(attachment_path, secret)

    await flow_session.inject_unify_message(
        "I attached a file. What is the secret code in it? "
        "Reply with just the code.",
        attachments=[attachment_path],
    )
    reply = await flow_session.wait_for_unify_reply(timeout=240.0)
    content = str(reply.content or "")
    assert secret in content
