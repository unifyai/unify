"""Local workspace directory bootstrap for ConversationManager sessions."""

from __future__ import annotations

import shutil
from pathlib import Path


def ensure_local_workspace_dirs(root: Path) -> None:
    """Ensure the standard local workspace layout exists for a new session.

    ``Attachments/`` is preserved across sessions. ``Outputs/`` and
    ``Screenshots/`` are wiped when present, then recreated empty so
    outbound staging and screenshot capture always have a writable home.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "Attachments").mkdir(exist_ok=True)

    outputs = root / "Outputs"
    if outputs.exists():
        shutil.rmtree(outputs)
    outputs.mkdir(exist_ok=True)

    screenshots = root / "Screenshots"
    if screenshots.exists():
        shutil.rmtree(screenshots)
    (screenshots / "User").mkdir(parents=True, exist_ok=True)
    (screenshots / "Assistant").mkdir(parents=True, exist_ok=True)
    (screenshots / "Webcam").mkdir(parents=True, exist_ok=True)
