"""Tests for workspace directory bootstrap in conversation_manager."""

import base64
import os
from datetime import datetime, timezone

from unify.conversation_manager.cm_types import ScreenshotEntry
from unify.conversation_manager.cm_types.screenshot import (
    generate_screenshot_path,
    write_screenshot_to_disk,
)
from unify.conversation_manager.workspace import ensure_local_workspace_dirs
from unify.file_manager.settings import get_local_root


class TestWorkspaceBootstrap:
    """Verify standard local workspace dirs are created and session-cleared."""

    def test_creates_workspace_directories(self, tmp_path):
        root = tmp_path / "Unity" / "Local"
        ensure_local_workspace_dirs(root)

        assert root.is_dir()
        assert (root / "Attachments").is_dir()
        assert (root / "Outputs").is_dir()
        assert (root / "Screenshots" / "User").is_dir()
        assert (root / "Screenshots" / "Assistant").is_dir()
        assert (root / "Screenshots" / "Webcam").is_dir()

    def test_outputs_cleared_between_sessions(self, tmp_path):
        root = tmp_path / "Unity" / "Local"
        ensure_local_workspace_dirs(root)

        # Simulate a file generated during a previous session.
        stale = root / "Outputs" / "old_report.pdf"
        stale.write_bytes(b"stale")
        assert stale.exists()

        # Second bootstrap (new session) should clear Outputs/.
        ensure_local_workspace_dirs(root)

        assert not stale.exists()
        assert (root / "Outputs").is_dir()

    def test_screenshots_cleared_between_sessions(self, tmp_path):
        root = tmp_path / "Unity" / "Local"
        ensure_local_workspace_dirs(root)

        # Simulate screenshots from a previous session.
        stale_user = root / "Screenshots" / "User" / "old.png"
        stale_asst = root / "Screenshots" / "Assistant" / "old.png"
        stale_webcam = root / "Screenshots" / "Webcam" / "old.png"
        stale_user.write_bytes(b"stale")
        stale_asst.write_bytes(b"stale")
        stale_webcam.write_bytes(b"stale")
        assert stale_user.exists()
        assert stale_asst.exists()
        assert stale_webcam.exists()

        # Second bootstrap (new session) should clear all screenshot trees.
        ensure_local_workspace_dirs(root)

        assert not stale_user.exists()
        assert not stale_asst.exists()
        assert not stale_webcam.exists()
        assert (root / "Screenshots" / "User").is_dir()
        assert (root / "Screenshots" / "Assistant").is_dir()
        assert (root / "Screenshots" / "Webcam").is_dir()

    def test_attachments_preserved_between_sessions(self, tmp_path):
        root = tmp_path / "Unity" / "Local"
        ensure_local_workspace_dirs(root)

        # Simulate an attachment from a previous session.
        attachment = root / "Attachments" / "att-1_invoice.pdf"
        attachment.write_bytes(b"attachment")

        # Second bootstrap should NOT clear Attachments/.
        ensure_local_workspace_dirs(root)

        assert attachment.exists()

    def test_get_local_root_returns_unity_local(self):
        """get_local_root() should resolve to ~/Unity/Local by default."""
        root = get_local_root()
        assert root.endswith(os.path.join("Unity", "Local"))


class TestSaveScreenshot:
    """Verify generate_screenshot_path + write_screenshot_to_disk produce
    correct paths and file content.
    """

    @staticmethod
    def _make_entry(
        source: str = "assistant",
        ts: datetime | None = None,
        pixel: bytes = b"\x89PNG\r\n\x1a\n",
    ) -> ScreenshotEntry:
        b64 = base64.b64encode(pixel).decode()
        return ScreenshotEntry(
            b64=b64,
            utterance="test",
            timestamp=ts
            or datetime(2026, 2, 16, 14, 30, 45, 123456, tzinfo=timezone.utc),
            source=source,
        )

    def test_saves_to_correct_subfolder(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "Screenshots" / "Assistant").mkdir(parents=True)
        (tmp_path / "Screenshots" / "User").mkdir(parents=True)

        asst_entry = self._make_entry(source="assistant")
        user_entry = self._make_entry(source="user")
        asst_path = generate_screenshot_path(asst_entry)
        user_path = generate_screenshot_path(user_entry)
        write_screenshot_to_disk(asst_entry, asst_path)
        write_screenshot_to_disk(user_entry, user_path)

        assert asst_path.startswith("Screenshots/Assistant/")
        assert user_path.startswith("Screenshots/User/")

    def test_saves_webcam_to_correct_subfolder(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "Screenshots" / "Webcam").mkdir(parents=True)

        entry = self._make_entry(source="webcam")
        webcam_path = generate_screenshot_path(entry)
        write_screenshot_to_disk(entry, webcam_path)

        assert webcam_path.startswith("Screenshots/Webcam/")

    def test_timestamp_filename(self):
        ts = datetime(2026, 2, 16, 14, 30, 45, 123456, tzinfo=timezone.utc)
        path = generate_screenshot_path(self._make_entry(source="assistant", ts=ts))

        assert path == "Screenshots/Assistant/2026-02-16T14-30-45.123456.jpg"

    def test_writes_decoded_content(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "Screenshots" / "Assistant").mkdir(parents=True)

        raw = b"\x89PNG\r\n\x1a\nfake_image_data"
        entry = self._make_entry(pixel=raw)
        path = generate_screenshot_path(entry)
        write_screenshot_to_disk(entry, path)

        written = (tmp_path / path).read_bytes()
        assert written == raw
