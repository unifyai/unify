"""On-demand SFTP access to a user's own home directory.

Distinct from :mod:`unity.file_manager.sync.manager` (continuous bisync with the
assistant's managed VM). This module pulls/pushes individual paths from a
*user's* machine on request over the raw-TCP tunnel the device registered:

- Reads stage into ``~/Unity/Remote/<user-id>/`` (mirroring the remote tree).
- Writebacks never overwrite the user's originals: edited content lands as a
  timestamped copy under the remote ``/.unity-edits/`` mirror tree, which is in
  turn excluded from reads.

The per-link private key is fetched on demand from the admin assistant read
(``user_desktop_filesync_keys`` keyed by ``owner_user_id``) so it never rides
the assistant pod env. The device serves ``$HOME`` as the SFTP root and accepts
the fixed username ``unity``.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Optional

from unity.common.hierarchical_logger import ICONS
from unity.logger import LOGGER
from unity.session_details import UserDesktopLink

# Fixed handshake contract with the device-side ``rclone serve sftp``.
SFTP_USER = "unity"
# The directory (relative to the served home root) that receives versioned
# writebacks. Excluded from reads so the assistant never pulls its own edits.
EDITS_DIR = ".unity-edits"


def _utc_stamp() -> str:
    """Filesystem-safe UTC timestamp for versioned writeback names."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _normalize_remote(path: str) -> PurePosixPath:
    """Normalize a user-supplied home-relative path to a clean POSIX path."""
    cleaned = (path or "").strip().lstrip("/")
    pure = PurePosixPath(cleaned)
    if pure.is_absolute() or any(part == ".." for part in pure.parts):
        raise ValueError(f"Unsafe remote path: {path!r}")
    return pure


class UserHomeSFTP:
    """On-demand rclone-SFTP client for one user's linked home directory."""

    REMOTE_NAME = "user_home"

    def __init__(self, user_id: str, link: UserDesktopLink):
        if not link.filesys_available:
            raise ValueError(
                f"User {user_id!r} has no usable home filesystem link "
                "(filesys_sync disabled or SFTP tunnel not registered).",
            )
        self._user_id = user_id
        self._link = link
        self._config_path: Optional[str] = None
        self._key_path: Optional[str] = None
        self._op_lock = asyncio.Lock()
        self._setup_done = False

    @property
    def local_root(self) -> Path:
        """Local stage directory mirroring this user's remote home."""
        return Path.home() / "Unity" / "Remote" / self._user_id

    # ── setup ────────────────────────────────────────────────────────────
    async def setup(self) -> bool:
        """Fetch the per-link key, write the rclone config, verify the link."""
        async with self._op_lock:
            if self._setup_done:
                return True
            key = await self._get_private_key()
            if not key:
                LOGGER.error(
                    f"{ICONS['file_sync']} [UserSFTP] No filesync key for "
                    f"user {self._user_id}",
                )
                return False

            self._key_path = tempfile.mktemp(prefix="user_sftp_key_")
            Path(self._key_path).write_text(key)
            os.chmod(self._key_path, 0o600)

            self.local_root.mkdir(parents=True, exist_ok=True)

            self._config_path = tempfile.mktemp(suffix=".conf", prefix="user_sftp_")
            Path(self._config_path).write_text(
                f"[{self.REMOTE_NAME}]\n"
                "type = sftp\n"
                f"host = {self._link.sftp_tunnel_host}\n"
                f"port = {self._link.sftp_tunnel_port}\n"
                f"user = {SFTP_USER}\n"
                f"key_file = {self._key_path}\n"
                "set_modtime = false\n",
            )

            ok = await self._run(
                ["lsf", f"{self.REMOTE_NAME}:/", "--max-depth", "1"],
                operation="connection test",
            )
            self._setup_done = ok
            if ok:
                LOGGER.info(
                    f"{ICONS['file_sync']} [UserSFTP] Connected to "
                    f"{self._user_id}'s home",
                )
            return ok

    # ── operations ───────────────────────────────────────────────────────
    async def list_dir(self, remote_path: str = "") -> list[str]:
        """List entries under a home-relative directory (excludes edits dir)."""
        rel = _normalize_remote(remote_path)
        async with self._op_lock:
            out: list[str] = []
            ok = await self._run(
                [
                    "lsf",
                    f"{self.REMOTE_NAME}:/{rel}",
                    "--exclude",
                    f"/{EDITS_DIR}/**",
                ],
                operation=f"list {rel}",
                capture=out,
            )
            if not ok:
                raise RuntimeError(f"Failed to list {rel} on {self._user_id}'s home")
            return [line for line in "".join(out).splitlines() if line]

    async def pull(self, remote_path: str) -> str:
        """Copy a home file/dir into the local stage; return its local path."""
        rel = _normalize_remote(remote_path)
        dest = self.local_root / rel
        async with self._op_lock:
            dest.parent.mkdir(parents=True, exist_ok=True)
            ok = await self._run(
                [
                    "copyto",
                    f"{self.REMOTE_NAME}:/{rel}",
                    str(dest),
                    "--exclude",
                    f"/{EDITS_DIR}/**",
                    "-v",
                ],
                operation=f"pull {rel}",
            )
            if not ok:
                raise RuntimeError(f"Failed to pull {rel} from {self._user_id}'s home")
            return str(dest)

    async def push(self, local_path: str, dest_path: str) -> str:
        """Write a local file back as a timestamped copy under the edits dir.

        ``dest_path`` is the home-relative path the content corresponds to; the
        original is never touched. Returns the remote path of the versioned copy.
        """
        rel = _normalize_remote(dest_path)
        stamped = f"{rel.stem}.unity-{_utc_stamp()}{rel.suffix}"
        remote_rel = PurePosixPath(EDITS_DIR) / rel.parent / stamped
        src = Path(local_path).expanduser()
        if not src.is_file():
            raise ValueError(f"Local file not found: {local_path}")
        async with self._op_lock:
            ok = await self._run(
                ["copyto", str(src), f"{self.REMOTE_NAME}:/{remote_rel}", "-v"],
                operation=f"push {remote_rel}",
            )
            if not ok:
                raise RuntimeError(
                    f"Failed to push to {remote_rel} on {self._user_id}'s home",
                )
            return f"/{remote_rel}"

    def cleanup(self) -> None:
        """Remove the temp key and config files."""
        for path in (self._config_path, self._key_path):
            if path and Path(path).exists():
                try:
                    Path(path).unlink()
                except OSError as e:
                    LOGGER.error(
                        f"{ICONS['file_sync']} [UserSFTP] cleanup failed for "
                        f"{path}: {e}",
                    )
        self._setup_done = False

    # ── internals ────────────────────────────────────────────────────────
    async def _run(
        self,
        args: list[str],
        *,
        operation: str,
        capture: Optional[list[str]] = None,
    ) -> bool:
        """Run a single rclone command; append stdout to ``capture`` if given."""
        cmd = ["rclone", "--config", self._config_path, *args]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
        except FileNotFoundError:
            LOGGER.error(f"{ICONS['file_sync']} [UserSFTP] rclone not installed")
            return False
        if proc.returncode != 0:
            LOGGER.error(
                f"{ICONS['file_sync']} [UserSFTP] {operation} failed: "
                f"{(stderr.decode() if stderr else '')[:500]}",
            )
            return False
        if capture is not None and stdout:
            capture.append(stdout.decode())
        return True

    async def _get_private_key(self) -> Optional[str]:
        """Fetch this link's private key from the admin assistant read.

        Mirrors the managed-VM key fetch: keyed by ``owner_user_id`` in the
        admin/runtime-only ``user_desktop_filesync_keys`` map so the key never
        reaches the pod env via ``user_desktops``.
        """
        from unify.utils import http

        from unity.session_details import SESSION_DETAILS
        from unity.settings import SETTINGS

        assistant_id = SESSION_DETAILS.assistant.agent_id
        base_url = SETTINGS.ORCHESTRA_URL
        admin_key = SETTINGS.ORCHESTRA_ADMIN_KEY.get_secret_value()
        if assistant_id is None or not base_url or not admin_key:
            return None

        resp = http.get(
            f"{base_url}/admin/assistant",
            headers={"Authorization": f"Bearer {admin_key}"},
            params={"agent_id": str(assistant_id)},
            timeout=30,
        )
        if resp.status_code != 200:
            LOGGER.error(
                f"{ICONS['file_sync']} [UserSFTP] admin read failed: "
                f"{resp.status_code}",
            )
            return None
        assistants = resp.json().get("info", [])
        if not assistants:
            return None
        keys = assistants[0].get("user_desktop_filesync_keys") or {}
        return keys.get(self._user_id)
