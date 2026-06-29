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

# Exclude patterns are tiered by the operation's intent. rclone exclude
# semantics: a leading "/" anchors to the served home root; an unanchored
# pattern matches that path segment at any depth.
#
# Tier 1 (always): our own versioned writebacks — never read back.
# Tier 2 (noise): caches, dependency trees, VCS metadata, trashes — no document
#   value and bloat any copy. Applied to pull + sync, never to list.
# Tier 3 (secrets): credential/secret dirs — not copied off the user's machine.
#   Applied to pull + sync, never to list.
# list stays truthful (tier 1 only) so the real tree is visible; pull and sync
# skip tiers 2 + 3 so copies stay lean and never exfiltrate credentials.
_ALWAYS_EXCLUDES: tuple[str, ...] = (f"/{EDITS_DIR}/**",)

_NOISE_EXCLUDES: tuple[str, ...] = (
    "/.cache/**",
    "/.npm/**",
    "/.cargo/**",
    "/.rustup/**",
    "/.gradle/**",
    "/.m2/**",
    "/.local/share/Trash/**",
    "/Library/Caches/**",
    "/.Trash/**",
    "node_modules/**",
    ".git/**",
    "__pycache__/**",
    ".venv/**",
    "venv/**",
    ".tox/**",
    ".mypy_cache/**",
    ".pytest_cache/**",
    ".next/**",
    ".idea/**",
    ".DS_Store",
)

_SECRET_EXCLUDES: tuple[str, ...] = (
    "/.ssh/**",
    "/.gnupg/**",
    "/.aws/**",
    "/.kube/**",
    "/.config/gcloud/**",
    "/.docker/**",
    "/.netrc",
    "/.git-credentials",
)


def _build_excludes(*, noise: bool, secrets: bool) -> list[str]:
    """Flatten the requested exclude tiers into ``--exclude PAT`` rclone args.

    Tier 1 (the writeback dir) is always included; ``noise`` adds tier 2 and
    ``secrets`` adds tier 3.
    """
    patterns: list[str] = list(_ALWAYS_EXCLUDES)
    if noise:
        patterns += _NOISE_EXCLUDES
    if secrets:
        patterns += _SECRET_EXCLUDES
    args: list[str] = []
    for pattern in patterns:
        args += ["--exclude", pattern]
    return args


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

            with tempfile.NamedTemporaryFile(
                "w",
                prefix="user_sftp_key_",
                delete=False,
            ) as key_file:
                key_file.write(key)
                self._key_path = key_file.name
            os.chmod(self._key_path, 0o600)

            self.local_root.mkdir(parents=True, exist_ok=True)

            with tempfile.NamedTemporaryFile(
                "w",
                suffix=".conf",
                prefix="user_sftp_",
                delete=False,
            ) as config_file:
                config_file.write(
                    f"[{self.REMOTE_NAME}]\n"
                    "type = sftp\n"
                    f"host = {self._link.sftp_tunnel_host}\n"
                    f"port = {self._link.sftp_tunnel_port}\n"
                    f"user = {SFTP_USER}\n"
                    f"key_file = {self._key_path}\n"
                    "set_modtime = false\n",
                )
                self._config_path = config_file.name

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
        """List entries under a home-relative directory.

        Browsing stays truthful: only the writeback dir is hidden, so caches,
        dependency trees and credential dirs are all still visible here. The
        leaner exclude set (noise + secrets) applies when content is actually
        copied via :meth:`pull` / :meth:`sync`.
        """
        rel = _normalize_remote(remote_path)
        async with self._op_lock:
            out: list[str] = []
            ok = await self._run(
                [
                    "lsf",
                    f"{self.REMOTE_NAME}:/{rel}",
                    *_build_excludes(noise=False, secrets=False),
                ],
                operation=f"list {rel}",
                capture=out,
            )
            if not ok:
                raise RuntimeError(f"Failed to list {rel} on {self._user_id}'s home")
            return [line for line in "".join(out).splitlines() if line]

    async def pull(self, remote_path: str) -> str:
        """Copy a home file/dir into the local stage; return its local path.

        Noise (caches, deps, VCS metadata) and credential dirs (``.ssh``,
        ``.gnupg``, ``.aws``, …) are skipped, so pulling a directory won't drag
        in its dependency trees or secrets. Use :meth:`list_dir` to see the
        full tree first.
        """
        rel = _normalize_remote(remote_path)
        dest = self.local_root / rel
        async with self._op_lock:
            dest.parent.mkdir(parents=True, exist_ok=True)
            ok = await self._run(
                [
                    "copyto",
                    f"{self.REMOTE_NAME}:/{rel}",
                    str(dest),
                    *_build_excludes(noise=True, secrets=True),
                    "-v",
                ],
                operation=f"pull {rel}",
            )
            if not ok:
                raise RuntimeError(f"Failed to pull {rel} from {self._user_id}'s home")
            return str(dest)

    def _sync_args(self, rel: PurePosixPath, dest: Path) -> list[str]:
        """Build the rclone ``copy`` args for a bulk sync (testable, pure)."""
        return [
            "copy",
            f"{self.REMOTE_NAME}:/{rel}",
            str(dest),
            *_build_excludes(noise=True, secrets=True),
            "--stats",
            "15s",
            "--stats-one-line",
            "--stats-log-level",
            "NOTICE",
        ]

    async def sync(self, remote_path: str = "") -> list[str]:
        """Recursively mirror a home subtree into the local stage.

        Prefer :meth:`list_dir` + :meth:`pull` to fetch only what's needed; this
        bulk mirror is for when the whole subtree is genuinely wanted. Caches,
        dependency trees, VCS metadata and trashes are skipped, and credential
        dirs (``.ssh``, ``.gnupg``, ``.aws``, …) are never copied off the
        machine; progress is logged periodically as it runs.

        ``remote_path`` is home-relative (``""`` mirrors the whole home, which
        can be large and slow — scope to a subtree like ``"Documents"`` when
        possible). Returns the absolute local paths now staged.
        """
        rel = _normalize_remote(remote_path)
        dest = self.local_root / rel
        async with self._op_lock:
            dest.mkdir(parents=True, exist_ok=True)
            ok = await self._run(
                self._sync_args(rel, dest),
                operation=f"sync {rel}",
                stream=True,
            )
            if not ok:
                raise RuntimeError(
                    f"Failed to sync {rel} from {self._user_id}'s home",
                )
            return [str(p) for p in sorted(dest.rglob("*")) if p.is_file()]

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
        stream: bool = False,
    ) -> bool:
        """Run a single rclone command; append stdout to ``capture`` if given.

        When ``stream`` is set, stderr is drained line-by-line and logged live
        (so a long ``--stats`` mirror is observable as it runs) rather than
        buffered until completion.
        """
        cmd = ["rclone", "--config", self._config_path, *args]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            LOGGER.error(f"{ICONS['file_sync']} [UserSFTP] rclone not installed")
            return False

        if stream:
            stderr_tail: list[str] = []
            assert proc.stderr is not None
            async for raw in proc.stderr:
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                stderr_tail.append(line)
                LOGGER.info(
                    f"{ICONS['file_sync']} [UserSFTP] {operation}: {line}",
                )
            stdout = await proc.stdout.read() if proc.stdout else b""
            await proc.wait()
            stderr_summary = "\n".join(stderr_tail[-10:])
        else:
            stdout, stderr = await proc.communicate()
            stderr_summary = stderr.decode() if stderr else ""

        if proc.returncode != 0:
            LOGGER.error(
                f"{ICONS['file_sync']} [UserSFTP] {operation} failed: "
                f"{stderr_summary[:500]}",
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
