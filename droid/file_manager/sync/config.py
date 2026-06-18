"""Configuration for managed VM file sync via rclone SFTP."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List
from urllib.parse import urlparse

from droid.logger import LOGGER
from droid.common.hierarchical_logger import ICONS


def _get_local_root() -> str:
    from droid.file_manager.settings import get_local_root

    return get_local_root()


@dataclass
class SyncConfig:
    """Configuration for managed VM file sync via rclone SFTP.

    Paths:
    - local_root: get_local_root() (defaults to ~/Droid/Local)
    - remote_root: /Droid/Local (VM, set up by ubuntu-vm-startup.sh)

    Syncs the dedicated Droid workspace which contains user files
    (Attachments/, functions/, etc.).

    Conflict resolution: Latest wins (by modification time)
    """

    enabled: bool = False

    # SSH connection
    ssh_host: str = ""
    ssh_port: int = 2222
    ssh_user: str = ""
    ssh_key_path: str = ""  # Temp file path for private key

    # Paths
    local_root: str = field(
        default_factory=lambda: _get_local_root(),
    )
    remote_root: str = "/Droid/Local"

    # Sync behavior
    sync_on_write: bool = True
    conflict_resolution: str = "latest"  # "latest" = modification time wins
    max_delete_percent: int = 100  # bisync delete safety threshold (100 = disabled)

    # Exclude patterns (rclone filter syntax)
    exclude_patterns: List[str] = field(
        default_factory=lambda: [
            ".git/**",
            "__pycache__/**",
            "*.pyc",
            ".DS_Store",
            ".bisync/**",  # rclone's own state files
            "venvs/**",  # Virtual environments (managed via HTTP API)
            "lost+found/**",  # ext4 filesystem recovery directory by GCP Disks
        ],
    )

    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 2.0

    # Polling interval for remote changes (seconds)
    poll_interval_seconds: float = 30.0

    @classmethod
    def from_session_details(cls) -> "SyncConfig":
        """Create SyncConfig from SESSION_DETAILS for managed VM mode."""
        from droid.session_details import SESSION_DETAILS

        desktop_url = SESSION_DETAILS.assistant.desktop_url
        assistant_id = SESSION_DETAILS.assistant.agent_id

        if os.environ.get("DROID_DESKTOP_SHARED_MOUNT") == "1":
            LOGGER.debug(
                f"{ICONS['file_sync']} [FileSync] Shared desktop mount enabled, sync disabled",
            )
            return cls(enabled=False)

        if not desktop_url:
            LOGGER.debug(
                f"{ICONS['file_sync']} [FileSync] No desktop_url configured, sync disabled",
            )
            return cls(enabled=False)

        ssh_host = cls._extract_host(desktop_url)
        if not ssh_host:
            LOGGER.error(
                f"{ICONS['file_sync']} [FileSync] Could not extract host from desktop_url: {desktop_url}",
            )
            return cls(enabled=False)

        ssh_user = "unityuser"

        ssh_key_path = f"/tmp/.droid_vm_key_{assistant_id}"
        remote_root = "/Local" if os.environ.get("SELF_HOST") == "1" else "/Droid/Local"

        LOGGER.debug(
            f"{ICONS['file_sync']} [FileSync] Config: host={ssh_host}, port=2222, user={ssh_user}, "
            f"local=~/Droid/Local, remote={remote_root}",
        )

        return cls(
            enabled=True,
            ssh_host=ssh_host,
            ssh_user=ssh_user,
            ssh_key_path=ssh_key_path,
            remote_root=remote_root,
        )

    @staticmethod
    def _extract_host(desktop_url: str) -> str:
        """Extract hostname from desktop URL."""
        if not desktop_url:
            return ""
        try:
            parsed = urlparse(desktop_url)
            return parsed.hostname or ""
        except Exception:
            return ""
