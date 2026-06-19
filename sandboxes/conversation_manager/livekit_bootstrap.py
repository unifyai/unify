"""
Local LiveKit server auto-start helper for the ConversationManager sandbox.

Starts a local ``livekit-server --dev`` process when LIVEKIT_URL is either
unset or points to localhost but the server is not yet listening.  Uses the
same lifecycle pattern as ``gateway_bootstrap.py``.

Only used by the sandbox; not part of the production CM or voice runtime.
"""

from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

LG = logging.getLogger("conversation_manager_sandbox")

ProgressCallback = Callable[[str], None]

_LIVEKIT_PORT = 7880
_LIVEKIT_DEV_URL = "ws://localhost:7880"
_LIVEKIT_DEV_KEY = "devkey"  # pragma: allowlist secret
_LIVEKIT_DEV_SECRET = "secret"  # pragma: allowlist secret
_LIVEKIT_STARTUP_TIMEOUT_S = 15.0
_LIVEKIT_POLL_INTERVAL_S = 0.5


@dataclass(frozen=True)
class LiveKitBootstrapResult:
    ok: bool
    summary: str
    process: Optional[subprocess.Popen] = None  # type: ignore[type-arg]
    url: Optional[str] = None


def livekit_should_auto_start() -> bool:
    """Return True if the sandbox should try to start a local LiveKit server.

    Only auto-starts when LIVEKIT_URL is unset or points to localhost, so
    users with LiveKit Cloud credentials are never overridden.
    """
    url = os.environ.get("LIVEKIT_URL", "")
    if not url:
        return True
    return "localhost" in url or "127.0.0.1" in url


def _port_open(port: int) -> bool:
    """Return True if something is already listening on the given port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect(("127.0.0.1", port))
            return True
        except (OSError, ConnectionRefusedError):
            return False


def _find_livekit_binary() -> Optional[str]:
    """Locate the livekit-server binary on PATH or in ~/.local/bin."""
    binary = shutil.which("livekit-server")
    if binary:
        return binary
    local_bin = Path.home() / ".local" / "bin" / "livekit-server"
    if local_bin.exists() and os.access(str(local_bin), os.X_OK):
        return str(local_bin)
    return None


def _install_livekit(repo_root: Path) -> Optional[str]:
    """Run install_livekit.sh to download the binary; return its path or None."""
    install_script = repo_root / "scripts" / "install_livekit.sh"
    if not install_script.exists():
        return None
    local_bin = Path.home() / ".local" / "bin"
    local_bin.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run(
            ["bash", str(install_script), str(local_bin)],
            check=True,
            capture_output=True,
        )
    except Exception:
        return None
    binary = local_bin / "livekit-server"
    if binary.exists() and os.access(str(binary), os.X_OK):
        # Ensure ~/.local/bin is on PATH for this process so the binary is
        # reachable for the rest of the session.
        existing_path = os.environ.get("PATH", "")
        if str(local_bin) not in existing_path.split(os.pathsep):
            os.environ["PATH"] = f"{local_bin}{os.pathsep}{existing_path}"
        return str(binary)
    return None


def try_start_livekit_direct(
    *,
    repo_root: Path,
    progress: Optional[ProgressCallback] = None,
    timeout_s: float = _LIVEKIT_STARTUP_TIMEOUT_S,
) -> LiveKitBootstrapResult:
    """Start a local livekit-server --dev subprocess for voice call support.

    Skips if LIVEKIT_URL points at a non-localhost server (cloud credentials).
    If the server is already listening on port 7880, just wires the env vars
    and returns ok=True without spawning a new process.

    On success, sets LIVEKIT_URL / LIVEKIT_API_KEY / LIVEKIT_API_SECRET in
    os.environ so that call_manager.start_persistent_worker() picks them up.
    """
    _log = progress or (lambda _m: None)

    if not livekit_should_auto_start():
        return LiveKitBootstrapResult(
            ok=True,
            summary="LiveKit already configured (non-local URL — skipping auto-start).",
        )

    if _port_open(_LIVEKIT_PORT):
        _log(f"[livekit] Already running on port {_LIVEKIT_PORT}.")
        os.environ.setdefault("LIVEKIT_URL", _LIVEKIT_DEV_URL)
        os.environ.setdefault("LIVEKIT_API_KEY", _LIVEKIT_DEV_KEY)
        os.environ.setdefault("LIVEKIT_API_SECRET", _LIVEKIT_DEV_SECRET)
        return LiveKitBootstrapResult(
            ok=True,
            summary=f"LiveKit already running on port {_LIVEKIT_PORT}.",
            url=_LIVEKIT_DEV_URL,
        )

    binary = _find_livekit_binary()
    if not binary:
        _log("[livekit] Binary not found — installing…")
        binary = _install_livekit(repo_root)
        if not binary:
            return LiveKitBootstrapResult(
                ok=False,
                summary=(
                    "livekit-server not found and auto-install failed. "
                    "Run `droid voice` to install it manually."
                ),
            )
        _log("[livekit] livekit-server installed.")

    log_dir = repo_root / "logs" / "livekit"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    log_path = log_dir / "livekit-server.log"

    try:
        log_file = open(log_path, "a")  # noqa: WPS515 – intentional open for subprocess
        process = subprocess.Popen(
            [binary, "--dev"],
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
        )
    except Exception as exc:
        return LiveKitBootstrapResult(
            ok=False,
            summary=f"Failed to start livekit-server: {exc}",
        )

    _log(f"[livekit] Starting on port {_LIVEKIT_PORT}…")

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return LiveKitBootstrapResult(
                ok=False,
                summary=(
                    f"livekit-server exited early (code {process.returncode}). "
                    f"Check logs: {log_path}"
                ),
            )
        if _port_open(_LIVEKIT_PORT):
            os.environ["LIVEKIT_URL"] = _LIVEKIT_DEV_URL
            os.environ["LIVEKIT_API_KEY"] = _LIVEKIT_DEV_KEY
            os.environ["LIVEKIT_API_SECRET"] = _LIVEKIT_DEV_SECRET
            _log(f"[livekit] Ready on port {_LIVEKIT_PORT}.")
            return LiveKitBootstrapResult(
                ok=True,
                summary=f"livekit-server started on port {_LIVEKIT_PORT}.",
                process=process,
                url=_LIVEKIT_DEV_URL,
            )
        time.sleep(_LIVEKIT_POLL_INTERVAL_S)

    try:
        process.terminate()
    except Exception:
        pass

    return LiveKitBootstrapResult(
        ok=False,
        summary=(
            f"livekit-server did not bind port {_LIVEKIT_PORT} "
            f"within {timeout_s:.0f}s. Check logs: {log_path}"
        ),
    )


def stop_livekit(
    process: subprocess.Popen,  # type: ignore[type-arg]
    *,
    progress: Optional[ProgressCallback] = None,
) -> None:
    """Terminate a livekit-server subprocess started by ``try_start_livekit_direct``."""
    _log = progress or (lambda _m: None)
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                process.kill()
            _log("[livekit] Stopped.")
    except Exception:
        pass
