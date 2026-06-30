"""
Local gateway auto-start helper for the ConversationManager sandbox.

Starts a local ``unify.gateway`` process on port 8787 for:
- UniLLM proxy traffic used by agent-service / Magnitude computer use
- Outbound SMS/calls when Twilio credentials are also configured

Only used by the sandbox; not part of the production gateway or CM runtime.
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

LG = logging.getLogger("conversation_manager_sandbox")

ProgressCallback = Callable[[str], None]

_GATEWAY_HEALTH_PATH = "/health"
# Port 8787 matches the CM's LOCAL_COMMS_PORT default (UNITY_CONVERSATION_LOCAL_COMMS_PORT).
# Starting on this exact port means the CM's _local_comms_base_url() fallback resolves
# correctly without needing UNITY_COMMS_URL to be set (which would be too late after
# pydantic-settings has already instantiated SETTINGS at import time).
_GATEWAY_PORT = 8787
_GATEWAY_STARTUP_TIMEOUT_S = 15.0
_GATEWAY_POLL_INTERVAL_S = 0.5

# Minimum env vars required for outbound SMS/calls.
_OUTBOUND_REQUIRED_VARS = (
    "ORCHESTRA_ADMIN_KEY",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
)


@dataclass(frozen=True)
class GatewayBootstrapResult:
    ok: bool
    summary: str
    process: Optional[subprocess.Popen] = None  # type: ignore[type-arg]
    port: Optional[int] = None
    url: Optional[str] = None


def outbound_comms_configured() -> bool:
    """Return True if the minimum credentials for outbound SMS/calls are present."""
    return all(os.environ.get(v) for v in _OUTBOUND_REQUIRED_VARS)


def gateway_prerequisites_met() -> bool:
    """Return True when the local gateway can authenticate UniLLM proxy requests."""
    return bool(os.environ.get("UNIFY_KEY"))


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _health_check(url: str) -> bool:
    try:
        import urllib.request

        with urllib.request.urlopen(f"{url}{_GATEWAY_HEALTH_PATH}", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


def try_start_gateway_direct(
    *,
    repo_root: Path,
    progress: Optional[ProgressCallback] = None,
    timeout_s: float = _GATEWAY_STARTUP_TIMEOUT_S,
) -> GatewayBootstrapResult:
    """
    Start a local unify.gateway subprocess on port 8787.

    The gateway exposes ``/unillm`` for agent-service computer use and, when
    Twilio credentials are configured, outbound SMS/call routes as well.

    Returns ok=True with process=None if the gateway is already healthy on that
    port (e.g. from a previous sandbox run).
    """
    _log = progress or (lambda _m: None)

    if not gateway_prerequisites_met():
        return GatewayBootstrapResult(
            ok=False,
            summary="UNIFY_KEY is not set (required for local gateway UniLLM proxy)",
        )

    port = _GATEWAY_PORT
    url = f"http://localhost:{port}"

    if _health_check(url):
        _log(f"[gateway] Already running on port {port}.")
        return GatewayBootstrapResult(
            ok=True,
            summary=f"Gateway already running on port {port}.",
            port=port,
            url=url,
        )

    if not _port_is_free(port):
        return GatewayBootstrapResult(
            ok=False,
            summary=(
                f"Port {port} is in use by an unrelated process. "
                "Free that port or set UNITY_CONVERSATION_LOCAL_COMMS_PORT to override."
            ),
        )

    python = str(repo_root / ".venv" / "bin" / "python")
    if not Path(python).exists():
        python = "python"

    gateway_env = {
        **os.environ,
        "UNITY_GATEWAY_HOST": "127.0.0.1",
        "UNITY_GATEWAY_PORT": str(port),
    }

    log_dir = repo_root / "logs" / "gateway"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    log_path = log_dir / f"gateway_{port}.log"

    try:
        log_file = open(log_path, "a")  # noqa: WPS515 – intentional open for subprocess
        process = subprocess.Popen(
            [python, "-m", "unify.gateway", "serve"],
            cwd=str(repo_root),
            env=gateway_env,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
        )
    except Exception as exc:
        return GatewayBootstrapResult(
            ok=False,
            summary=f"Failed to start gateway subprocess: {exc}",
        )

    _log(f"[gateway] Starting on port {port}…")

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return GatewayBootstrapResult(
                ok=False,
                summary=(
                    f"Gateway process exited early (code {process.returncode}). "
                    f"Check logs: {log_path}"
                ),
            )
        if _health_check(url):
            _log(f"[gateway] Ready on port {port}.")
            summary = f"Gateway started on port {port}."
            if not outbound_comms_configured():
                summary += " (UniLLM proxy only; SMS/calls need Twilio credentials)"
            return GatewayBootstrapResult(
                ok=True,
                summary=summary,
                process=process,
                port=port,
                url=url,
            )
        time.sleep(_GATEWAY_POLL_INTERVAL_S)

    try:
        process.terminate()
    except Exception:
        pass

    return GatewayBootstrapResult(
        ok=False,
        summary=(
            f"Gateway did not become ready within {timeout_s:.0f}s. "
            f"Check logs: {log_path}"
        ),
    )


def stop_gateway(process: subprocess.Popen, *, progress: Optional[ProgressCallback] = None) -> None:  # type: ignore[type-arg]
    """Terminate a gateway subprocess started by ``try_start_gateway_direct``."""
    _log = progress or (lambda _m: None)
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                process.kill()
            _log("[gateway] Stopped.")
    except Exception:
        pass
