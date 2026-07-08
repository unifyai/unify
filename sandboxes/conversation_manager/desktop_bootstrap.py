"""
ConversationManager sandbox support for the Docker-based virtual desktop.

The Docker container provides the virtual desktop (TigerVNC + XFCE4 + noVNC)
plus the Magnitude agent-service for desktop and web-vm sessions.

This module mirrors the pattern in ``agent_service_bootstrap.py``:
- Structured result types with progress callbacks
- Two-tier bootstrap: direct start (image already built) and full auto-bootstrap
- Best-effort cleanup on sandbox exit

Design notes
------------
- These helpers are intentionally conservative and UI-agnostic.
- They return structured results; callers decide how to display progress and errors.
- The desktop container is the only supported computer-use path in the sandbox.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import quote

ProgressCallback = Callable[[str], None]

DESKTOP_IMAGE_TAG = "unity-desktop"
DESKTOP_CONTAINER_NAME = "unity-desktop-sandbox"
_DEFAULT_LOCAL_COMMS_URL = "http://localhost:8787"

# Env vars referenced by deploy/desktop/supervisord.conf via %(ENV_X)s interpolation.
# These must be passed as explicit ``docker run -e KEY=VALUE`` flags — supervisord
# reads os.environ at startup and does not reliably expand vars supplied only via
# ``--env-file`` on all platforms.
CONTAINER_ENV_KEYS: tuple[str, ...] = (
    "UNIFY_KEY",
    "ORCHESTRA_URL",
    "UNITY_GATEWAY_URL",
    "UNITY_COMMS_URL",
    "UNIFY_MODEL",
    "UNITY_AGENT_SERVICE_LLM_MODEL",
    "ANTHROPIC_API_KEY",
)

# Vars supervisord interpolates in deploy/desktop/supervisord.conf — all required.
_SUPERVISORD_ENV_KEYS: tuple[str, ...] = (
    "UNIFY_KEY",
    "ORCHESTRA_URL",
    "UNITY_GATEWAY_URL",
    "UNITY_COMMS_URL",
    "UNIFY_MODEL",
)


def _desktop_novnc_url() -> str:
    """Return the noVNC viewer URL with the VNC password embedded."""
    key = os.environ.get("UNIFY_KEY", "")
    if key:
        # Query params are parsed with URLSearchParams in custom.html; '+' must be
        # percent-encoded as %2B (otherwise it is decoded as a space).
        encoded_key = quote(key, safe="")
        return f"http://localhost:6080/custom.html?password={encoded_key}"
    return "http://localhost:6080/custom.html"


@dataclass(frozen=True)
class DesktopBootstrapResult:
    ok: bool
    summary: str
    container_id: Optional[str] = None


def _docker_available() -> bool:
    """Return True if the Docker CLI is installed and the daemon is reachable."""
    try:
        subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=True,
        )
        return True
    except Exception:
        return False


def _desktop_image_exists(tag: str = DESKTOP_IMAGE_TAG) -> bool:
    """Return True if the desktop Docker image is already built."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", tag],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def _check_magnitude_packages(repo_root: Path) -> Optional[str]:
    """
    Return an error message if the magnitude packages required by agent-service
    are missing or unbuilt. Returns None if everything is present.
    """
    magnitude_dir = repo_root / "magnitude"
    if not magnitude_dir.exists():
        return (
            "Missing `magnitude/` directory. This is Unity's Magnitude fork, required "
            "because agent-service depends on local magnitude-core and magnitude-extract.\n\n"
            "Clone it into the repo root:\n"
            "  git clone <magnitude-repo-url> magnitude"
        )
    for pkg in ("magnitude-core", "magnitude-extract"):
        pkg_dir = magnitude_dir / "packages" / pkg
        if not pkg_dir.exists():
            return f"Missing magnitude package directory: {pkg_dir}"
        dist_dir = pkg_dir / "dist"
        if not dist_dir.exists() or not any(dist_dir.iterdir()):
            return (
                f"Magnitude package '{pkg}' has not been built (missing dist/).\n\n"
                "Build it with:\n"
                f"  cd magnitude/packages/{pkg} && npm ci && npm run build"
            )
    return None


def _build_desktop_image(
    *,
    repo_root: Path,
    tag: str = DESKTOP_IMAGE_TAG,
    progress: Optional[ProgressCallback] = None,
) -> bool:
    """Build the desktop Docker image. Returns True on success."""
    progress = progress or (lambda _m: None)
    dockerfile = repo_root / "deploy" / "desktop" / "Dockerfile"
    if not dockerfile.exists():
        progress(f"[desktop] Missing Dockerfile at {dockerfile}")
        return False

    # The Dockerfile COPYs magnitude packages into the image; verify they exist.
    mag_err = _check_magnitude_packages(repo_root)
    if mag_err:
        progress(f"[desktop] {mag_err}")
        return False

    progress(
        f"[desktop] Building Docker image '{tag}' (this may take a few minutes on first run)...",
    )
    try:
        subprocess.run(
            ["docker", "build", "-t", tag, "-f", str(dockerfile), "."],
            cwd=str(repo_root),
            check=True,
            timeout=600,
        )
        progress(f"[desktop] Image '{tag}' built successfully")
        return True
    except subprocess.CalledProcessError as exc:
        progress(
            f"[desktop] Docker build failed (exit {exc.returncode})",
        )
        return False
    except subprocess.TimeoutExpired:
        progress("[desktop] Docker build timed out (10 min limit)")
        return False


def _find_running_container(name: str = DESKTOP_CONTAINER_NAME) -> Optional[str]:
    """Return the container ID if a container with the given name is running."""
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.Id}}",
                name,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        container_id = (result.stdout or "").strip()
        if not container_id:
            return None
        # Verify it's actually running.
        state_result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}}",
                name,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if (state_result.stdout or "").strip().lower() == "true":
            return container_id
        return None
    except Exception:
        return None


def stop_desktop_container(
    name: str = DESKTOP_CONTAINER_NAME,
    *,
    progress: Optional[ProgressCallback] = None,
) -> None:
    """Best-effort stop and remove the desktop container."""
    progress = progress or (lambda _m: None)
    try:
        subprocess.run(
            ["docker", "stop", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
        progress(f"[desktop] Stopped container '{name}'")
    except Exception:
        pass
    try:
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception:
        pass


def _free_desktop_ports(
    ports: tuple[int, ...] = (5901, 6080, 3000),
    *,
    progress: Optional[ProgressCallback] = None,
) -> None:
    """Stop any Docker container (any name) that holds the given ports, then kill
    non-Docker processes that are still listening, so ``docker run`` does not fail
    with "address already in use".
    """
    progress = progress or (lambda _m: None)
    for port in ports:
        # Find containers (running or stopped) that bind this port.
        try:
            result = subprocess.run(
                [
                    "docker",
                    "ps",
                    "-a",
                    "--filter",
                    f"publish={port}",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for cname in (result.stdout or "").splitlines():
                cname = cname.strip()
                if not cname:
                    continue
                progress(
                    f"[desktop] Removing container '{cname}' holding port {port}...",
                )
                subprocess.run(
                    ["docker", "rm", "-f", cname],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
        except Exception:
            pass
        # Kill any non-Docker process still listening on the port.
        try:
            import signal

            lsof = subprocess.run(
                ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for pid_s in (lsof.stdout or "").splitlines():
                try:
                    os.kill(int(pid_s.strip()), signal.SIGTERM)
                except Exception:
                    pass
        except Exception:
            pass


def _validate_agent_service(
    *,
    agent_server_url: str,
    unify_key: str,
    timeout_s: float = 2.5,
) -> bool:
    """Return True iff the agent-service inside the container responds with HTTP 200."""
    try:
        import httpx
    except Exception:
        httpx = None  # type: ignore

    url = str(agent_server_url).rstrip("/") + "/sessions"
    headers = {"authorization": f"Bearer {unify_key}"}
    try:
        if httpx is None:
            from urllib.request import Request, urlopen

            req = Request(url, headers=headers)
            with urlopen(req, timeout=float(timeout_s)) as resp:  # nosec B310
                return int(getattr(resp, "status", 0) or 0) == 200
        resp = httpx.get(url, headers=headers, timeout=float(timeout_s))
        return int(resp.status_code) == 200
    except Exception:
        return False


def _wait_for_container_ready(
    *,
    container_name: str,
    agent_server_url: str,
    unify_key: str,
    timeout_s: float = 60.0,
    poll_interval_s: float = 1.0,
) -> DesktopBootstrapResult:
    """Poll the containerized agent-service until it responds or the container dies."""
    start_t = time.time()
    deadline = start_t + float(timeout_s)
    while time.time() < deadline:
        # Check if the container is still running.
        if _find_running_container(container_name) is None:
            logs = _container_startup_logs(container_name)
            summary = "Desktop container exited during startup"
            if logs:
                summary = f"{summary}\n{logs}"
            return DesktopBootstrapResult(
                ok=False,
                summary=summary,
                container_id=None,
            )
        if _validate_agent_service(
            agent_server_url=agent_server_url,
            unify_key=unify_key,
        ):
            container_id = _find_running_container(container_name)
            return DesktopBootstrapResult(
                ok=True,
                summary=f"Desktop container ready — view at {_desktop_novnc_url()}",
                container_id=container_id,
            )
        time.sleep(float(poll_interval_s))
    return DesktopBootstrapResult(
        ok=False,
        summary=f"Desktop container did not become ready within {timeout_s:.0f}s",
        container_id=_find_running_container(container_name),
    )


def _parse_env_file(env_file: Path) -> dict[str, str]:
    """Parse a dotenv-style file into a key/value mapping."""
    values: dict[str, str] = {}
    try:
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            values[key] = value
    except Exception:
        pass
    return values


def _rewrite_local_host_for_container(value: str) -> str:
    """Map host-loopback URLs so processes inside Docker reach the host."""
    return value.replace("localhost", "host.docker.internal").replace(
        "127.0.0.1",
        "host.docker.internal",
    )


def _container_env_values(
    *,
    repo_root: Path,
    llm_model: str | None = None,
) -> dict[str, str]:
    """Resolve container env vars from the process environment and repo ``.env``."""
    file_values = _parse_env_file(repo_root / ".env")
    resolved: dict[str, str] = {}
    for key in CONTAINER_ENV_KEYS:
        value = os.environ.get(key) or file_values.get(key) or ""
        value = value.strip()
        if value:
            resolved[key] = value

    gateway = (
        resolved.get("UNITY_GATEWAY_URL")
        or resolved.get("UNITY_COMMS_URL")
        or _DEFAULT_LOCAL_COMMS_URL
    )
    comms = resolved.get("UNITY_COMMS_URL") or gateway
    gateway = resolved.get("UNITY_GATEWAY_URL") or comms
    resolved["UNITY_GATEWAY_URL"] = gateway
    resolved["UNITY_COMMS_URL"] = comms

    if llm_model:
        resolved["UNITY_AGENT_SERVICE_LLM_MODEL"] = llm_model.strip()
        resolved["UNIFY_MODEL"] = llm_model.strip()

    if not resolved.get("UNIFY_MODEL"):
        try:
            from unify.settings import SETTINGS

            resolved["UNIFY_MODEL"] = SETTINGS.UNIFY_MODEL
        except Exception:
            pass

    if not resolved.get("UNITY_AGENT_SERVICE_LLM_MODEL"):
        resolved["UNITY_AGENT_SERVICE_LLM_MODEL"] = resolved.get(
            "UNIFY_MODEL",
            "",
        )

    for key, value in list(resolved.items()):
        resolved[key] = _rewrite_local_host_for_container(value)
    return resolved


def _docker_env_args(
    *,
    repo_root: Path,
    llm_model: str | None = None,
) -> list[str]:
    """Return ``docker run`` ``-e KEY=VALUE`` args for the desktop container."""
    args: list[str] = []
    for key, value in _container_env_values(
        repo_root=repo_root,
        llm_model=llm_model,
    ).items():
        args.extend(["-e", f"{key}={value}"])
    return args


def _running_container_env(container_name: str) -> dict[str, str]:
    """Return env vars baked into a running container's config."""
    try:
        import json

        result = subprocess.run(
            ["docker", "inspect", "-f", "{{json .Config.Env}}", container_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return {}
        raw = (result.stdout or "").strip()
        if not raw:
            return {}
        entries = json.loads(raw)
        env: dict[str, str] = {}
        for entry in entries:
            if "=" in entry:
                key, value = entry.split("=", 1)
                env[key] = value
        return env
    except Exception:
        return {}


def _container_matches_llm_model(
    container_name: str,
    *,
    llm_model: str,
) -> bool:
    """Return True when the running container was started with the same LLM model."""
    env = _running_container_env(container_name)
    configured = env.get("UNITY_AGENT_SERVICE_LLM_MODEL", "").strip()
    return configured == llm_model.strip()


def _container_startup_logs(container_name: str, *, tail: int = 40) -> str:
    """Return recent container logs for startup failure diagnostics."""
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = (result.stdout or "") + (result.stderr or "")
        return output.strip()
    except Exception:
        return ""


def _start_container(
    *,
    repo_root: Path,
    tag: str = DESKTOP_IMAGE_TAG,
    name: str = DESKTOP_CONTAINER_NAME,
    llm_model: str | None = None,
    progress: Optional[ProgressCallback] = None,
) -> Optional[str]:
    """Run the desktop container in detached mode. Returns container ID or None."""
    progress = progress or (lambda _m: None)

    env_values = _container_env_values(repo_root=repo_root, llm_model=llm_model)
    missing = [key for key in _SUPERVISORD_ENV_KEYS if not env_values.get(key)]
    if missing:
        progress(
            "[desktop] Missing required env for desktop container: "
            f"{', '.join(missing)}. "
            "Run the install wizard or add them to .env "
            "(UNITY_GATEWAY_URL/UNITY_COMMS_URL default to http://localhost:8787).",
        )
        return None

    cmd = [
        "docker",
        "run",
        "--rm",
        "-d",
        "--name",
        name,
        "-p",
        "6080:6080",
        "-p",
        "5901:5900",
        "-p",
        "3000:3000",
        # Allow the container to reach host-side services (e.g. Orchestra).
        "--add-host=host.docker.internal:host-gateway",
    ]
    supervisord_conf = repo_root / "deploy/desktop/supervisord.conf"
    if supervisord_conf.is_file():
        cmd.extend(
            [
                "-v",
                f"{supervisord_conf.resolve()}:/app/desktop/supervisord.conf:ro",
            ],
        )
    cmd.extend(_docker_env_args(repo_root=repo_root, llm_model=llm_model))
    cmd.append(tag)

    progress(f"[desktop] Starting container '{name}' from image '{tag}'...")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(repo_root),
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            progress(f"[desktop] docker run failed: {stderr}")
            return None
        container_id = (result.stdout or "").strip()
        progress(f"[desktop] Container started (ID: {container_id[:12]})")
        return container_id
    except Exception as exc:
        progress(f"[desktop] Failed to start container: {exc}")
        return None


def try_start_desktop_direct(
    *,
    repo_root: Path,
    agent_server_url: str,
    llm_model: str | None = None,
    progress: Optional[ProgressCallback] = None,
    timeout_s: float = 60.0,
) -> DesktopBootstrapResult:
    """
    Start the desktop container assuming the Docker image is already built.

    If a container with the expected name is already running and healthy,
    reuse it. Otherwise, start a fresh one.
    """
    progress = progress or (lambda _m: None)
    unify_key = os.environ.get("UNIFY_KEY") or ""
    if not unify_key:
        return DesktopBootstrapResult(
            ok=False,
            summary="UNIFY_KEY is not set (required for agent-service auth)",
        )

    # Already running and healthy with the requested model?
    existing = _find_running_container(DESKTOP_CONTAINER_NAME)
    if existing and _validate_agent_service(
        agent_server_url=agent_server_url,
        unify_key=unify_key,
    ):
        if llm_model and not _container_matches_llm_model(
            DESKTOP_CONTAINER_NAME,
            llm_model=llm_model,
        ):
            progress(
                f"[desktop] LLM model changed to {llm_model!r} — recreating container...",
            )
            stop_desktop_container(DESKTOP_CONTAINER_NAME, progress=progress)
            existing = None
        else:
            progress(
                f"[desktop] Reusing existing desktop container — view at {_desktop_novnc_url()}",
            )
            return DesktopBootstrapResult(
                ok=True,
                summary=f"Desktop container already running — view at {_desktop_novnc_url()}",
                container_id=existing,
            )

    # Stop any stale container with the same name.
    if existing:
        progress("[desktop] Stopping stale container...")
        stop_desktop_container(DESKTOP_CONTAINER_NAME, progress=progress)

    if not _docker_available():
        return DesktopBootstrapResult(
            ok=False,
            summary=(
                "Docker is not available. Desktop mode requires Docker to run the "
                "virtual desktop environment. Install Docker or use --agent-mode web"
            ),
        )

    if not _desktop_image_exists():
        return DesktopBootstrapResult(
            ok=False,
            summary=(
                f"Docker image '{DESKTOP_IMAGE_TAG}' not found. "
                "Run with --agent-service-bootstrap auto to build it automatically, "
                f"or build manually: docker build -t {DESKTOP_IMAGE_TAG} -f deploy/desktop/Dockerfile ."
            ),
        )

    # Free any port conflicts before starting — another container or process may
    # already hold ports 5900/6080/3000 from a previous session.
    _free_desktop_ports(progress=progress)

    container_id = _start_container(
        repo_root=repo_root,
        llm_model=llm_model,
        progress=progress,
    )
    if not container_id:
        return DesktopBootstrapResult(
            ok=False,
            summary="Failed to start desktop container",
        )

    return _wait_for_container_ready(
        container_name=DESKTOP_CONTAINER_NAME,
        agent_server_url=agent_server_url,
        unify_key=unify_key,
        timeout_s=timeout_s,
    )


def try_auto_bootstrap_desktop(
    *,
    repo_root: Path,
    agent_server_url: str,
    llm_model: str | None = None,
    progress: Optional[ProgressCallback] = None,
    timeout_s: float = 90.0,
) -> DesktopBootstrapResult:
    """
    Full bootstrap: check Docker, build image if needed, start container.

    This is the "auto" tier that handles everything from scratch.
    """
    progress = progress or (lambda _m: None)
    unify_key = os.environ.get("UNIFY_KEY") or ""
    if not unify_key:
        return DesktopBootstrapResult(
            ok=False,
            summary="UNIFY_KEY is not set (required for agent-service auth)",
        )

    # Already running and healthy with the requested model?
    existing = _find_running_container(DESKTOP_CONTAINER_NAME)
    if existing and _validate_agent_service(
        agent_server_url=agent_server_url,
        unify_key=unify_key,
    ):
        if llm_model and not _container_matches_llm_model(
            DESKTOP_CONTAINER_NAME,
            llm_model=llm_model,
        ):
            progress(
                f"[desktop] LLM model changed to {llm_model!r} — recreating container...",
            )
            stop_desktop_container(DESKTOP_CONTAINER_NAME, progress=progress)
            existing = None
        else:
            progress(
                f"[desktop] Reusing existing desktop container — view at {_desktop_novnc_url()}",
            )
            return DesktopBootstrapResult(
                ok=True,
                summary=f"Desktop container already running — view at {_desktop_novnc_url()}",
                container_id=existing,
            )

    # Stop any stale container.
    if existing:
        progress("[desktop] Stopping stale container...")
        stop_desktop_container(DESKTOP_CONTAINER_NAME, progress=progress)

    if not _docker_available():
        return DesktopBootstrapResult(
            ok=False,
            summary=(
                "Docker is not available (either not installed or daemon not running). "
                "Desktop mode requires Docker to run the virtual desktop environment.\n\n"
                "Options:\n"
                "  1. Install and start Docker, then re-run the sandbox\n"
                "  2. Use --agent-mode web for browser-only mode (no Docker required)"
            ),
        )

    # Build the image if it doesn't exist.
    if not _desktop_image_exists():
        if not _build_desktop_image(repo_root=repo_root, progress=progress):
            return DesktopBootstrapResult(
                ok=False,
                summary="Failed to build desktop Docker image",
            )

    # Free any port conflicts before starting — another container or process may
    # already hold ports 5900/6080/3000 from a previous session.
    _free_desktop_ports(progress=progress)

    container_id = _start_container(
        repo_root=repo_root,
        llm_model=llm_model,
        progress=progress,
    )
    if not container_id:
        return DesktopBootstrapResult(
            ok=False,
            summary="Failed to start desktop container",
        )

    return _wait_for_container_ready(
        container_name=DESKTOP_CONTAINER_NAME,
        agent_server_url=agent_server_url,
        unify_key=unify_key,
        timeout_s=timeout_s,
    )


def bootstrap_desktop_container(
    *,
    repo_root: Path,
    agent_server_url: str,
    progress: Optional[ProgressCallback] = None,
    timeout_s: float = 90.0,
) -> DesktopBootstrapResult:
    """
    Start the desktop container, building the Docker image when needed.

    Tries a direct start first; on failure, runs the full auto-bootstrap path.
    """
    progress = progress or (lambda _m: None)
    direct = try_start_desktop_direct(
        repo_root=repo_root,
        agent_server_url=agent_server_url,
        progress=progress,
        timeout_s=timeout_s,
    )
    if direct.ok:
        return direct
    progress(f"[desktop] {direct.summary}")
    progress("[desktop] Attempting full bootstrap (build image if needed)...")
    return try_auto_bootstrap_desktop(
        repo_root=repo_root,
        agent_server_url=agent_server_url,
        progress=progress,
        timeout_s=timeout_s,
    )


def diagnose_desktop_setup(
    *,
    repo_root: Path,
    agent_server_url: str,
) -> str:
    """Return user-facing help text for desktop mode setup issues."""
    lines: list[str] = []
    lines.append(
        "Desktop mode requires a Docker container running the virtual desktop environment.",
    )
    lines.append("")

    if not _docker_available():
        lines.append("Issue: Docker is not available.")
        lines.append("  - Install Docker: https://docs.docker.com/get-docker/")
        lines.append("  - Ensure the Docker daemon is running")
        lines.append("")
    else:
        lines.append("Docker: OK")
        lines.append("")

    mag_err = _check_magnitude_packages(repo_root)
    if mag_err:
        lines.append(f"Issue: {mag_err}")
        lines.append("")
    else:
        lines.append("Magnitude packages: OK")
        lines.append("")

    if not _desktop_image_exists():
        lines.append(f"Issue: Docker image '{DESKTOP_IMAGE_TAG}' not found.")
        lines.append("  Build it with:")
        lines.append(
            f"    docker build -t {DESKTOP_IMAGE_TAG} -f deploy/desktop/Dockerfile .",
        )
        lines.append("")
    else:
        lines.append(f"Image '{DESKTOP_IMAGE_TAG}': OK")
        lines.append("")

    running = _find_running_container(DESKTOP_CONTAINER_NAME)
    if running:
        lines.append(f"Container '{DESKTOP_CONTAINER_NAME}': running ({running[:12]})")
    else:
        lines.append(f"Container '{DESKTOP_CONTAINER_NAME}': not running")
        lines.append("  Start it with:")
        lines.append(
            f"    docker run --rm -d --name {DESKTOP_CONTAINER_NAME} "
            f"-p 6080:6080 -p 5901:5900 -p 3000:3000 --env-file .env {DESKTOP_IMAGE_TAG}",
        )
    lines.append("")

    env_file = repo_root / ".env"
    if not env_file.exists():
        lines.append("Warning: no .env file found in repo root.")
        lines.append(
            "  The container needs ANTHROPIC_API_KEY, ORCHESTRA_URL, and UNIFY_KEY.",
        )
        lines.append("")

    lines.append(
        "Alternative: use --agent-mode web for browser-only mode (no Docker required)",
    )
    lines.append("")
    lines.append("Docs:")
    lines.append("  - desktop/README.md")

    return "\n".join(lines)
