"""Local self-host stack lifecycle helpers for coordinator integration tests."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import requests

LOCAL_COMMUNICATION_CONFIG = Path(
    os.getenv(
        "TEST_COMMUNICATION_LOCAL_CONFIG",
        "/tmp/communication-local.config",
    ),
)
DEFAULT_LOCAL_ORCHESTRA_URL = "http://localhost:8000/v0"
DEFAULT_LOCAL_ADAPTERS_URL = "http://127.0.0.1:8081"
DEFAULT_LOCAL_COMMS_URL = "http://127.0.0.1:8082"
DEFAULT_LOCAL_PUBSUB_HOST = "localhost:8085"
DEFAULT_LOCAL_GCP_PROJECT_ID = "local-test-project"
DEFAULT_LOCAL_ADMIN_KEY = "local-admin-key"
DEFAULT_ORCHESTRA_DB_PORT = 5432
LOCAL_STACK_START_TIMEOUT_SECONDS = 600
LOCAL_STACK_WAIT_TIMEOUT_SECONDS = 300
SELF_HOST_BOOTSTRAP_PATH = Path("/tmp/self-host-bootstrap.json")


@dataclass(frozen=True)
class LocalStackUrls:
    """Resolved service URLs for the local self-host stack."""

    orchestra_url: str
    adapters_url: str
    comms_url: str
    pubsub_emulator_host: str
    gcp_project_id: str
    pubsub_suffix: str


@dataclass
class ManagedLocalStack:
    """Tracks whether this pytest session started the local self-host stack."""

    started_by_session: bool
    urls: LocalStackUrls


def _real_home() -> Path:
    """Return the developer home directory before Unity pytest HOME sandboxing."""

    configured = os.getenv("UNITY_REAL_HOME", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home()


def _resolve_unify_root() -> Path:
    configured = os.getenv("UNIFY_STACK_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser()
    return _real_home() / "Unify"


def resolve_sibling_repo(name: str) -> Path:
    """Return a sibling repo path, preferring ``*-teams-unify`` worktrees."""

    env_key = f"{name.upper()}_REPO_PATH"
    configured = os.getenv(env_key, "").strip()
    if configured:
        return Path(configured).expanduser()

    unify_root = _resolve_unify_root()
    for candidate in (f"{name}-teams-unify", name):
        path = unify_root / candidate
        if path.is_dir():
            return path
    return unify_root / name


def _resolve_stack_script() -> tuple[Path, Path]:
    unity_repo = resolve_sibling_repo("unity")
    stack_script = unity_repo / "scripts" / "stack.sh"
    if not stack_script.is_file():
        raise FileNotFoundError(f"stack.sh not found at {stack_script}")
    return unity_repo, stack_script


def _self_host_credentials_path() -> Path:
    configured = os.getenv("SELF_HOST_CREDENTIALS_FILE", "").strip()
    if configured:
        return Path(configured).expanduser()
    return _real_home() / ".unity" / "self-host-credentials.json"


def _orchestra_db_port() -> int:
    raw = os.getenv("ORCHESTRA_DB_PORT", str(DEFAULT_ORCHESTRA_DB_PORT)).strip()
    return int(raw)


def _ensure_orchestra_db_port_available() -> None:
    """Stop Docker containers blocking Orchestra's configured PostgreSQL port."""

    port = _orchestra_db_port()
    completed = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"publish={port}",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Failed to inspect Docker for PostgreSQL port conflicts: "
            f"{completed.stderr or completed.stdout}",
        )

    for container_name in completed.stdout.splitlines():
        name = container_name.strip()
        if not name:
            continue
        print(f"Stopping container '{name}' occupying port {port}...")
        stop = subprocess.run(
            ["docker", "stop", name],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if stop.returncode != 0:
            raise RuntimeError(
                f"Failed to stop container '{name}' on port {port}: "
                f"{stop.stderr or stop.stdout}",
            )

    probe = subprocess.run(
        ["lsof", "-i", f":{port}", "-sTCP:LISTEN", "-t"],
        capture_output=True,
        text=True,
        check=False,
    )
    if probe.returncode == 0 and probe.stdout.strip():
        pids = probe.stdout.strip().replace("\n", ", ")
        raise RuntimeError(
            f"Port {port} is still in use by process(es) {pids}. "
            "Stop them before running local-stack integration tests.",
        )


def _parse_communication_local_config(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_local_stack_urls() -> LocalStackUrls:
    config_values = _parse_communication_local_config(LOCAL_COMMUNICATION_CONFIG)
    orchestra_url = os.getenv("TEST_ORCHESTRA_URL", DEFAULT_LOCAL_ORCHESTRA_URL).rstrip(
        "/",
    )
    adapters_url = os.getenv(
        "TEST_ADAPTERS_URL",
        config_values.get("UNITY_ADAPTERS_URL", DEFAULT_LOCAL_ADAPTERS_URL),
    ).rstrip("/")
    comms_url = os.getenv(
        "TEST_COMMS_APP_URL",
        config_values.get("UNITY_COMMS_URL", DEFAULT_LOCAL_COMMS_URL),
    ).rstrip("/")
    pubsub_emulator_host = os.getenv(
        "PUBSUB_EMULATOR_HOST",
        config_values.get("PUBSUB_EMULATOR_HOST", DEFAULT_LOCAL_PUBSUB_HOST),
    )
    gcp_project_id = os.getenv(
        "TEST_GCP_PROJECT_ID",
        config_values.get("GCP_PROJECT_ID", DEFAULT_LOCAL_GCP_PROJECT_ID),
    )
    pubsub_suffix = os.getenv("TEST_PUBSUB_SUFFIX", "-staging")
    return LocalStackUrls(
        orchestra_url=orchestra_url,
        adapters_url=adapters_url,
        comms_url=comms_url,
        pubsub_emulator_host=pubsub_emulator_host,
        gcp_project_id=gcp_project_id,
        pubsub_suffix=pubsub_suffix,
    )


def _stack_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    real_home = str(_real_home())
    env["HOME"] = real_home
    env.setdefault("ORCHESTRA_REPO_PATH", str(resolve_sibling_repo("orchestra")))
    env.setdefault(
        "COMMUNICATION_REPO_PATH",
        str(resolve_sibling_repo("communication")),
    )
    env.setdefault("UNITY_REPO_PATH", str(resolve_sibling_repo("unity")))
    env.setdefault("CONSOLE_REPO_PATH", str(resolve_sibling_repo("console")))
    env.setdefault("UNIFY_REPO_PATH", str(resolve_sibling_repo("unify")))
    env.setdefault("UNIFY_STACK_ROOT", str(_resolve_unify_root()))
    env.setdefault("ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS", "0")
    return env


def _load_self_host_bootstrap_credentials() -> dict:
    for path in (_self_host_credentials_path(), SELF_HOST_BOOTSTRAP_PATH):
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    return {}


def resolve_local_stack_credentials() -> tuple[str, str]:
    bootstrap = _load_self_host_bootstrap_credentials()
    unify_key = os.getenv("UNIFY_KEY", bootstrap.get("api_key", "")).strip()
    admin_key = os.getenv("ORCHESTRA_ADMIN_KEY", DEFAULT_LOCAL_ADMIN_KEY).strip()
    return unify_key, admin_key


def apply_local_stack_credentials(*, unify_key: str, admin_key: str) -> None:
    os.environ["UNIFY_KEY"] = unify_key
    os.environ["ORCHESTRA_ADMIN_KEY"] = admin_key


def _local_stack_service_reachable(url: str, *, api_key: str) -> bool:
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
    for path in ("", "/health", "/healthz"):
        try:
            response = requests.get(
                f"{url.rstrip('/')}{path}",
                headers=headers,
                timeout=3,
            )
            if response.status_code < 500:
                return True
        except requests.RequestException:
            continue
    return False


def _orchestra_reachable(orchestra_url: str, *, api_key: str) -> bool:
    try:
        response = requests.get(
            f"{orchestra_url.rstrip('/')}/user/basic-info",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=5,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


def local_stack_is_ready(
    urls: LocalStackUrls,
    *,
    unify_key: str,
    admin_key: str,
) -> bool:
    if not unify_key or not admin_key:
        return False
    if not _orchestra_reachable(urls.orchestra_url, api_key=unify_key):
        return False
    if not _local_stack_service_reachable(urls.adapters_url, api_key=admin_key):
        return False
    if not _local_stack_service_reachable(urls.comms_url, api_key=admin_key):
        return False
    return True


def wait_for_local_stack(
    urls: LocalStackUrls,
    *,
    unify_key: str,
    admin_key: str,
    timeout_seconds: int = LOCAL_STACK_WAIT_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if local_stack_is_ready(
            urls,
            unify_key=unify_key,
            admin_key=admin_key,
        ):
            return
        time.sleep(2)
    raise TimeoutError(
        "Timed out waiting for local stack at "
        f"Orchestra={urls.orchestra_url}, Adapters={urls.adapters_url}, "
        f"Comms={urls.comms_url}",
    )


def local_stack_auto_manage_enabled() -> bool:
    return os.getenv("LOCAL_STACK_NO_AUTO", "").strip().lower() not in {
        "1",
        "true",
        "yes",
    }


def _seed_local_orchestra_rbac() -> None:
    orchestra_repo = resolve_sibling_repo("orchestra")
    seed_path = orchestra_repo / "orchestra" / "tests" / "seeding.sql"
    if not seed_path.is_file():
        raise FileNotFoundError(f"Orchestra RBAC seed file not found at {seed_path}")

    content = seed_path.read_text(encoding="utf-8")
    marker = "-- RBAC: Permissions"
    if marker not in content:
        raise RuntimeError(f"RBAC seed marker not found in {seed_path}")
    rbac_sql = content[content.index(marker) :]

    db_container = os.getenv("ORCHESTRA_DB_CONTAINER", "orchestra-local-db")
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            db_container,
            "psql",
            "-U",
            "orchestra",
            "-d",
            "orchestra",
        ],
        input=rbac_sql,
        text=True,
        capture_output=True,
        timeout=30,
        env=_stack_subprocess_env(),
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Failed to seed local Orchestra RBAC: "
            f"{completed.stderr or completed.stdout}",
        )


def _purge_local_orchestra() -> None:
    orchestra_repo = resolve_sibling_repo("orchestra")
    local_script = orchestra_repo / "scripts" / "local.sh"
    if not local_script.is_file():
        raise FileNotFoundError(f"Orchestra local.sh not found at {local_script}")

    subprocess.run(
        ["bash", str(local_script), "purge"],
        cwd=orchestra_repo,
        env=_stack_subprocess_env(),
        check=True,
        timeout=120,
    )


def _bootstrap_self_host(urls: LocalStackUrls) -> None:
    orchestra_repo = resolve_sibling_repo("orchestra")
    bootstrap_script = orchestra_repo / "scripts" / "bootstrap_self_host.sh"
    if not bootstrap_script.is_file():
        raise FileNotFoundError(
            f"Self-host bootstrap script not found at {bootstrap_script}",
        )

    env = _stack_subprocess_env()
    env["SELF_HOST"] = "1"
    env["PUBSUB_EMULATOR_HOST"] = urls.pubsub_emulator_host
    env.setdefault("GCP_PROJECT_ID", urls.gcp_project_id)
    subprocess.run(
        ["bash", str(bootstrap_script)],
        cwd=orchestra_repo,
        env=env,
        check=True,
        timeout=120,
    )


def reset_and_start_local_stack(urls: LocalStackUrls) -> None:
    """Tear down, purge Orchestra, and bring up a fresh local self-host stack."""

    _ensure_orchestra_db_port_available()

    unity_repo, stack_script = _resolve_stack_script()
    subprocess.run(
        ["bash", str(stack_script), "down"],
        cwd=unity_repo,
        env=_stack_subprocess_env(),
        check=False,
        timeout=120,
    )
    _purge_local_orchestra()
    completed = subprocess.run(
        ["bash", str(stack_script), "up"],
        cwd=unity_repo,
        env=_stack_subprocess_env(),
        check=False,
        timeout=LOCAL_STACK_START_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        print(
            f"stack.sh up exited {completed.returncode}. "
            "Unity/Console startup may have failed; continuing if Orchestra, "
            "Adapters, and Comms are reachable.",
        )
    _seed_local_orchestra_rbac()
    _bootstrap_self_host(urls)


def stop_local_stack() -> None:
    unity_repo, stack_script = _resolve_stack_script()
    subprocess.run(
        ["bash", str(stack_script), "down"],
        cwd=unity_repo,
        env=_stack_subprocess_env(),
        check=False,
        timeout=120,
    )


def auth_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def admin_headers() -> dict[str, str]:
    """Return Orchestra admin auth headers for the local self-host stack."""

    _unify_key, admin_key = resolve_local_stack_credentials()
    return auth_headers(admin_key)


def bootstrap_user_id(urls: LocalStackUrls, *, unify_key: str | None = None) -> str:
    """Return the bootstrap user's Orchestra id."""

    resolved_unify_key = unify_key or resolve_local_stack_credentials()[0]
    response = requests.get(
        f"{urls.orchestra_url}/user/basic-info",
        headers=auth_headers(resolved_unify_key),
        timeout=30,
    )
    if response.status_code != 200:
        raise AssertionError(
            f"bootstrap user lookup failed: {response.status_code} {response.text}",
        )
    user_id = response.json().get("user_id")
    if not user_id:
        raise AssertionError("bootstrap user lookup returned no user_id")
    return str(user_id)


def create_user(
    urls: LocalStackUrls,
    *,
    email: str,
    name: str = "Test",
) -> dict:
    """Create a disposable Orchestra user with an API key."""

    response = requests.post(
        f"{urls.orchestra_url}/admin/user",
        json={"email": email, "name": name},
        headers=admin_headers(),
        timeout=30,
    )
    if response.status_code != 200:
        raise AssertionError(
            f"create user failed: {response.status_code} {response.text}",
        )
    body = response.json()
    user_id = str(body["id"])
    api_key = body.get("api_key")
    if not api_key:
        detail_response = requests.get(
            f"{urls.orchestra_url}/admin/user/by-user-id",
            params={"user_id": user_id},
            headers=admin_headers(),
            timeout=30,
        )
        if detail_response.status_code != 200:
            raise AssertionError(
                "create user succeeded but API key lookup failed: "
                f"{detail_response.status_code} {detail_response.text}",
            )
        api_key = detail_response.json().get("api_key")
    if not api_key:
        raise AssertionError(f"API key not found for user {email}")
    return {
        "id": user_id,
        "email": email,
        "api_key": api_key,
        "headers": auth_headers(api_key),
    }


def add_org_member(urls: LocalStackUrls, org: dict, *, user_id: str) -> None:
    """Add an organization member and provision their workspace coordinator."""

    response = requests.post(
        f"{urls.orchestra_url}/organizations/{org['id']}/members",
        json={"user_id": user_id},
        headers=org["headers"],
        timeout=30,
    )
    if response.status_code != 201:
        raise AssertionError(
            f"add org member failed: {response.status_code} {response.text}",
        )


def list_org_assistants(urls: LocalStackUrls, org: dict) -> list[dict]:
    """List every assistant in an organization workspace."""

    response = requests.get(
        f"{urls.orchestra_url}/assistant",
        params={"list_all_org": "true"},
        headers=org["headers"],
        timeout=30,
    )
    if response.status_code != 200:
        raise AssertionError(
            f"list org assistants failed: {response.status_code} {response.text}",
        )
    body = response.json()
    return body.get("info", body)


def find_org_coordinator_for_user(
    assistants: list[dict],
    *,
    user_id: str,
) -> dict:
    """Return the org-scoped workspace coordinator owned by ``user_id``."""

    for assistant in assistants:
        if not assistant.get("is_coordinator"):
            continue
        if str(assistant.get("user_id")) == str(user_id):
            return assistant
    raise AssertionError(
        f"No org coordinator found for user {user_id} among {len(assistants)} assistants",
    )


def fetch_admin_assistant_record(urls: LocalStackUrls, assistant_id: int) -> dict:
    """Fetch one assistant record through the Orchestra admin API."""

    response = requests.get(
        f"{urls.orchestra_url}/admin/assistant",
        params={"agent_id": assistant_id},
        headers=admin_headers(),
        timeout=30,
    )
    if response.status_code != 200:
        raise AssertionError(
            f"fetch admin assistant failed: {response.status_code} {response.text}",
        )
    info = response.json()["info"]
    return info[0] if isinstance(info, list) else info


def credit_organization(urls: LocalStackUrls, *, organization_id: int) -> None:
    """Grant promo credits so org-scoped LLM and log writes can run locally."""

    response = requests.post(
        f"{urls.orchestra_url}/admin/create_recharge",
        headers=admin_headers(),
        json={
            "organization_id": organization_id,
            "quantity": 10,
            "type": "promo",
        },
        timeout=30,
    )
    if response.status_code >= 300:
        raise AssertionError(
            f"credit organization failed: {response.status_code} {response.text}",
        )


def create_organization(urls: LocalStackUrls, *, name: str) -> dict:
    unify_key, _admin_key = resolve_local_stack_credentials()
    response = requests.post(
        f"{urls.orchestra_url}/organizations",
        json={"name": name},
        headers=auth_headers(unify_key),
        timeout=30,
    )
    if response.status_code != 201:
        raise AssertionError(
            f"create organization failed: {response.status_code} {response.text}",
        )
    body = response.json()
    org_api_key = body["api_key"]
    return {
        "id": body["id"],
        "api_key": org_api_key,
        "headers": auth_headers(org_api_key),
    }


def delete_team(urls: LocalStackUrls, org: dict, team_id: int) -> None:
    requests.delete(
        f"{urls.orchestra_url}/organizations/{org['id']}/teams/{team_id}",
        headers=org["headers"],
        timeout=30,
    )


def delete_assistant(urls: LocalStackUrls, org: dict, assistant_id: int) -> None:
    requests.delete(
        f"{urls.orchestra_url}/assistant",
        params={"agent_id": assistant_id},
        headers=org["headers"],
        timeout=30,
    )


def unique_org_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"
