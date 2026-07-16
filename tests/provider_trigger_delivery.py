"""Deliver signed Composio-shaped webhooks to a running Orchestra ingress."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

import requests

_ORCHESTRA_ROOT = Path(
    os.getenv(
        "ORCHESTRA_REPO_ROOT",
        str(Path(__file__).resolve().parents[2] / "orchestra"),
    ),
)
_FIXTURE_PATH = (
    _ORCHESTRA_ROOT
    / "orchestra"
    / "tests"
    / "fixtures"
    / "provider_trigger_contract"
    / "composio_github_issue_created.redacted.json"
)
_PIPEDREAM_FIXTURE_PATH = (
    _ORCHESTRA_ROOT
    / "orchestra"
    / "tests"
    / "fixtures"
    / "provider_trigger_contract"
    / "pipedream_github_issue.redacted.json"
)

_DEFAULT_PROBE_REPO_OWNER = "YushaArif99"
_DEFAULT_PROBE_REPO_NAME = "triggers-test-repo"


def probe_github_repo_owner() -> str:
    """GitHub owner for provider-trigger probe tasks and webhook fixtures."""

    return os.getenv("COMPOSIO_PROBE_REPO_OWNER", _DEFAULT_PROBE_REPO_OWNER).strip()


def probe_github_repo_name() -> str:
    """GitHub repository name for provider-trigger probe tasks and fixtures."""

    return os.getenv("COMPOSIO_PROBE_REPO_NAME", _DEFAULT_PROBE_REPO_NAME).strip()


def probe_github_repository_full_name() -> str:
    """Return owner/repo for the disposable provider-trigger probe repository."""

    return f"{probe_github_repo_owner()}/{probe_github_repo_name()}"


def probe_github_trigger_config() -> dict[str, str]:
    """Composio issue-created trigger config for the probe repository."""

    return {
        "owner": probe_github_repo_owner(),
        "repo": probe_github_repo_name(),
    }


def orchestra_api_base() -> str:
    raw = os.getenv("UNIFY_BASE_URL", "http://localhost:8000/v0").rstrip("/")
    if raw.endswith("/v0"):
        return raw[: -len("/v0")]
    return raw


def orchestra_api_key() -> str:
    return os.getenv("UNIFY_KEY", "local-test-api-key")


def _orchestra_db_container() -> str:
    return os.getenv("ORCHESTRA_DB_CONTAINER", "orchestra-local-db")


def ensure_pipedream_integration_backend_enabled() -> None:
    """Enable the Pipedream integration backend row for local actor E2E runs."""

    subprocess.check_output(
        [
            "docker",
            "exec",
            _orchestra_db_container(),
            "psql",
            "-U",
            "orchestra",
            "-d",
            "orchestra",
            "-c",
            "UPDATE integration_backends "
            "SET status = 'enabled' "
            "WHERE backend_id = 'pipedream';",
        ],
        text=True,
    )


def sign_composio_payload(
    raw_body: bytes,
    *,
    signing_secret: str,
    webhook_id: str,
) -> dict[str, str]:
    # ponytail: mirror orchestra/tests/provider_triggers/composio_delivery.py;
    # parity guarded by orchestra/tests/provider_triggers/test_composio_delivery_signing.py
    timestamp = str(int(time.time()))
    digest = base64.b64encode(
        hmac.new(
            signing_secret.encode("utf-8"),
            f"{webhook_id}.{timestamp}.{raw_body.decode('utf-8')}".encode("utf-8"),
            hashlib.sha256,
        ).digest(),
    ).decode("utf-8")
    return {
        "webhook-id": webhook_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": f"v1,{digest}",
    }


def sign_pipedream_payload(
    raw_body: bytes,
    *,
    signing_secret: str,
    timestamp: str | None = None,
) -> dict[str, str]:
    webhook_timestamp = timestamp or str(int(time.time()))
    signed_payload = f"{webhook_timestamp}.".encode("utf-8") + raw_body
    digest = hmac.new(
        signing_secret.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()
    return {"x-pd-signature": f"t={webhook_timestamp},v1={digest}"}


def load_pipedream_github_issue_fixture(**overrides: Any) -> dict[str, Any]:
    if not _PIPEDREAM_FIXTURE_PATH.is_file():
        raise RuntimeError(
            f"Orchestra Pipedream fixture missing at {_PIPEDREAM_FIXTURE_PATH}; "
            "set ORCHESTRA_REPO_ROOT to a checkout that includes orchestra/tests/fixtures/",
        )
    payload = json.loads(_PIPEDREAM_FIXTURE_PATH.read_text(encoding="utf-8"))
    if "action" in overrides and overrides["action"] is not None:
        payload["action"] = overrides["action"]
    if "trace_id" in overrides and overrides["trace_id"] is not None:
        payload["trace_id"] = overrides["trace_id"]
    if "repository" in overrides and overrides["repository"] is not None:
        repo = dict(payload.get("repository") or {})
        repo["full_name"] = overrides["repository"]
        payload["repository"] = repo
    return payload


def deliver_signed_pipedream_webhook(
    *,
    ingress_key: str,
    payload: Mapping[str, Any],
    signing_secret: str,
) -> requests.Response:
    raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return requests.post(
        f"{orchestra_api_base()}/v0/webhooks/integrations/pipedream/{ingress_key}",
        data=raw_body,
        headers=sign_pipedream_payload(raw_body, signing_secret=signing_secret),
        timeout=30,
    )


def _orchestra_worker_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in (
        "COMPOSIO_API_KEY",
        "PIPEDREAM_CLIENT_ID",
        "PIPEDREAM_CLIENT_SECRET",
        "PIPEDREAM_PROJECT_ID",
    ):
        env.pop(key, None)
    env.setdefault("ORCHESTRA_DB_USER", "orchestra")
    env.setdefault("ORCHESTRA_DB_PASS", "orchestra")
    env.setdefault("ORCHESTRA_DB_BASE", "orchestra")
    env.setdefault("ORCHESTRA_DB_HOST", "localhost")
    env.setdefault("ORCHESTRA_DB_PORT", "5432")
    env.setdefault("SELF_HOST", "1")
    env.setdefault("COMPOSIO_WEBHOOK_SECRET", "test-composio-webhook-secret")
    env.setdefault("TRIGGER_EVENT_WRAPPING_MASTER_KEY", "test-master-key-material")
    env.setdefault(
        "TRIGGER_EVENT_PRIVATE_ROOT",
        str(_ORCHESTRA_ROOT / ".local" / "provider-event-blobs"),
    )
    env.setdefault(
        "ORCHESTRA_TRIGGER_CALLBACK_BASE_URL",
        "https://orchestra.example",
    )
    return env


def resolve_orchestra_signing_secret(secret_ref: str) -> str:
    """Resolve one Orchestra signing-secret reference to raw material."""

    python_bin = _ORCHESTRA_ROOT / ".venv/bin/python"
    if not python_bin.exists():
        raise RuntimeError(f"Orchestra venv not found at {python_bin}")
    output = subprocess.check_output(
        [
            str(python_bin),
            "-c",
            (
                "from orchestra.provider_triggers.signing_secret_refs import "
                "resolve_signing_secret_ref; "
                f"print(resolve_signing_secret_ref({secret_ref!r}) or '')"
            ),
        ],
        cwd=_ORCHESTRA_ROOT,
        env=_orchestra_worker_env(),
        text=True,
    ).strip()
    if not output:
        raise RuntimeError(f"could not resolve signing secret ref {secret_ref!r}")
    return output


def fetch_active_generation_signing_secret(*, binding_id: str) -> dict[str, str]:
    """Read active generation routing and signing material from Orchestra."""

    container = os.getenv("ORCHESTRA_DB_CONTAINER", "orchestra-local-db")
    sql = (
        "SELECT ingress_key, external_trigger_id, signing_secret_ref "
        "FROM event_trigger_subscription_generations "
        f"WHERE binding_id = '{binding_id}' "
        "AND lifecycle_state = 'active' "
        "ORDER BY id DESC "
        "LIMIT 1;"
    )
    output = subprocess.check_output(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "orchestra",
            "-d",
            "orchestra",
            "-tAc",
            sql,
        ],
        text=True,
    ).strip()
    if not output or output.count("|") < 2:
        raise RuntimeError(f"active generation not found for binding {binding_id}")
    ingress_key, external_trigger_id, signing_secret_ref = output.split("|", 2)
    signing_secret = resolve_orchestra_signing_secret(signing_secret_ref.strip())
    return {
        "ingress_key": ingress_key.strip(),
        "external_trigger_id": external_trigger_id.strip(),
        "signing_secret": signing_secret,
    }


def load_composio_github_issue_fixture(**overrides: Any) -> dict[str, Any]:
    if not _FIXTURE_PATH.is_file():
        raise RuntimeError(
            f"Orchestra Composio fixture missing at {_FIXTURE_PATH}; "
            "set ORCHESTRA_REPO_ROOT to a checkout that includes orchestra/tests/fixtures/",
        )
    payload = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    metadata = dict(payload.get("metadata") or {})
    for key in ("external_trigger_id", "connected_account_id", "provider_user_id"):
        if key in overrides and overrides[key] is not None:
            mapped = {
                "external_trigger_id": "trigger_id",
                "connected_account_id": "connected_account_id",
                "provider_user_id": "user_id",
            }[key]
            metadata[mapped] = overrides[key]
    payload["metadata"] = metadata
    if "repository" in overrides and overrides["repository"] is not None:
        data = dict(payload.get("data") or {})
        repo = dict(data.get("repository") or {})
        repo["full_name"] = overrides["repository"]
        data["repository"] = repo
        payload["data"] = data
    return payload


def deliver_signed_composio_webhook(
    *,
    ingress_key: str,
    payload: Mapping[str, Any],
    signing_secret: str,
    webhook_id: str,
) -> requests.Response:
    raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return requests.post(
        f"{orchestra_api_base()}/v0/webhooks/integrations/composio/{ingress_key}",
        data=raw_body,
        headers=sign_composio_payload(
            raw_body,
            signing_secret=signing_secret,
            webhook_id=webhook_id,
        ),
        timeout=30,
    )


def run_orchestra_trigger_worker_cycle(
    *,
    use_live_provider_credentials: bool = False,
) -> None:
    """Advance Orchestra trigger reconciliation/dispatch using the local worker."""

    python_bin = _ORCHESTRA_ROOT / ".venv/bin/python"
    if not python_bin.exists():
        raise RuntimeError(f"Orchestra venv not found at {python_bin}")
    env = _orchestra_worker_env()
    if use_live_provider_credentials:
        for key in (
            "COMPOSIO_API_KEY",
            "PIPEDREAM_CLIENT_ID",
            "PIPEDREAM_CLIENT_SECRET",
            "PIPEDREAM_PROJECT_ID",
        ):
            if key in os.environ:
                env[key] = os.environ[key]
    env["PROVIDER_TRIGGER_WORKER_READINESS"] = "0"
    Path(env["TRIGGER_EVENT_PRIVATE_ROOT"]).mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            str(python_bin),
            "-m",
            "orchestra.workers.provider_trigger_worker",
            "--once",
        ],
        cwd=_ORCHESTRA_ROOT,
        check=True,
        env=env,
    )


def fetch_active_generation_for_binding(*, binding_id: str) -> dict[str, str]:
    """Read the active generation ingress key from the local Orchestra database."""

    container = os.getenv("ORCHESTRA_DB_CONTAINER", "orchestra-local-db")
    sql = (
        "SELECT ingress_key, external_trigger_id "
        "FROM event_trigger_subscription_generations "
        f"WHERE binding_id = '{binding_id}' "
        "AND lifecycle_state = 'active' "
        "ORDER BY id DESC "
        "LIMIT 1;"
    )
    output = subprocess.check_output(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "orchestra",
            "-d",
            "orchestra",
            "-tAc",
            sql,
        ],
        text=True,
    ).strip()
    if not output or "|" not in output:
        raise RuntimeError(f"active generation not found for binding {binding_id}")
    ingress_key, external_trigger_id = output.split("|", 1)
    return {
        "ingress_key": ingress_key.strip(),
        "external_trigger_id": external_trigger_id.strip(),
    }


def fetch_latest_receipt_run_key(*, binding_id: str) -> str:
    """Read the newest receipt run_key for one binding from Orchestra."""

    container = os.getenv("ORCHESTRA_DB_CONTAINER", "orchestra-local-db")
    sql = (
        "SELECT run_key "
        "FROM provider_event_receipts "
        f"WHERE binding_id = '{binding_id}' "
        "ORDER BY id DESC "
        "LIMIT 1;"
    )
    output = subprocess.check_output(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "orchestra",
            "-d",
            "orchestra",
            "-tAc",
            sql,
        ],
        text=True,
    ).strip()
    if not output:
        raise RuntimeError(f"receipt run_key not found for binding {binding_id}")
    return output


def fetch_orchestra_task_by_name_fragment(
    *,
    assistant_id: int,
    name_fragment: str,
) -> dict[str, Any]:
    """Return the single Orchestra task whose name contains ``name_fragment``."""

    headers = {"Authorization": f"Bearer {orchestra_api_key()}"}
    response = requests.get(
        f"{orchestra_api_base()}/v0/assistants/{assistant_id}/tasks",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    tasks = response.json()["info"]["tasks"]
    needle = name_fragment.lower()
    matches = [
        task
        for task in tasks
        if needle in str(task.get("name") or "").lower()
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one Orchestra task matching {name_fragment!r}, "
            f"found {len(matches)}",
        )
    return matches[0]


def create_github_composio_connection(
    *,
    assistant_id: int,
    provider_connection_id: str = "ca_local_stub",
    provider_user_id: str = "assistant:provider-trigger-probe",
) -> dict[str, Any]:
    """Start and complete one assistant-scoped Composio GitHub connection."""

    api_key = orchestra_api_key()
    base = orchestra_api_base()
    headers = {"Authorization": f"Bearer {api_key}"}
    start = requests.post(
        f"{base}/v0/integrations/connect/start",
        headers=headers,
        json={
            "owner_scope": "assistant",
            "assistant_id": assistant_id,
            "canonical_app_slug": "github",
            "backend_id": "composio",
            "provider_app_id": "GITHUB",
            "requested_scopes": [],
            "auth_mode": "oauth",
        },
        timeout=30,
    )
    start.raise_for_status()
    connection = start.json()["connection"]
    connection_id = connection["connection_id"]
    complete = requests.post(
        f"{base}/v0/integrations/connections/{connection_id}/complete",
        headers=headers,
        json={
            "provider_connection_id": provider_connection_id,
            "provider_user_id": provider_user_id,
            "granted_scopes": [],
            "status": "connected",
        },
        timeout=30,
    )
    complete.raise_for_status()
    return complete.json()


def create_github_pipedream_connection(*, assistant_id: int) -> dict[str, Any]:
    """Start and complete one assistant-scoped Pipedream GitHub connection."""

    api_key = orchestra_api_key()
    base = orchestra_api_base()
    headers = {"Authorization": f"Bearer {api_key}"}
    start = requests.post(
        f"{base}/v0/integrations/connect/start",
        headers=headers,
        json={
            "owner_scope": "assistant",
            "assistant_id": assistant_id,
            "canonical_app_slug": "github",
            "backend_id": "pipedream",
            "provider_app_id": "github",
            "requested_scopes": [],
            "auth_mode": "oauth",
        },
        timeout=30,
    )
    start.raise_for_status()
    connection = start.json()["connection"]
    connection_id = connection["connection_id"]
    complete = requests.post(
        f"{base}/v0/integrations/connections/{connection_id}/complete",
        headers=headers,
        json={
            "provider_connection_id": "apn_local_stub",
            "provider_user_id": "assistant:provider-trigger-probe",
            "granted_scopes": [],
            "status": "connected",
        },
        timeout=30,
    )
    complete.raise_for_status()
    return complete.json()
