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


def orchestra_api_base() -> str:
    raw = os.getenv("UNIFY_BASE_URL", "http://localhost:8000/v0").rstrip("/")
    if raw.endswith("/v0"):
        return raw[: -len("/v0")]
    return raw


def orchestra_api_key() -> str:
    return os.getenv("UNIFY_KEY", "local-test-api-key")


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


def run_orchestra_trigger_worker_cycle() -> None:
    """Advance Orchestra trigger reconciliation/dispatch using the local worker."""

    python_bin = _ORCHESTRA_ROOT / ".venv/bin/python"
    if not python_bin.exists():
        raise RuntimeError(f"Orchestra venv not found at {python_bin}")
    env = os.environ.copy()
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


def create_github_composio_connection(*, assistant_id: int) -> dict[str, Any]:
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
            "provider_connection_id": "ca_local_stub",
            "provider_user_id": "assistant:provider-trigger-probe",
            "granted_scopes": [],
            "status": "connected",
        },
        timeout=30,
    )
    complete.raise_for_status()
    return complete.json()
