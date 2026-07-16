"""Live provider-event dispatch helpers for Unity."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import requests
from pydantic import BaseModel, ConfigDict, Field

from unify.session_details import SESSION_DETAILS
from unify.settings import SETTINGS

PROVIDER_EVENT_DISPATCH_AUDIENCE = "unity:provider-event-dispatch"
PublicDispatchStatus = Literal["adopted", "started", "terminal"]

_DISPATCH_CLAIM_PATH = "/provider-event-dispatch/claim"
_DISPATCH_REPORT_STARTED_PATH = "/provider-event-dispatch/report-started"
_DISPATCH_REPORT_TERMINAL_PATH = "/provider-event-dispatch/report-terminal"
_HTTP_TIMEOUT_SECONDS = 30
PROVIDER_EVENT_OPERATION_INFO_PREFIX = "provider_event_operation:"


class ProviderEventDispatchRequest(BaseModel):
    """Internal dispatch authorization for provider-event live execution."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["1"] = "1"
    operation_id: str
    run_id: int
    run_key: str
    assistant_id: str
    task_id: int
    binding_id: str
    receipt_id: str
    accepted_activation_revision: str
    source_type: Literal["provider_event"] = "provider_event"
    dispatch_mode: Literal["live", "offline"] = "live"
    event_context_ref: str
    issued_at: datetime
    audience: str = Field(default=PROVIDER_EVENT_DISPATCH_AUDIENCE)


class ProviderEventDispatchValidationError(ValueError):
    """Raised when one dispatch request fails authorization validation."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class ProviderEventDispatchAuthorizationError(ValueError):
    """Raised when Orchestra rejects a reused operation authorization snapshot."""

    def __init__(self, reason_code: str = "dispatch_authorization_mismatch") -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class LiveProviderEventDispatchOutcome:
    """Result of one live provider-event dispatch attempt."""

    operation_id: str
    run_id: int
    run_key: str
    captured_task_revision: int | None
    status: PublicDispatchStatus
    fencing_token: int
    adopted_only: bool
    launch_identity: str | None = None
    terminal_reason: str | None = None


def validate_provider_event_dispatch_request(
    request: ProviderEventDispatchRequest,
    *,
    ttl_seconds: int,
    now: datetime | None = None,
) -> None:
    """Validate audience, dispatch mode, and request freshness."""

    if request.audience != PROVIDER_EVENT_DISPATCH_AUDIENCE:
        raise ProviderEventDispatchValidationError("invalid_audience")
    if request.dispatch_mode != "live":
        raise ProviderEventDispatchValidationError("invalid_dispatch_mode")
    current_time = now or datetime.now(timezone.utc)
    issued_at = request.issued_at
    if issued_at.tzinfo is None:
        issued_at = issued_at.replace(tzinfo=timezone.utc)
    age_seconds = (current_time - issued_at.astimezone(timezone.utc)).total_seconds()
    if age_seconds < 0 or age_seconds > ttl_seconds:
        raise ProviderEventDispatchValidationError("dispatch_request_expired")


def live_launch_identity(*, operation_id: str) -> str:
    """Return the deterministic captured-instance identity for one operation."""

    return f"{PROVIDER_EVENT_OPERATION_INFO_PREFIX}{operation_id}"


def _orchestra_headers() -> dict[str, str]:
    unify_key = SESSION_DETAILS.unify_key
    if not unify_key:
        raise ProviderEventDispatchValidationError("orchestra_credentials_missing")
    return {
        "accept": "application/json",
        "Authorization": f"Bearer {unify_key}",
        "Content-Type": "application/json",
    }


def _orchestra_url(path: str) -> str:
    base = (SETTINGS.ORCHESTRA_URL or "").rstrip("/")
    if not base:
        raise ProviderEventDispatchValidationError("orchestra_url_missing")
    return f"{base}{path}"


def _orchestra_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        _orchestra_url(path),
        headers=_orchestra_headers(),
        json=payload,
        timeout=_HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code == 409:
        detail = response.json() if response.content else {}
        reason = "dispatch_authorization_mismatch"
        if isinstance(detail, dict):
            nested = detail.get("detail")
            if isinstance(nested, dict) and nested.get("reason"):
                reason = str(nested["reason"])
            elif detail.get("reason"):
                reason = str(detail["reason"])
        raise ProviderEventDispatchAuthorizationError(reason)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict):
        raise RuntimeError(f"Unexpected Orchestra response for {path}")
    return body


def _claimant_id() -> str:
    hostname = os.environ.get("HOSTNAME") or uuid.uuid4().hex[:8]
    return f"unity:{hostname}:{os.getpid()}"


def claim_provider_event_dispatch(
    request: ProviderEventDispatchRequest,
    *,
    launch_identity: str,
    claimant_id: str | None = None,
) -> LiveProviderEventDispatchOutcome:
    """Claim Orchestra launch ownership before live instance start I/O."""

    body = _orchestra_post(
        _DISPATCH_CLAIM_PATH,
        {
            "operation_id": request.operation_id,
            "run_id": request.run_id,
            "run_key": request.run_key,
            "assistant_id": request.assistant_id,
            "task_id": request.task_id,
            "binding_id": request.binding_id,
            "receipt_id": request.receipt_id,
            "accepted_activation_revision": request.accepted_activation_revision,
            "dispatch_mode": request.dispatch_mode,
            "audience": request.audience,
            "claimant_id": claimant_id or _claimant_id(),
            "launch_identity": launch_identity,
        },
    )
    return LiveProviderEventDispatchOutcome(
        operation_id=str(body["operation_id"]),
        run_id=int(body["run_id"]),
        run_key=str(body["run_key"]),
        captured_task_revision=None,
        status=str(body["status"]),  # type: ignore[arg-type]
        fencing_token=int(body["fencing_token"]),
        adopted_only=not bool(body.get("owns_launch")),
        launch_identity=body.get("launch_identity"),
        terminal_reason=body.get("terminal_reason"),
    )


def report_provider_event_dispatch_started(
    *,
    operation_id: str,
    fencing_token: int,
    launch_identity: str | None,
    captured_task_revision: int | None = None,
) -> LiveProviderEventDispatchOutcome:
    """Report fenced start after the captured-revision instance is reconciled."""

    body = _orchestra_post(
        _DISPATCH_REPORT_STARTED_PATH,
        {
            "operation_id": operation_id,
            "fencing_token": fencing_token,
            "launch_identity": launch_identity,
        },
    )
    return LiveProviderEventDispatchOutcome(
        operation_id=str(body["operation_id"]),
        run_id=int(body["run_id"]),
        run_key=str(body["run_key"]),
        captured_task_revision=captured_task_revision,
        status=str(body["status"]),  # type: ignore[arg-type]
        fencing_token=int(body["fencing_token"]),
        adopted_only=False,
        launch_identity=body.get("launch_identity"),
        terminal_reason=body.get("terminal_reason"),
    )


def report_provider_event_dispatch_terminal(
    *,
    operation_id: str,
    fencing_token: int,
    terminal_reason: str,
    launch_identity: str | None = None,
    captured_task_revision: int | None = None,
) -> LiveProviderEventDispatchOutcome:
    """Report fenced terminal failure for one live dispatch attempt."""

    body = _orchestra_post(
        _DISPATCH_REPORT_TERMINAL_PATH,
        {
            "operation_id": operation_id,
            "fencing_token": fencing_token,
            "terminal_reason": terminal_reason,
            "launch_identity": launch_identity,
        },
    )
    return LiveProviderEventDispatchOutcome(
        operation_id=str(body["operation_id"]),
        run_id=int(body["run_id"]),
        run_key=str(body["run_key"]),
        captured_task_revision=captured_task_revision,
        status=str(body["status"]),  # type: ignore[arg-type]
        fencing_token=int(body["fencing_token"]),
        adopted_only=False,
        launch_identity=body.get("launch_identity"),
        terminal_reason=body.get("terminal_reason"),
    )


__all__ = [
    "PROVIDER_EVENT_DISPATCH_AUDIENCE",
    "PROVIDER_EVENT_OPERATION_INFO_PREFIX",
    "LiveProviderEventDispatchOutcome",
    "ProviderEventDispatchAuthorizationError",
    "ProviderEventDispatchRequest",
    "ProviderEventDispatchValidationError",
    "PublicDispatchStatus",
    "claim_provider_event_dispatch",
    "live_launch_identity",
    "report_provider_event_dispatch_started",
    "report_provider_event_dispatch_terminal",
    "validate_provider_event_dispatch_request",
]
