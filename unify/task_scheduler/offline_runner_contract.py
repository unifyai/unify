"""Shared subprocess contract for ``unify.task_scheduler.offline_runner``.

This module is THE source of truth for the env-var shape and the run-key
shape that an offline-execution attempt uses.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Literal

__all__ = [
    "build_offline_runner_env",
    "build_offline_run_key",
    "build_provider_event_run_key",
    "normalize_run_key_component",
    "provider_event_offline_env_keys",
]


_PROVIDER_EVENT_OFFLINE_ENV_KEYS = (
    "UNITY_OFFLINE_PROVIDER_EVENT_OPERATION_ID",
    "UNITY_OFFLINE_PROVIDER_EVENT_RUN_ID",
    "UNITY_OFFLINE_PROVIDER_EVENT_BINDING_ID",
    "UNITY_OFFLINE_PROVIDER_EVENT_RECEIPT_ID",
    "UNITY_OFFLINE_PROVIDER_EVENT_CONTEXT_REF",
    "UNITY_OFFLINE_PROVIDER_EVENT_ISSUED_AT",
)


def provider_event_offline_env_keys() -> tuple[str, ...]:
    """Return env-var names required for one offline provider-event run."""

    return _PROVIDER_EVENT_OFFLINE_ENV_KEYS


_RUN_KEY_SAFE_RE = re.compile(r"[^a-z0-9-]+")


def build_offline_runner_env(
    *,
    assistant_id: str,
    task_id: int,
    source_task_log_id: int,
    revision: str,
    wake: str,
    run_key: str,
    task_name: str = "",
    task_description: str = "",
    scheduled_for: str | None = None,
    source_ref: str | None = None,
    source_medium: str | None = None,
    source_contact_id: int | str | None = None,
    source_contact_display_name: str | None = None,
    entrypoint: int | None = None,
    destination: str | None = None,
    job_name: str = "",
    requires_filesystem: bool = False,
    requires_computer: bool = False,
    provider_event_operation_id: str | None = None,
    provider_event_run_id: int | None = None,
    provider_event_binding_id: str | None = None,
    provider_event_receipt_id: str | None = None,
    provider_event_context_ref: str | None = None,
    provider_event_issued_at: str | None = None,
) -> dict[str, str]:
    """Build the task-specific env-var dict for one offline_runner subprocess."""

    request_text = _request_text(
        task_description=task_description,
        task_name=task_name,
        task_id=task_id,
    )

    env: dict[str, str] = {
        "UNITY_OFFLINE_TASK_MODE": "actor",
        "UNITY_OFFLINE_RUN_KEY": run_key,
        "UNITY_OFFLINE_TASK_ID": str(task_id),
        "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID": str(source_task_log_id),
        "UNITY_OFFLINE_TASK_REVISION": str(revision or ""),
        "UNITY_OFFLINE_TASK_FUNCTION_ID": (
            str(int(entrypoint)) if entrypoint is not None else ""
        ),
        "UNITY_OFFLINE_TASK_REQUEST": request_text,
        "UNITY_OFFLINE_TASK_NAME": str(task_name or ""),
        "UNITY_OFFLINE_TASK_DESCRIPTION": str(task_description or ""),
        "UNITY_OFFLINE_TASK_WAKE": wake,
        "UNITY_OFFLINE_TASK_SCHEDULED_FOR": _iso_utc_or_empty(scheduled_for),
        "UNITY_OFFLINE_TASK_SOURCE_REF": source_ref or "",
        "UNITY_OFFLINE_TASK_SOURCE_MEDIUM": source_medium or "",
        "UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID": (
            str(source_contact_id) if source_contact_id is not None else ""
        ),
        "UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM": ("1" if requires_filesystem else "0"),
        "UNITY_OFFLINE_TASK_REQUIRES_COMPUTER": "1" if requires_computer else "0",
        "ASSISTANT_ID": str(assistant_id),
    }
    if source_contact_display_name:
        env["UNITY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME"] = str(
            source_contact_display_name,
        )
    if job_name:
        env["UNITY_OFFLINE_TASK_JOB_NAME"] = str(job_name)
    if destination:
        env["TASK_DESTINATION"] = str(destination)
    if wake == "provider_event":
        if not all(
            (
                provider_event_operation_id,
                provider_event_run_id is not None,
                provider_event_binding_id,
                provider_event_receipt_id,
                provider_event_context_ref,
                provider_event_issued_at,
            ),
        ):
            raise ValueError(
                "provider_event offline runs require operation_id, run_id, "
                "binding_id, receipt_id, event_context_ref, and issued_at",
            )
        env.update(
            {
                "UNITY_OFFLINE_PROVIDER_EVENT_OPERATION_ID": str(
                    provider_event_operation_id,
                ),
                "UNITY_OFFLINE_PROVIDER_EVENT_RUN_ID": str(provider_event_run_id),
                "UNITY_OFFLINE_PROVIDER_EVENT_BINDING_ID": str(
                    provider_event_binding_id,
                ),
                "UNITY_OFFLINE_PROVIDER_EVENT_RECEIPT_ID": str(
                    provider_event_receipt_id,
                ),
                "UNITY_OFFLINE_PROVIDER_EVENT_CONTEXT_REF": str(
                    provider_event_context_ref,
                ),
                "UNITY_OFFLINE_PROVIDER_EVENT_ISSUED_AT": str(
                    provider_event_issued_at,
                ),
            },
        )
    return env


def build_offline_run_key(
    *,
    assistant_id: str,
    task_id: int,
    revision: str,
    wake: str,
    scheduled_for: str | datetime | None = None,
    source_contact_id: int | str | None = None,
    source_medium: str | None = None,
    source_ref: str | None = None,
) -> str:
    """Build the deterministic run-key shared across attempt retries."""

    revision_digest = hashlib.sha256(
        str(revision or "").encode("utf-8"),
    ).hexdigest()[:12]
    tail_parts: list[str] = []
    scheduled_iso_fragment = _scheduled_for_fragment(scheduled_for)
    if scheduled_iso_fragment:
        tail_parts.append(scheduled_iso_fragment)
    if source_contact_id is not None:
        tail_parts.append(f"contact-{source_contact_id}")
    if source_medium:
        tail_parts.append(normalize_run_key_component(source_medium)[:24])
    if source_ref:
        tail_parts.append(
            hashlib.sha256(str(source_ref).encode("utf-8")).hexdigest()[:12],
        )
    tail = "-".join(tail_parts) or "once"
    return f"offline:{wake}:{assistant_id}:" f"{task_id}:{revision_digest}:{tail}"


def build_provider_event_run_key(
    *,
    assistant_id: str,
    task_id: int,
    binding_id: str,
    revision: str,
    event_identity_hmac: str,
    delivery: Literal["live", "offline"] = "offline",
) -> str:
    """Build the deterministic provider-event run key."""

    revision_digest = hashlib.sha256(
        str(revision or "").encode("utf-8"),
    ).hexdigest()[:12]
    binding_part = normalize_run_key_component(binding_id)
    identity = str(event_identity_hmac).strip()
    if not identity:
        raise ValueError("event_identity_hmac is required")
    return (
        f"{delivery}:provider_event:{assistant_id}:{task_id}:"
        f"{binding_part}:{revision_digest}:{identity}"
    )


def normalize_run_key_component(value: str) -> str:
    """Normalise one free-form identifier into a run-key tail fragment."""

    normalised = _RUN_KEY_SAFE_RE.sub("-", value.lower()).strip("-")
    return normalised or "assistant"


def _request_text(*, task_description: str, task_name: str, task_id: int) -> str:
    """Pick the most descriptive text to hand offline_runner as the prompt."""

    cleaned_description = (task_description or "").strip()
    if cleaned_description:
        return cleaned_description
    cleaned_name = (task_name or "").strip()
    if cleaned_name:
        return cleaned_name
    return f"Execute task {task_id}"


def _iso_utc_or_empty(value: str | datetime | None) -> str:
    """Normalise a timestamp value to canonical UTC ISO-8601, or empty string."""

    if value is None:
        return ""
    if isinstance(value, datetime):
        parsed: datetime = value
    else:
        text = str(value)
        if not text:
            return ""
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _scheduled_for_fragment(value: str | datetime | None) -> str | None:
    """Compact ``YYYYMMDDTHHMMSSZ`` form of one scheduled-for timestamp."""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed: datetime = value
    else:
        text = str(value)
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
