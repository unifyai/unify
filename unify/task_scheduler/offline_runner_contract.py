"""Shared subprocess contract for ``unify.task_scheduler.offline_runner``.

This module is THE source of truth for the env-var shape and the run-key
shape that an offline-execution attempt uses.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

__all__ = [
    "build_offline_runner_env",
    "build_offline_run_key",
    "normalize_run_key_component",
]


_PROVIDER_EVENT_OFFLINE_ENV_KEYS = (
    "UNIFY_OFFLINE_PROVIDER_EVENT_OPERATION_ID",
    "UNIFY_OFFLINE_PROVIDER_EVENT_RUN_ID",
    "UNIFY_OFFLINE_PROVIDER_EVENT_BINDING_ID",
    "UNIFY_OFFLINE_PROVIDER_EVENT_RECEIPT_ID",
    "UNIFY_OFFLINE_PROVIDER_EVENT_CONTEXT_REF",
    "UNIFY_OFFLINE_PROVIDER_EVENT_ISSUED_AT",
)


def build_offline_runner_env(
    *,
    assistant_id: str,
    task_id: int,
    source_task_log_id: int,
    revision: str,
    wake: str,
    run_key: str,
    task_name: str = "",
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
) -> dict[str, str]:
    """Build the task-specific env-var dict for one offline_runner subprocess."""

    request_text = _request_text(task_name=task_name, task_id=task_id)

    env: dict[str, str] = {
        "UNIFY_OFFLINE_RUN_KEY": run_key,
        "UNIFY_OFFLINE_TASK_ID": str(task_id),
        "UNIFY_OFFLINE_TASK_SOURCE_TASK_LOG_ID": str(source_task_log_id),
        "UNIFY_OFFLINE_TASK_REVISION": str(revision or ""),
        "UNIFY_OFFLINE_TASK_FUNCTION_ID": (
            str(int(entrypoint)) if entrypoint is not None else ""
        ),
        "UNIFY_OFFLINE_TASK_REQUEST": request_text,
        "UNIFY_OFFLINE_TASK_NAME": str(task_name or ""),
        "UNIFY_OFFLINE_TASK_WAKE": wake,
        "UNIFY_OFFLINE_TASK_SCHEDULED_FOR": _iso_utc_or_empty(scheduled_for),
        "UNIFY_OFFLINE_TASK_SOURCE_REF": source_ref or "",
        "UNIFY_OFFLINE_TASK_SOURCE_MEDIUM": source_medium or "",
        "UNIFY_OFFLINE_TASK_SOURCE_CONTACT_ID": (
            str(source_contact_id) if source_contact_id is not None else ""
        ),
        "UNIFY_OFFLINE_TASK_REQUIRES_FILESYSTEM": ("1" if requires_filesystem else "0"),
        "UNIFY_OFFLINE_TASK_REQUIRES_COMPUTER": "1" if requires_computer else "0",
        "ASSISTANT_ID": str(assistant_id),
    }
    if source_contact_display_name:
        env["UNIFY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME"] = str(
            source_contact_display_name,
        )
    if job_name:
        env["UNIFY_OFFLINE_TASK_JOB_NAME"] = str(job_name)
    if destination:
        env["TASK_DESTINATION"] = str(destination)
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


def normalize_run_key_component(value: str) -> str:
    """Normalise one free-form identifier into a run-key tail fragment."""

    normalised = _RUN_KEY_SAFE_RE.sub("-", value.lower()).strip("-")
    return normalised or "assistant"


def _request_text(*, task_name: str, task_id: int) -> str:
    """Label the run for display fallbacks; the definition holds the program.

    The scheduler-managed lane fetches the authored description itself, so
    this text is only ever a human-facing label, never the prompt.
    """

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
