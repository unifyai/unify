"""Shared subprocess contract for ``unify.task_scheduler.offline_runner``.

This module is THE source of truth for the env-var shape and the run-key
shape that an offline-execution attempt uses. It is imported by:

- :mod:`unify.task_scheduler.local_scheduler.offline_dispatcher` — local
  in-process spawn (``asyncio.create_subprocess_exec``).
- :mod:`communication.infra.task_activation` — hosted cold session wake /
  warm in-pod spawn (env embedded in AssistantSession bootstrap or Pub/Sub).

Keep this module dependency-free (no Orchestra, no Communication, no
DB or HTTP imports) so it can be loaded from either side without
circular dependencies. The functions here describe what
``offline_runner._load_config_from_env`` expects to see in the
environment of the spawned process; they do NOT spawn anything.

The reason for the split is symmetry of failure modes: if the env-var
contract drifts between the local and hosted paths, the same task can
run differently depending on deployment topology. Centralising both
builders here eliminates that risk by construction.
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
    activation_revision: str,
    source_type: str,
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
    """Build the task-specific env-var dict for one offline_runner subprocess.

    Returns ONLY the variables that ``offline_runner._load_config_from_env``
    reads (the ``UNITY_OFFLINE_TASK_*`` family plus ``ASSISTANT_ID``).
    Assistant-identity vars (``ASSISTANT_FIRST_NAME``, ``USER_NUMBER``,
    ``VOICE_ID``, ``UNIFY_KEY``, ``ORCHESTRA_URL``, etc.) are NOT
    included here — callers supply them differently in the two
    deployment topologies:

    - Hosted cold wakes compose assistant-identity vars on top of this
      dict and embed them in the AssistantSession bootstrap secret.
    - Local / warm in-pod subprocesses inherit ``os.environ`` from the
      parent process and merge ``{**os.environ, **this_dict}``.

    All scalar parameters are converted to strings; missing optional
    parameters resolve to empty-string env vars so downstream parsing
    in ``_load_config_from_env`` sees the same shape regardless of
    which producer built the dict.
    """

    request_text = _request_text(
        task_description=task_description,
        task_name=task_name,
        task_id=task_id,
    )

    env: dict[str, str] = {
        "UNITY_OFFLINE_TASK_MODE": "actor",
        "UNITY_OFFLINE_TASK_RUN_KEY": run_key,
        "UNITY_OFFLINE_TASK_ID": str(task_id),
        "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID": str(source_task_log_id),
        "UNITY_OFFLINE_TASK_ACTIVATION_REVISION": str(activation_revision or ""),
        "UNITY_OFFLINE_TASK_FUNCTION_ID": (
            str(int(entrypoint)) if entrypoint is not None else ""
        ),
        "UNITY_OFFLINE_TASK_REQUEST": request_text,
        "UNITY_OFFLINE_TASK_NAME": str(task_name or ""),
        "UNITY_OFFLINE_TASK_DESCRIPTION": str(task_description or ""),
        "UNITY_OFFLINE_TASK_SOURCE_TYPE": source_type,
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
    if source_type == "provider_event":
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
    activation_revision: str,
    source_type: str,
    scheduled_for: str | datetime | None = None,
    source_contact_id: int | str | None = None,
    source_medium: str | None = None,
    source_ref: str | None = None,
) -> str:
    """Build the deterministic run-key shared across attempt retries.

    The key shape lets Orchestra's create-or-adopt task-run endpoint
    deduplicate concurrent execution attempts: any two attempts that
    agree on (assistant, task, source-type, activation revision,
    scheduled time, trigger provenance) resolve to the same row.

    Key shape (all components included in this exact order):

    ``offline:{source_type}:{assistant_id}:{task_id}:{revision_digest}:{tail}``

    Where:

    - ``revision_digest`` is the first 12 hex chars of SHA-256(activation_revision).
    - ``tail`` joins (with ``-``) whichever of the following are
      present, in this order:

      * ``YYYYMMDDTHHMMSSZ`` form of ``scheduled_for`` (UTC).
      * ``contact-{source_contact_id}`` (string).
      * Normalised first 24 chars of ``source_medium``.
      * First 12 hex chars of SHA-256(source_ref).

      If no tail parts are present, the tail is ``once`` so the key
      stays well-formed for non-recurring attempts.
    """

    revision_digest = hashlib.sha256(
        str(activation_revision or "").encode("utf-8"),
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
    return (
        f"offline:{source_type}:{assistant_id}:" f"{task_id}:{revision_digest}:{tail}"
    )


def build_provider_event_run_key(
    *,
    assistant_id: str,
    task_id: int,
    binding_id: str,
    activation_revision: str,
    event_identity_hmac: str,
    execution_mode: Literal["live", "offline"] = "offline",
) -> str:
    """Build the deterministic provider-event run key.

    Unlike communication-trigger keys, the provider event identity digest is
    included in full so two identities that share a 12-hex prefix cannot
    collide through truncation.
    """

    revision_digest = hashlib.sha256(
        str(activation_revision or "").encode("utf-8"),
    ).hexdigest()[:12]
    binding_part = normalize_run_key_component(binding_id)
    identity = str(event_identity_hmac).strip()
    if not identity:
        raise ValueError("event_identity_hmac is required")
    return (
        f"{execution_mode}:provider_event:{assistant_id}:{task_id}:"
        f"{binding_part}:{revision_digest}:{identity}"
    )


def normalize_run_key_component(value: str) -> str:
    """Normalise one free-form identifier into a run-key tail fragment.

    Lower-cases the input and replaces every run of non-(a-z, 0-9, ``-``)
    characters with a single dash, then strips leading / trailing dashes.
    Returns ``"assistant"`` for an empty result so the key segment is
    never empty.
    """

    normalised = _RUN_KEY_SAFE_RE.sub("-", value.lower()).strip("-")
    return normalised or "assistant"


# ---------------------------------------------------------------------------
# Helpers (private)
# ---------------------------------------------------------------------------


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
