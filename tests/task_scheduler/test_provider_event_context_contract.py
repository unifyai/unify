"""Provider-event context request/response contract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from unify.task_scheduler.provider_event_context import (
    PROVIDER_EVENT_CONTEXT_AUDIENCE,
    ProviderEventContext,
    ProviderEventContextRequest,
)

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def test_provider_event_context_request_v1_forbids_extra_fields() -> None:
    payload = json.loads(
        (_FIXTURE_DIR / "provider_event_context_request.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    request = ProviderEventContextRequest.model_validate(payload)
    assert request.audience == PROVIDER_EVENT_CONTEXT_AUDIENCE
    assert request.event_context_ref == payload["event_context_ref"]

    with pytest.raises(ValueError):
        ProviderEventContextRequest.model_validate({**payload, "raw_body": "secret"})


def test_provider_event_context_response_v1_matches_shared_fixture() -> None:
    payload = json.loads(
        (_FIXTURE_DIR / "provider_event_context_response.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    response = ProviderEventContext.model_validate(payload)
    assert response.receipt_id == payload["receipt_id"]
    assert response.source_body == payload["source_body"]
    assert response.expires_at is not None
