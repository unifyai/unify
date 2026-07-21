"""Provider-event dispatch envelope contract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from unify.task_scheduler.provider_event_dispatch import ProviderEventDispatchRequest

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1] / "fixtures" / "task_trigger_contract"
)


def test_provider_event_dispatch_request_v1_forbids_extra_and_rejects_raw_payload() -> (
    None
):
    payload = json.loads(
        (_FIXTURE_DIR / "provider_event_dispatch_request.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    request = ProviderEventDispatchRequest.model_validate(payload)
    assert request.contract_version == "1"
    assert request.event_context_ref == "blob://binding-a/receipt-b"
    assert "raw_body" not in request.model_dump()

    with pytest.raises(ValueError):
        ProviderEventDispatchRequest.model_validate({**payload, "raw_body": "secret"})


def test_provider_event_dispatch_request_matches_shared_fixture_fields() -> None:
    payload = json.loads(
        (_FIXTURE_DIR / "provider_event_dispatch_request.v1.json").read_text(
            encoding="utf-8",
        ),
    )
    request = ProviderEventDispatchRequest.model_validate(payload)
    assert request.operation_id == payload["operation_id"]
    assert request.binding_id == payload["binding_id"]
    assert request.wake == "provider_event"
