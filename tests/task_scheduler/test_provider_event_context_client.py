"""Contract tests for the Unity provider-event context fetch client."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from unify.task_scheduler.provider_event_context import (
    PROVIDER_EVENT_CONTEXT_AUDIENCE,
    build_provider_event_context_request,
    fetch_provider_event_context,
)
from unify.task_scheduler.provider_event_dispatch import ProviderEventDispatchRequest


class _ContextHandler(BaseHTTPRequestHandler):
    captured_body: dict | None = None

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        _ContextHandler.captured_body = json.loads(raw.decode("utf-8"))
        response = {
            "receipt_id": _ContextHandler.captured_body["receipt_id"],
            "run_id": _ContextHandler.captured_body["run_id"],
            "event_context_ref": _ContextHandler.captured_body["event_context_ref"],
            "envelope": {"provider_trigger_slug": "GITHUB_ISSUE_CREATED_TRIGGER"},
            "curated_projection": {"title": "Issue"},
            "source_body": {"number": 1},
            "expires_at": datetime.now(timezone.utc).isoformat(),
        }
        payload = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


@pytest.fixture
def orchestra_context_url(monkeypatch: pytest.MonkeyPatch):
    server = HTTPServer(("127.0.0.1", 0), _ContextHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base_url = f"http://127.0.0.1:{port}/v0"
    monkeypatch.setattr(
        "unify.task_scheduler.provider_event_context.SETTINGS.ORCHESTRA_URL",
        base_url,
    )
    monkeypatch.setattr(
        "unify.task_scheduler.provider_event_context.SESSION_DETAILS.unify_key",
        "local-test-api-key",
    )
    yield base_url
    server.shutdown()


def test_fetch_provider_event_context_sends_fresh_issued_at(
    orchestra_context_url: str,
) -> None:
    del orchestra_context_url
    request = ProviderEventDispatchRequest(
        operation_id="op-ctx-1",
        run_id=42,
        run_key="run-key-42",
        assistant_id="7",
        task_id=99,
        binding_id="binding-1",
        receipt_id="receipt-1",
        accepted_activation_revision="rev-1",
        dispatch_mode="live",
        event_context_ref="blob-1",
        issued_at=datetime.now(timezone.utc),
        audience="unity:provider-event-dispatch",
    )

    before = datetime.now(timezone.utc)
    context = fetch_provider_event_context(request)
    after = datetime.now(timezone.utc)

    assert _ContextHandler.captured_body is not None
    assert _ContextHandler.captured_body["audience"] == PROVIDER_EVENT_CONTEXT_AUDIENCE
    assert _ContextHandler.captured_body["receipt_id"] == "receipt-1"
    expected = build_provider_event_context_request(request).model_dump(mode="json")
    assert _ContextHandler.captured_body["assistant_id"] == expected["assistant_id"]
    assert _ContextHandler.captured_body["task_id"] == expected["task_id"]
    assert _ContextHandler.captured_body["run_id"] == expected["run_id"]
    issued_at = datetime.fromisoformat(_ContextHandler.captured_body["issued_at"])
    if issued_at.tzinfo is None:
        issued_at = issued_at.replace(tzinfo=timezone.utc)
    assert before <= issued_at <= after + timedelta(seconds=5)
    assert context.source_body == {"number": 1}
