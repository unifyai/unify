"""Tests for Unify Orchestra-transport integration retries."""

from __future__ import annotations

from unify.integrations.transient import (
    call_with_transport_retries,
    is_transient_transport_envelope,
)


def test_transport_envelope_classifier() -> None:
    assert is_transient_transport_envelope(
        {
            "status": "error",
            "error": {
                "code": "unify_integration_request_failed",
                "message": "HTTPSConnectionPool: Connection reset by peer",
            },
        },
    )
    assert not is_transient_transport_envelope(
        {
            "status": "error",
            "error": {
                "code": "unify_integration_request_failed",
                "message": "Invalid API key",
            },
        },
    )
    assert not is_transient_transport_envelope({"status": "ok", "result": {"data": 1}})
    assert not is_transient_transport_envelope(
        {
            "status": "provider_error",
            "error": {"code": "provider_error", "message": "rate limit"},
        },
    )


def test_transport_retries_then_succeeds() -> None:
    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        if calls["n"] < 2:
            return {
                "status": "error",
                "error": {
                    "code": "unify_integration_request_failed",
                    "message": "504 Gateway Timeout",
                },
            }
        return {"status": "ok", "result": {}}

    out = call_with_transport_retries(_fn, max_attempts=4, sleep=lambda _: None)
    assert out["status"] == "ok"
    assert calls["n"] == 2
