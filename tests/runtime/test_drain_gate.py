"""Unit tests for in-pod drain admission gate."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unify.runtime import drain_gate


@pytest.fixture(autouse=True)
def _reset_cache():
    with drain_gate._lock:
        drain_gate._cached_at = 0.0
        drain_gate._cached_blocked = False
        drain_gate._cached_detail = None
    yield
    with drain_gate._lock:
        drain_gate._cached_at = 0.0
        drain_gate._cached_blocked = False
        drain_gate._cached_detail = None


def test_is_admission_blocked_when_comms_reports_draining(monkeypatch):
    monkeypatch.setenv("UNITY_COMMS_URL", "https://comms.example")
    monkeypatch.setenv("ASSISTANT_ID", "1406")
    monkeypatch.setenv("UNIFY_KEY", "test-key")

    response = MagicMock()
    response.status_code = 200
    response.content = b'{"draining": true}'
    response.json.return_value = {"draining": True}
    response.raise_for_status = MagicMock()

    with patch.object(drain_gate.requests, "get", return_value=response) as get:
        assert drain_gate.is_admission_blocked(force_refresh=True) is True
        get.assert_called_once()


def test_refuse_if_draining_raises(monkeypatch):
    monkeypatch.setenv("UNITY_COMMS_URL", "https://comms.example")
    monkeypatch.setenv("ASSISTANT_ID", "1406")
    monkeypatch.setenv("UNIFY_KEY", "test-key")

    response = MagicMock()
    response.status_code = 200
    response.content = b'{"draining": true}'
    response.json.return_value = {"draining": True}
    response.raise_for_status = MagicMock()

    with patch.object(drain_gate.requests, "get", return_value=response):
        with pytest.raises(drain_gate.DrainInProgressError):
            drain_gate.refuse_if_draining()


def test_not_blocked_when_draining_false(monkeypatch):
    monkeypatch.setenv("UNITY_COMMS_URL", "https://comms.example")
    monkeypatch.setenv("ASSISTANT_ID", "1406")
    monkeypatch.setenv("UNIFY_KEY", "test-key")

    response = MagicMock()
    response.status_code = 200
    response.content = b'{"draining": false}'
    response.json.return_value = {"draining": False}
    response.raise_for_status = MagicMock()

    with patch.object(drain_gate.requests, "get", return_value=response):
        assert drain_gate.is_admission_blocked(force_refresh=True) is False
        drain_gate.refuse_if_draining()  # no raise
