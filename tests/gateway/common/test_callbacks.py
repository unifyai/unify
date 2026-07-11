"""Tests for ``unify.gateway.common.callbacks``.

Pin the environment-aware resolution of Twilio-facing callback URLs:
hosted deployments keep their public ``COMMS_URL``/``ADAPTERS_URL`` base,
while the self-host source stack rewrites callbacks onto the cloudflared
tunnel fronting the ConversationManager local ingress so Twilio can reach
them.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from unify.gateway.common import callbacks


def _patch_conversation(monkeypatch: pytest.MonkeyPatch, **fields: object) -> None:
    conversation = SimpleNamespace(
        LOCAL_COMMS_ENABLED=fields.get("LOCAL_COMMS_ENABLED", False),
        LOCAL_COMMS_MODE=fields.get("LOCAL_COMMS_MODE", "hosted"),
        LOCAL_COMMS_HOST=fields.get("LOCAL_COMMS_HOST", "127.0.0.1"),
        LOCAL_COMMS_PORT=fields.get("LOCAL_COMMS_PORT", 8787),
        LOCAL_COMMS_PUBLIC_URL=fields.get("LOCAL_COMMS_PUBLIC_URL", ""),
        # Empty file path skips filesystem lookup and uses LOCAL_COMMS_PUBLIC_URL.
        LOCAL_COMMS_PUBLIC_URL_FILE=fields.get("LOCAL_COMMS_PUBLIC_URL_FILE", ""),
    )
    monkeypatch.setattr(
        callbacks,
        "SETTINGS",
        SimpleNamespace(conversation=conversation),
    )


def test_hosted_mode_uses_hosted_base_and_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_conversation(monkeypatch)

    url = callbacks.twilio_callback_url(
        local_path="/local/twilio/whatsapp-call-status",
        hosted_base="https://adapters.example.com",
        hosted_path="/twilio/whatsapp-call-status",
    )

    assert url == "https://adapters.example.com/twilio/whatsapp-call-status"
    assert callbacks.use_local_comms() is False


@pytest.mark.parametrize(
    "fields",
    [
        {"LOCAL_COMMS_ENABLED": True},
        {"LOCAL_COMMS_MODE": "local"},
    ],
)
def test_local_mode_uses_tunnel_and_local_path(
    monkeypatch: pytest.MonkeyPatch,
    fields: dict,
) -> None:
    _patch_conversation(
        monkeypatch,
        LOCAL_COMMS_PUBLIC_URL="https://tunnel.trycloudflare.com",
        **fields,
    )

    url = callbacks.twilio_callback_url(
        local_path="/local/twilio/whatsapp-call-status",
        hosted_base="https://adapters.example.com",
        hosted_path="/twilio/whatsapp-call-status",
    )

    assert url == ("https://tunnel.trycloudflare.com/local/twilio/whatsapp-call-status")
    assert callbacks.use_local_comms() is True


def test_local_mode_falls_back_to_internal_ingress_without_tunnel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_conversation(
        monkeypatch,
        LOCAL_COMMS_MODE="local",
        LOCAL_COMMS_HOST="127.0.0.1",
        LOCAL_COMMS_PORT=8787,
    )

    url = callbacks.twilio_callback_url(
        local_path="/local/twilio/call-status",
        hosted_base="https://adapters.example.com",
        hosted_path="/twilio/call-status",
    )

    assert url == "http://127.0.0.1:8787/local/twilio/call-status"


def test_conference_wait_url_is_public_asset() -> None:
    assert callbacks.CONFERENCE_WAIT_URL.startswith("https://")
    assert "127.0.0.1" not in callbacks.CONFERENCE_WAIT_URL
