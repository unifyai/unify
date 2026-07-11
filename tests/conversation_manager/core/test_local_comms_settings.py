"""Contract tests for self-host callback and listener URL resolution."""

from unify.conversation_manager.settings import (
    ConversationSettings,
    local_comms_callback_base_url,
    local_comms_listener_url,
    local_comms_public_url,
)


def test_callback_url_tracks_runtime_tunnel_rotation(tmp_path) -> None:
    """A running CM reads each new quick-tunnel URL without restarting."""
    tunnel_url_file = tmp_path / "call-tunnel-url"
    settings = ConversationSettings(
        LOCAL_COMMS_PUBLIC_URL="https://fallback.example",
        LOCAL_COMMS_PUBLIC_URL_FILE=str(tunnel_url_file),
    )

    tunnel_url_file.write_text("https://first.trycloudflare.com/\n", encoding="utf-8")
    assert local_comms_public_url(settings) == "https://first.trycloudflare.com"

    tunnel_url_file.write_text("https://second.trycloudflare.com\n", encoding="utf-8")
    assert local_comms_callback_base_url(settings) == (
        "https://second.trycloudflare.com"
    )


def test_internal_listener_never_uses_public_tunnel(tmp_path) -> None:
    """Internal outbox and attachment traffic remains on the private listener."""
    tunnel_url_file = tmp_path / "call-tunnel-url"
    tunnel_url_file.write_text("https://public.trycloudflare.com\n", encoding="utf-8")
    settings = ConversationSettings(
        LOCAL_COMMS_HOST="unity-cm",
        LOCAL_COMMS_PORT=8787,
        LOCAL_COMMS_PUBLIC_URL_FILE=str(tunnel_url_file),
    )

    assert local_comms_listener_url(settings) == "http://unity-cm:8787"
    assert local_comms_callback_base_url(settings) == (
        "https://public.trycloudflare.com"
    )
