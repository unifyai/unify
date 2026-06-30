from __future__ import annotations

from unify.gateway.app import create_app
from unify.gateway.local_setup import (
    all_channel_setups,
    callback_urls,
    channel_names,
    env_placeholder_lines,
    public_url_provider_from_base,
)


def test_local_setup_declares_operator_channels() -> None:
    assert set(channel_names()) == {
        "local-stack",
        "twilio",
        "whatsapp",
        "social",
        "slack",
        "google",
        "microsoft",
        "discord",
        "email",
        "unillm",
        "voice",
        "internal",
    }


def test_callback_metadata_matches_mounted_gateway_routes() -> None:
    app = create_app()
    mounted_paths = {route.path for route in app.routes if hasattr(route, "path")}
    callback_paths = {
        callback.path for setup in all_channel_setups() for callback in setup.callbacks
    }
    assert callback_paths
    assert callback_paths <= mounted_paths


def test_public_https_channels_have_callback_metadata() -> None:
    for setup in all_channel_setups():
        if setup.public_https_required:
            assert setup.callbacks, setup.name


def test_all_gateway_surfaces_are_classified_for_local_setup() -> None:
    represented = {setup.name for setup in all_channel_setups()}
    assert {
        "twilio",
        "whatsapp",
        "social",
        "slack",
        "google",
        "microsoft",
        "discord",
        "email",
        "unillm",
        "internal",
    } <= represented
    assert {setup.kind for setup in all_channel_setups()} == {
        "channel",
        "capability",
        "internal",
    }


def test_callback_urls_use_selected_public_base() -> None:
    provider = public_url_provider_from_base("https://example.com")
    twilio = next(setup for setup in all_channel_setups() if setup.name == "twilio")
    urls = {callback.name: url for callback, url in callback_urls(twilio, provider)}
    assert urls["Inbound SMS webhook"] == "https://example.com/twilio/sms"
    assert urls["Call TwiML callback"] == "https://example.com/phone/twiml"


def test_env_placeholder_lines_do_not_duplicate_shared_credentials() -> None:
    lines = env_placeholder_lines(all_channel_setups())
    variable_lines = [line for line in lines if line and not line.startswith("#")]
    assert len(variable_lines) == len(set(variable_lines))
    assert "ORCHESTRA_ADMIN_KEY=" in variable_lines
    assert "TWILIO_AUTH_TOKEN=" in variable_lines
    assert "UNITY_GATEWAY_PUBLIC_URL=" in variable_lines
