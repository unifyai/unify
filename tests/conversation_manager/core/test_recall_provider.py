"""Contract tests for the Recall meeting backend."""

from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import jwt
import pytest

from unify.conversation_manager.domains.recall.client import (
    RecallBotState,
    RecallError,
    RecallParticipant,
)
from unify.conversation_manager.domains.recall.provider import (
    _BRIDGE_TOKEN_TTL,
    RecallMeetProvider,
    _default_relay_url,
)
from unify.session_details import SESSION_DETAILS

LIVEKIT_ENV = {
    "LIVEKIT_URL": "wss://livekit.example.com",
    "LIVEKIT_API_KEY": "lk-key",  # pragma: allowlist secret
    # Long enough that PyJWT does not warn about a short HMAC key.
    "LIVEKIT_API_SECRET": "lk-secret-that-is-long-enough-for-hmac-sha256",  # pragma: allowlist secret
}
BRIDGE = "https://comms.example.com/meet/bridge"


def _provider(**kwargs) -> RecallMeetProvider:
    client = MagicMock()
    client.create_bot = AsyncMock(
        return_value=RecallBotState(bot_id="bot_1", status="joining_call"),
    )
    client.get_bot = AsyncMock()
    client.leave_call = AsyncMock()
    client.send_chat_message = AsyncMock()
    kwargs.setdefault("bridge_page_url", BRIDGE)
    kwargs.setdefault("relay_url", "")
    provider = RecallMeetProvider(client=client, **kwargs)
    return provider


# ---------------------------------------------------------------------------
# Bridge URL and token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_hands_the_bot_a_room_scoped_token() -> None:
    """The token is the bot's only credential, so it must grant only this room.

    A room-wide or admin grant leaking into Recall's browser would be a much
    larger blast radius than one meeting.
    """
    provider = _provider()
    with patch.dict("os.environ", LIVEKIT_ENV, clear=False):
        result = await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    assert result.ok is True
    assert result.session_id == "bot_1"

    bridge_url = provider._client.create_bot.call_args.kwargs["bridge_page_url"]
    query = parse_qs(urlparse(bridge_url).query)
    assert query["url"] == ["wss://livekit.example.com"]

    claims = jwt.decode(
        query["token"][0],
        LIVEKIT_ENV["LIVEKIT_API_SECRET"],
        algorithms=["HS256"],
    )
    grants = claims["video"]
    assert grants["room"] == "unity_25_gmeet"
    assert grants["roomJoin"] is True
    # Audio both ways, and nothing else.
    assert grants.get("canPublish") is True
    assert grants.get("canSubscribe") is True
    assert grants.get("canPublishData") in (False, None)
    assert grants.get("roomAdmin") in (False, None)


@pytest.mark.asyncio
async def test_bridge_token_expires() -> None:
    """A leaked bridge URL must not stay usable indefinitely."""
    provider = _provider()
    with patch.dict("os.environ", LIVEKIT_ENV, clear=False):
        await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    bridge_url = provider._client.create_bot.call_args.kwargs["bridge_page_url"]
    token = parse_qs(urlparse(bridge_url).query)["token"][0]
    claims = jwt.decode(
        token,
        LIVEKIT_ENV["LIVEKIT_API_SECRET"],
        algorithms=["HS256"],
    )
    # LiveKit stamps nbf/exp and no iat, so the lifetime is the gap between
    # those two. Asserting the span rather than merely that exp exists is what
    # catches a dropped with_ttl(), which would fall back to LiveKit's default.
    assert claims["exp"] - claims["nbf"] == int(_BRIDGE_TOKEN_TTL.total_seconds())


@pytest.mark.asyncio
async def test_join_refuses_without_livekit_credentials() -> None:
    """Better a named failure than a bot joining a meeting deaf and mute."""
    provider = _provider()
    with patch.dict(
        "os.environ",
        {"LIVEKIT_URL": "", "LIVEKIT_API_KEY": "", "LIVEKIT_API_SECRET": ""},
        clear=False,
    ):
        result = await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    assert result.ok is False
    assert result.failure_reason == "livekit_unconfigured"
    provider._client.create_bot.assert_not_awaited()


@pytest.mark.asyncio
async def test_join_refuses_without_a_bridge_page() -> None:
    provider = _provider(bridge_page_url="")
    with patch.dict("os.environ", LIVEKIT_ENV, clear=False):
        result = await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    assert result.ok is False
    assert result.failure_reason == "bridge_page_unconfigured"


@pytest.mark.asyncio
async def test_join_reports_a_dispatch_failure_rather_than_raising() -> None:
    """call_manager needs a clean False to tear down and let the user retry."""
    provider = _provider()
    provider._client.create_bot = AsyncMock(side_effect=RecallError("boom"))
    with patch.dict("os.environ", LIVEKIT_ENV, clear=False):
        result = await provider.join(
            channel="teams_meet",
            meeting_url="https://teams.microsoft.com/l/meetup-join/x",
            display_name="Unify",
            room_name="unity_25_teams",
        )

    assert result.ok is False
    assert result.failure_reason == "recall_dispatch_failed"


# ---------------------------------------------------------------------------
# Relay wiring
# ---------------------------------------------------------------------------


def test_relay_url_is_derived_from_the_bridge_host() -> None:
    """One configured URL keeps page and relay from drifting onto two hosts."""
    with patch.dict("os.environ", {"MEET_BRIDGE_PAGE_URL": BRIDGE}, clear=False):
        assert _default_relay_url() == "wss://comms.example.com/meet/events"


@pytest.mark.asyncio
async def test_relay_is_skipped_when_no_secret_is_set() -> None:
    """Registering an endpoint the relay rejects is worse than none at all.

    Recall burns 30 reconnects per bot and then disables the endpoint for the
    whole workspace, which would break the relay for every later bot.
    """
    provider = _provider(relay_url="wss://comms.example.com/meet/events")
    env = dict(LIVEKIT_ENV)
    env["RECALL_RELAY_SECRET"] = ""
    with patch.dict("os.environ", env, clear=False):
        await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    assert provider._client.create_bot.call_args.kwargs["realtime_events_url"] is None


@pytest.mark.asyncio
async def test_relay_url_carries_the_room_and_token() -> None:
    provider = _provider(relay_url="wss://comms.example.com/meet/events")
    env = dict(LIVEKIT_ENV)
    env["RECALL_RELAY_SECRET"] = "relay-secret"  # pragma: allowlist secret
    with patch.dict("os.environ", env, clear=False):
        await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    url = provider._client.create_bot.call_args.kwargs["realtime_events_url"]
    query = parse_qs(urlparse(url).query)
    assert query["room"] == ["unity_25_gmeet"]
    assert query["token"] == ["relay-secret"]


# ---------------------------------------------------------------------------
# Participant video
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_asks_for_participant_video_by_default() -> None:
    """The request plumbing existed for a while and nothing ever set it.

    A bot dispatched without this joins fine and is simply blind to any screen
    somebody shares, which shows up as an assistant that ignores the thing the
    meeting is about.
    """
    provider = _provider()
    with patch.dict("os.environ", LIVEKIT_ENV, clear=False):
        await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    kwargs = provider._client.create_bot.call_args.kwargs
    assert kwargs["capture_participant_video"] is True


@pytest.mark.asyncio
async def test_participant_video_can_be_declined() -> None:
    """It carries a variant surcharge, so a workspace may opt out of it."""
    env = dict(LIVEKIT_ENV)
    env["RECALL_PARTICIPANT_VIDEO"] = "0"
    with patch.dict("os.environ", env, clear=False):
        provider = _provider()
        await provider.join(
            channel="google_meet",
            meeting_url="https://meet.google.com/abc",
            display_name="Unify",
            room_name="unity_25_gmeet",
        )

    kwargs = provider._client.create_bot.call_args.kwargs
    assert kwargs["capture_participant_video"] is False


# ---------------------------------------------------------------------------
# State translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_state_translates_lobby_and_roster() -> None:
    provider = _provider()
    provider._client.get_bot = AsyncMock(
        return_value=RecallBotState(
            bot_id="bot_1",
            status="in_waiting_room",
            participants=(
                RecallParticipant(
                    id="1",
                    name="Ada",
                    email="ada@example.com",
                    is_host=True,
                ),
            ),
        ),
    )
    state = await provider.state(channel="google_meet", session_id="bot_1")

    assert state.lobby is True
    assert state.ended is False
    assert state.participants[0].email == "ada@example.com"
    assert state.participants[0].is_host is True


@pytest.mark.asyncio
async def test_state_maps_a_denied_join_to_an_actionable_reason() -> None:
    """ "bot_denied_entry" is Recall's wording; ours drives the user-facing line."""
    provider = _provider()
    provider._client.get_bot = AsyncMock(
        return_value=RecallBotState(
            bot_id="bot_1",
            status="fatal",
            sub_code="bot_denied_entry",
        ),
    )
    state = await provider.state(channel="google_meet", session_id="bot_1")

    assert state.ended is True
    assert state.failure_reason == "meet_join_denied"


@pytest.mark.asyncio
async def test_unknown_sub_code_passes_through() -> None:
    """An unmapped reason must still reach logs rather than becoming None."""
    provider = _provider()
    provider._client.get_bot = AsyncMock(
        return_value=RecallBotState(
            bot_id="bot_1",
            status="fatal",
            sub_code="some_new_recall_code",
        ),
    )
    state = await provider.state(channel="google_meet", session_id="bot_1")

    assert state.failure_reason == "some_new_recall_code"


@pytest.mark.asyncio
async def test_state_returns_none_on_api_failure() -> None:
    """A failed poll is not evidence the meeting ended."""
    provider = _provider()
    provider._client.get_bot = AsyncMock(side_effect=RecallError("nope"))
    assert await provider.state(channel="google_meet", session_id="bot_1") is None


# ---------------------------------------------------------------------------
# Outbound screenshare (the assistant's own desktop)
# ---------------------------------------------------------------------------


DESKTOP_ENV = {
    "MEET_BRIDGE_PAGE_URL": BRIDGE,
    "RECALL_RELAY_SECRET": "relay-secret",  # pragma: allowlist secret
}


def _presenting_provider():
    """A provider whose managed desktop is entitled, healthy and reachable."""
    provider = _provider()
    provider._client.start_screenshare = AsyncMock()
    return provider


def _page_query(provider) -> dict:
    page_url = provider._client.start_screenshare.call_args.args[1]
    return {k: v[0] for k, v in parse_qs(urlparse(page_url).query).items()}


@pytest.mark.asyncio
async def test_present_frames_the_managed_desktop_liveview() -> None:
    """The meeting sees the real desktop, not a reconstruction of it."""
    provider = _presenting_provider()
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=True),
        ),
    ):
        assert await provider.present(channel="google_meet", session_id="bot_1")

    query = _page_query(provider)
    assert query["liveview"] == "https://vm.example.com/desktop/custom.html"
    assert query["sig"], "the page refuses an unsigned liveview"


@pytest.mark.asyncio
async def test_the_page_url_is_signed_so_it_cannot_frame_anything_else() -> None:
    """The page is unauthenticated: Recall loads it with no credential of ours.

    Unsigned, it would frame any URL anyone handed it on our own domain. The
    signature must be reproducible by the page, and must cover the liveview -- a
    signature over something else would let the URL be swapped.
    """
    import hashlib
    import hmac

    provider = _presenting_provider()
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=True),
        ),
    ):
        await provider.present(channel="google_meet", session_id="bot_1")

    query = _page_query(provider)
    payload = f"{query['liveview']}\x00{query.get('password', '')}".encode()
    expected = hmac.new(
        DESKTOP_ENV["RECALL_RELAY_SECRET"].encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    assert query["sig"] == expected


@pytest.mark.asyncio
async def test_present_prefers_the_desktop_secret_over_the_unify_key() -> None:
    """The per-binding secret is the intended credential; the key is a
    fallback for bindings minted before the secret existed."""
    provider = _presenting_provider()
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch.object(SESSION_DETAILS.assistant, "desktop_secret", "binding-secret"),
        patch.object(SESSION_DETAILS, "_unify_key", "the-unify-key"),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=True),
        ),
    ):
        await provider.present(channel="google_meet", session_id="bot_1")

    query = _page_query(provider)
    assert query["password"] == "binding-secret"  # pragma: allowlist secret


@pytest.mark.asyncio
async def test_present_falls_back_to_the_unify_key_without_a_desktop_secret() -> None:
    """Old bindings without a minted secret keep working on the legacy
    convention."""
    provider = _presenting_provider()
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch.object(SESSION_DETAILS.assistant, "desktop_secret", None),
        patch.object(SESSION_DETAILS, "_unify_key", "the-unify-key"),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=True),
        ),
    ):
        await provider.present(channel="google_meet", session_id="bot_1")

    query = _page_query(provider)
    assert query["password"] == "the-unify-key"  # pragma: allowlist secret


@pytest.mark.asyncio
async def test_present_refuses_when_the_desktop_is_not_serving() -> None:
    """Entitlement is not liveness.

    A dead VM would otherwise put noVNC's own error dialog on the meeting's main
    stage, which reads as the assistant being broken rather than the desktop.
    """
    provider = _presenting_provider()
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=False),
        ),
    ):
        assert (
            await provider.present(channel="google_meet", session_id="bot_1") is False
        )
    provider._client.start_screenshare.assert_not_called()


@pytest.mark.asyncio
async def test_present_reports_failure_rather_than_raising() -> None:
    """The assistant must be able to say it could not share.

    Claiming a screen is up when none is buys a participant hunting for it.
    """
    provider = _provider()
    provider._client.start_screenshare = AsyncMock(side_effect=RecallError("nope"))
    with (
        patch.dict("os.environ", DESKTOP_ENV, clear=False),
        patch(
            "unify.conversation_manager.domains.recall.provider.resolve_desktop_urls",
            return_value=("https://vm.example.com", "https://vm.example.com"),
        ),
        patch(
            "unify.conversation_manager.domains.recall.provider.desktop_proxy_healthy",
            AsyncMock(return_value=True),
        ),
    ):
        assert (
            await provider.present(channel="google_meet", session_id="bot_1") is False
        )


@pytest.mark.asyncio
async def test_stop_present_delegates_and_survives_a_gone_bot() -> None:
    provider = _provider()
    provider._client.stop_screenshare = AsyncMock()
    assert await provider.stop_present(channel="google_meet", session_id="bot_1")
    assert provider._client.stop_screenshare.call_args.args[0] == "bot_1"

    provider._client.stop_screenshare = AsyncMock(side_effect=RecallError("gone"))
    assert (
        await provider.stop_present(channel="google_meet", session_id="bot_1") is False
    )
