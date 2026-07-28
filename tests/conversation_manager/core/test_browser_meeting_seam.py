"""Backend construction behind the browser-meeting seam."""

from unittest.mock import MagicMock, patch

from unify.conversation_manager.domains.browser_meeting import (
    RECALL_PROVIDER,
    build_meet_provider,
)

RECALL_ENV = {
    "RECALL_API_KEY": "k",  # pragma: allowlist secret
    "MEET_BRIDGE_PAGE_URL": "https://comms.example.com/meet/bridge",
}


def _call_manager() -> MagicMock:
    cm = MagicMock()
    cm.assistant_id = 25
    return cm


def test_recall_is_the_backend() -> None:
    with patch.dict("os.environ", RECALL_ENV, clear=False):
        provider = build_meet_provider(_call_manager())

    assert provider.name == RECALL_PROVIDER


def test_the_assistant_id_reaches_the_backend() -> None:
    """It rides on bot metadata, which is how a bot is traced to an assistant."""
    with patch.dict("os.environ", RECALL_ENV, clear=False):
        provider = build_meet_provider(_call_manager())

    assert provider._assistant_id == "25"


def test_an_unconfigured_pod_fails_at_construction() -> None:
    """There is no second backend to fall back to now.

    Raising here surfaces the misconfiguration at join time with a clear cause,
    rather than somewhere deeper where it reads as a Recall outage.
    """
    from unify.conversation_manager.domains.recall.client import RecallNotConfigured

    env = dict(RECALL_ENV)
    env["RECALL_API_KEY"] = ""
    with patch.dict("os.environ", env, clear=False):
        try:
            build_meet_provider(_call_manager())
        except RecallNotConfigured:
            return
    raise AssertionError("expected RecallNotConfigured")
