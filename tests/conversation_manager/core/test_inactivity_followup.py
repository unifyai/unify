"""Unit tests for inactivity follow-up opt-out wiring (no brain wake).

Orchestra now emails soft check-ins from twin@ directly. Unity only
keeps activity-touch + opt-out / opt-in helpers and the matching
comms primitives so the boss can still ask to stop follow-ups from
console chat.

Covers:
    1. activity_sync.touch_assistant_activity
    2. activity_sync opt-out / opt-in helpers
    3. CommsPrimitives.stop_inactivity_followups / resume_inactivity_followups
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unify.comms.primitives import CommsPrimitives
from unify.transcript_manager.activity_sync import (
    opt_in_to_inactivity_followups_via_orchestra,
    opt_out_of_inactivity_followups_via_orchestra,
    touch_assistant_activity,
)

# ===========================================================================
# 1. activity_sync.touch_assistant_activity
# ===========================================================================


class TestTouchAssistantActivity:
    def test_returns_false_when_assistant_id_missing(self):
        assert touch_assistant_activity(None) is False

    def test_returns_false_when_assistant_id_not_int(self):
        assert touch_assistant_activity("not-an-int") is False

    def test_returns_false_when_orchestra_url_missing(self):
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value=None,
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
        ):
            assert touch_assistant_activity(42) is False

    def test_returns_false_when_admin_key_missing(self):
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value=None,
            ),
        ):
            assert touch_assistant_activity(42) is False

    def test_posts_to_correct_url_with_auth(self):
        captured: dict = {}

        def fake_post(url, headers=None, timeout=None, **kwargs):
            captured["url"] = url
            captured["headers"] = headers
            resp = MagicMock()
            resp.status_code = 200
            resp.text = ""
            return resp

        fake_http = MagicMock()
        fake_http.post = fake_post

        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="test-admin-key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=fake_http)}),
        ):
            assert touch_assistant_activity(42) is True

        assert captured["url"] == "http://orchestra.test/assistant/42/touch-activity"
        assert captured["headers"] == {"Authorization": "Bearer test-admin-key"}

    def test_returns_false_on_non_2xx(self):
        def fake_post(url, headers=None, timeout=None, **kwargs):
            resp = MagicMock()
            resp.status_code = 500
            resp.text = "boom"
            return resp

        fake_http = MagicMock()
        fake_http.post = fake_post

        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=fake_http)}),
        ):
            assert touch_assistant_activity(42) is False

    def test_swallows_exceptions(self):
        def fake_post(*_a, **_kw):
            raise RuntimeError("network down")

        fake_http = MagicMock()
        fake_http.post = fake_post

        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=fake_http)}),
        ):
            # Should not raise
            assert touch_assistant_activity(42) is False


# ===========================================================================
# 2. Opt-out / opt-in sync helpers
# ===========================================================================


class TestFollowupOptOutHelpers:
    """opt_out_of_inactivity_followups_via_orchestra + opt_in_… ."""

    def _make_fake_http(self, *, status: int = 200):
        captured: dict = {}

        def fake_post(url, headers=None, timeout=None, **kwargs):
            captured["url"] = url
            captured["headers"] = headers
            resp = MagicMock()
            resp.status_code = status
            resp.text = "" if status < 400 else "boom"
            return resp

        http_module = MagicMock()
        http_module.post = fake_post
        return http_module, captured

    def test_opt_out_posts_to_correct_url(self):
        http_module, captured = self._make_fake_http(status=200)
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="test-admin-key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=http_module)}),
        ):
            assert opt_out_of_inactivity_followups_via_orchestra(42) is True

        assert captured["url"] == "http://orchestra.test/assistant/42/opt-out-followups"
        assert captured["headers"] == {"Authorization": "Bearer test-admin-key"}

    def test_opt_in_posts_to_correct_url(self):
        http_module, captured = self._make_fake_http(status=200)
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="test-admin-key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=http_module)}),
        ):
            assert opt_in_to_inactivity_followups_via_orchestra(42) is True

        assert captured["url"] == "http://orchestra.test/assistant/42/opt-in-followups"

    def test_opt_out_returns_false_on_missing_config(self):
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value=None,
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
        ):
            assert opt_out_of_inactivity_followups_via_orchestra(42) is False

    def test_opt_out_returns_false_on_non_2xx(self):
        http_module, _ = self._make_fake_http(status=500)
        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=http_module)}),
        ):
            assert opt_out_of_inactivity_followups_via_orchestra(42) is False

    def test_swallows_exceptions(self):
        def raising_post(*_a, **_kw):
            raise RuntimeError("network down")

        http_module = MagicMock()
        http_module.post = raising_post

        with (
            patch(
                "unify.transcript_manager.activity_sync._base_url",
                return_value="http://orchestra.test",
            ),
            patch(
                "unify.transcript_manager.activity_sync._admin_key",
                return_value="key",
            ),
            patch.dict("sys.modules", {"unisdk.utils": MagicMock(http=http_module)}),
        ):
            assert opt_out_of_inactivity_followups_via_orchestra(42) is False
            assert opt_in_to_inactivity_followups_via_orchestra(42) is False


# ===========================================================================
# 3. CommsPrimitives.stop_inactivity_followups / resume_inactivity_followups
# ===========================================================================


class TestCommsFollowupOptOutPrimitives:
    """The brain-callable opt-out wrappers on CommsPrimitives."""

    def test_optout_methods_in_primitive_methods(self):
        """They must be discoverable by the function-manager registry."""
        assert "stop_inactivity_followups" in CommsPrimitives._PRIMITIVE_METHODS
        assert "resume_inactivity_followups" in CommsPrimitives._PRIMITIVE_METHODS

    @pytest.mark.anyio
    async def test_stop_calls_helper_and_returns_shape(self):
        primitives = CommsPrimitives.__new__(CommsPrimitives)
        primitives._cm = None
        primitives._event_broker = MagicMock()

        fake_session = MagicMock()
        fake_session.assistant.agent_id = 99

        with (
            patch("unify.comms.primitives.SESSION_DETAILS", fake_session),
            patch(
                "unify.transcript_manager.activity_sync."
                "opt_out_of_inactivity_followups_via_orchestra",
                return_value=True,
            ) as mock_stop,
        ):
            result = await primitives.stop_inactivity_followups()

        mock_stop.assert_called_once_with(99)
        assert result == {"success": True, "assistant_id": 99}

    @pytest.mark.anyio
    async def test_resume_calls_helper_and_returns_shape(self):
        primitives = CommsPrimitives.__new__(CommsPrimitives)
        primitives._cm = None
        primitives._event_broker = MagicMock()

        fake_session = MagicMock()
        fake_session.assistant.agent_id = 99

        with (
            patch("unify.comms.primitives.SESSION_DETAILS", fake_session),
            patch(
                "unify.transcript_manager.activity_sync."
                "opt_in_to_inactivity_followups_via_orchestra",
                return_value=True,
            ) as mock_resume,
        ):
            result = await primitives.resume_inactivity_followups()

        mock_resume.assert_called_once_with(99)
        assert result == {"success": True, "assistant_id": 99}

    @pytest.mark.anyio
    async def test_propagates_failure(self):
        primitives = CommsPrimitives.__new__(CommsPrimitives)
        primitives._cm = None
        primitives._event_broker = MagicMock()

        fake_session = MagicMock()
        fake_session.assistant.agent_id = 99

        with (
            patch("unify.comms.primitives.SESSION_DETAILS", fake_session),
            patch(
                "unify.transcript_manager.activity_sync."
                "opt_out_of_inactivity_followups_via_orchestra",
                return_value=False,
            ),
        ):
            result = await primitives.stop_inactivity_followups()
        assert result == {"success": False, "assistant_id": 99}

    @pytest.mark.anyio
    async def test_handles_missing_agent_id(self):
        primitives = CommsPrimitives.__new__(CommsPrimitives)
        primitives._cm = None
        primitives._event_broker = MagicMock()

        fake_session = MagicMock()
        fake_session.assistant.agent_id = None

        with (
            patch("unify.comms.primitives.SESSION_DETAILS", fake_session),
            patch(
                "unify.transcript_manager.activity_sync."
                "opt_out_of_inactivity_followups_via_orchestra",
                return_value=False,
            ),
        ):
            result = await primitives.stop_inactivity_followups()
        assert result == {"success": False, "assistant_id": None}
