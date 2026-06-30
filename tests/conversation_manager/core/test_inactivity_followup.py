"""Unit tests for the inactivity-followup unity-side wiring.

Covers:
    1. Event factories
        - _inactivity_followup_event_from_payload accepts any dict, copies reason
        - _inactivity_followup_event_from_wake_reason gates on type=inactivity_followup
    2. Handler side effects
        - pushes a notification to cm.notifications_bar
        - logs to the session logger
    3. EventHandler registration
    4. activity_sync.touch_assistant_activity
        - returns False on missing config
        - posts to the right URL with admin auth on success
        - swallows exceptions
    5. activity_sync opt-out / opt-in helpers
    6. CommsPrimitives.stop_inactivity_followups / resume_inactivity_followups
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from unify.comms.primitives import CommsPrimitives
from unify.conversation_manager.domains.event_handlers import EventHandler
from unify.conversation_manager.domains.inactivity import (
    _DEFAULT_REASON,
    _handle_inactivity_followup_event,
    _inactivity_followup_event_from_payload,
    _inactivity_followup_event_from_wake_reason,
    _inactivity_notification_text,
)
from unify.conversation_manager.domains.notifications import NotificationBar
from unify.conversation_manager.events import InactivityFollowup
from unify.transcript_manager.activity_sync import (
    opt_in_to_inactivity_followups_via_orchestra,
    opt_out_of_inactivity_followups_via_orchestra,
    touch_assistant_activity,
)

# ===========================================================================
# 1. Event factories
# ===========================================================================


class TestInactivityEventFactories:
    def test_payload_factory_returns_event_with_default_reason(self):
        event = _inactivity_followup_event_from_payload({})
        assert isinstance(event, InactivityFollowup)
        assert event.reason == _DEFAULT_REASON

    def test_payload_factory_uses_explicit_reason_when_given(self):
        event = _inactivity_followup_event_from_payload({}, reason="custom reason")
        assert event is not None
        assert event.reason == "custom reason"

    def test_payload_factory_returns_none_for_non_dict(self):
        assert _inactivity_followup_event_from_payload(None) is None  # type: ignore[arg-type]
        assert _inactivity_followup_event_from_payload("not a dict") is None  # type: ignore[arg-type]

    def test_wake_reason_factory_accepts_inactivity_type(self):
        event = _inactivity_followup_event_from_wake_reason(
            {"type": "inactivity_followup"},
        )
        assert isinstance(event, InactivityFollowup)
        assert event.reason == _DEFAULT_REASON

    def test_wake_reason_factory_rejects_other_types(self):
        assert (
            _inactivity_followup_event_from_wake_reason(
                {"type": "task_due", "task_id": 1},
            )
            is None
        )
        assert _inactivity_followup_event_from_wake_reason("not a dict") is None
        assert _inactivity_followup_event_from_wake_reason(None) is None


# ===========================================================================
# 2. Handler side effects
# ===========================================================================


class TestInactivityHandler:
    @pytest.mark.anyio
    async def test_handler_pushes_notification_and_logs(self):
        cm = MagicMock()
        cm.notifications_bar = NotificationBar()
        cm._session_logger = MagicMock()

        event = InactivityFollowup(reason="Time to follow up.")
        result = await _handle_inactivity_followup_event(event, cm)

        assert result is True
        assert len(cm.notifications_bar.notifications) == 1
        notif = cm.notifications_bar.notifications[0]
        assert notif.type == "Inactivity"
        # Reason text must be in the notification body
        assert "Time to follow up." in notif.content
        cm._session_logger.info.assert_called_once()

    def test_notification_text_includes_variant_guidance(self):
        text = _inactivity_notification_text(InactivityFollowup(reason="hi"))
        assert "hi" in text
        # Brain should know to inspect transcript history to choose variant
        assert "transcript" in text.lower()

    def test_notification_text_includes_optout_primitives(self):
        text = _inactivity_notification_text(InactivityFollowup(reason="hi"))
        # Brain must know it can honour an explicit opt-out via primitives,
        # and the old deletion-oriented primitives must be gone.
        assert "stop_inactivity_followups" in text
        assert "resume_inactivity_followups" in text
        assert "terminate_self" not in text
        assert "cancel_self_termination" not in text
        # Opt-out never deletes anything.
        assert "deleted" in text.lower()

    def test_notification_text_directs_email_send(self):
        """Inactivity follow-ups are email-only for V0."""
        text = _inactivity_notification_text(InactivityFollowup(reason="hi"))
        text_lower = text.lower()
        assert "send_email" in text
        assert "email" in text_lower
        # No primary-channel branching: brain must NOT be told to pick WhatsApp
        # over email based on availability.
        assert "whatsapp if available" not in text_lower
        assert "preferred channel" not in text_lower

    def test_notification_text_keeps_whatsapp_as_callback(self):
        """If the assistant has its own WhatsApp number, it goes in the body
        as a callback option only — never as the sending channel."""
        text = _inactivity_notification_text(InactivityFollowup(reason="hi"))
        text_lower = text.lower()
        assert "whatsapp" in text_lower
        assert "callback" in text_lower
        # Conditional phrasing ("if you have ...") must be present so the brain
        # only includes the number when one is provisioned.
        assert "if you have" in text_lower


# ===========================================================================
# 3. Registration
# ===========================================================================


class TestInactivityRegistration:
    def test_inactivity_followup_is_registered(self):
        assert (
            InactivityFollowup in EventHandler._registry
        ), "InactivityFollowup handler should be registered"


# ===========================================================================
# 4. activity_sync.touch_assistant_activity
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

        assert (
            captured["url"] == "http://orchestra.test/admin/assistant/42/touch-activity"
        )
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
# 5. Opt-out / opt-in sync helpers
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

        assert (
            captured["url"]
            == "http://orchestra.test/admin/assistant/42/opt-out-followups"
        )
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

        assert (
            captured["url"]
            == "http://orchestra.test/admin/assistant/42/opt-in-followups"
        )

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
# 6. CommsPrimitives.stop_inactivity_followups / resume_inactivity_followups
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
