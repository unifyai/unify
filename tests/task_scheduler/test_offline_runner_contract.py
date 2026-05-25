"""Tests for the shared offline_runner contract module.

These tests pin down the env-var and run-key shapes that both the local
in-process subprocess path and the hosted Kubernetes job path depend on.
If either side drifts from this contract, ``offline_runner._load_config_from_env``
breaks for that topology.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from unity.task_scheduler.offline_runner_contract import (
    build_offline_run_key,
    build_offline_runner_env,
    normalize_run_key_component,
)

# --------------------------------------------------------------------------- #
# build_offline_runner_env                                                    #
# --------------------------------------------------------------------------- #


class TestBuildOfflineRunnerEnv:
    """Env shape — verifies every key offline_runner._load_config_from_env reads."""

    @staticmethod
    def _make_env(**overrides):
        defaults = dict(
            assistant_id="assistant-123",
            task_id=101,
            source_task_log_id=555,
            activation_revision="rev-1",
            source_type="scheduled",
            run_key="offline:scheduled:assistant-123:101:rev-digest:once",
        )
        defaults.update(overrides)
        return build_offline_runner_env(**defaults)

    def test_required_env_vars_present(self):
        env = self._make_env()
        required = {
            "UNITY_OFFLINE_TASK_MODE",
            "EVENTBUS_PUBLISHING_ENABLED",
            "EVENTBUS_PUBSUB_STREAMING",
            "UNITY_OFFLINE_TASK_RUN_KEY",
            "UNITY_OFFLINE_TASK_ID",
            "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID",
            "UNITY_OFFLINE_TASK_ACTIVATION_REVISION",
            "UNITY_OFFLINE_TASK_FUNCTION_ID",
            "UNITY_OFFLINE_TASK_REQUEST",
            "UNITY_OFFLINE_TASK_NAME",
            "UNITY_OFFLINE_TASK_DESCRIPTION",
            "UNITY_OFFLINE_TASK_SOURCE_TYPE",
            "UNITY_OFFLINE_TASK_SCHEDULED_FOR",
            "UNITY_OFFLINE_TASK_SOURCE_REF",
            "UNITY_OFFLINE_TASK_SOURCE_MEDIUM",
            "UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID",
            "ASSISTANT_ID",
        }
        assert required - set(env.keys()) == set()

    def test_mode_is_actor(self):
        env = self._make_env()
        assert env["UNITY_OFFLINE_TASK_MODE"] == "actor"

    def test_eventbus_disabled(self):
        env = self._make_env()
        assert env["EVENTBUS_PUBLISHING_ENABLED"] == "false"
        assert env["EVENTBUS_PUBSUB_STREAMING"] == "false"

    def test_function_id_blank_without_entrypoint(self):
        env = self._make_env(entrypoint=None)
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == ""

    def test_function_id_serialised_with_entrypoint(self):
        env = self._make_env(entrypoint=42)
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == "42"

    def test_request_text_uses_description_first(self):
        env = self._make_env(
            task_description="The full description",
            task_name="Short name",
        )
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == "The full description"

    def test_request_text_falls_back_to_name(self):
        env = self._make_env(task_description="", task_name="Just the name")
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == "Just the name"

    def test_request_text_synthesised_when_both_empty(self):
        env = self._make_env(task_description="", task_name="")
        assert env["UNITY_OFFLINE_TASK_REQUEST"] == "Execute task 101"

    def test_scheduled_for_normalised_to_utc(self):
        env = self._make_env(scheduled_for="2030-04-10T11:00:00+02:00")
        assert env["UNITY_OFFLINE_TASK_SCHEDULED_FOR"] == "2030-04-10T09:00:00+00:00"

    def test_scheduled_for_z_suffix_normalised(self):
        env = self._make_env(scheduled_for="2030-04-10T09:00:00Z")
        assert env["UNITY_OFFLINE_TASK_SCHEDULED_FOR"] == "2030-04-10T09:00:00+00:00"

    def test_scheduled_for_naive_treated_as_utc(self):
        env = self._make_env(
            scheduled_for=datetime(2030, 4, 10, 9, 0, 0),  # naive
        )
        assert env["UNITY_OFFLINE_TASK_SCHEDULED_FOR"] == "2030-04-10T09:00:00+00:00"

    def test_scheduled_for_none_serialises_to_empty_string(self):
        env = self._make_env(scheduled_for=None)
        assert env["UNITY_OFFLINE_TASK_SCHEDULED_FOR"] == ""

    def test_display_name_only_when_provided(self):
        env_with = self._make_env(source_contact_display_name="Alice")
        env_without = self._make_env(source_contact_display_name=None)
        assert env_with["UNITY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME"] == "Alice"
        assert "UNITY_OFFLINE_TASK_SOURCE_CONTACT_DISPLAY_NAME" not in env_without

    def test_job_name_only_when_provided(self):
        env_with = self._make_env(job_name="unity-offline-abc")
        env_without = self._make_env(job_name="")
        assert env_with["UNITY_OFFLINE_TASK_JOB_NAME"] == "unity-offline-abc"
        assert "UNITY_OFFLINE_TASK_JOB_NAME" not in env_without

    def test_contact_id_str_or_int_both_serialised(self):
        env_int = self._make_env(source_contact_id=77)
        env_str = self._make_env(source_contact_id="77")
        assert env_int["UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID"] == "77"
        assert env_str["UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID"] == "77"


# --------------------------------------------------------------------------- #
# build_offline_run_key                                                       #
# --------------------------------------------------------------------------- #


class TestBuildOfflineRunKey:
    """Run-key shape — Communication's existing tests pin this exactly."""

    def test_scheduled_minimal_key_shape(self):
        key = build_offline_run_key(
            assistant_id="assistant-123",
            task_id=101,
            activation_revision="rev-abc",
            source_type="scheduled",
        )
        revision_digest = hashlib.sha256(b"rev-abc").hexdigest()[:12]
        assert key == f"offline:scheduled:assistant-123:101:{revision_digest}:once"

    def test_scheduled_with_due_includes_timestamp(self):
        key = build_offline_run_key(
            assistant_id="assistant-123",
            task_id=101,
            activation_revision="rev-abc",
            source_type="scheduled",
            scheduled_for="2030-04-10T09:00:00+00:00",
        )
        revision_digest = hashlib.sha256(b"rev-abc").hexdigest()[:12]
        assert key == (
            f"offline:scheduled:assistant-123:101:{revision_digest}:"
            f"20300410T090000Z"
        )

    def test_triggered_full_tail_matches_hosted_shape(self):
        """This is the exact assertion Communication's test_offline_run_key... has."""

        key = build_offline_run_key(
            assistant_id="assistant-123",
            task_id=101,
            activation_revision="rev-123",
            source_type="triggered",
            source_medium="sms_message",
            source_ref="message-123",
            source_contact_id="77",
        )
        revision_digest = hashlib.sha256(b"rev-123").hexdigest()[:12]
        source_ref_digest = hashlib.sha256(b"message-123").hexdigest()[:12]
        assert key == (
            f"offline:triggered:assistant-123:101:{revision_digest}:"
            f"contact-77-sms-message-{source_ref_digest}"
        )

    def test_revision_change_yields_different_key(self):
        a = build_offline_run_key(
            assistant_id="x",
            task_id=1,
            activation_revision="rev-1",
            source_type="scheduled",
        )
        b = build_offline_run_key(
            assistant_id="x",
            task_id=1,
            activation_revision="rev-2",
            source_type="scheduled",
        )
        assert a != b

    def test_datetime_input_for_scheduled_for(self):
        ts = datetime(2030, 4, 10, 9, 0, 0, tzinfo=timezone.utc)
        key = build_offline_run_key(
            assistant_id="x",
            task_id=1,
            activation_revision="rev",
            source_type="scheduled",
            scheduled_for=ts,
        )
        assert key.endswith(":20300410T090000Z")

    def test_naive_datetime_treated_as_utc(self):
        ts = datetime(2030, 4, 10, 9, 0, 0)  # naive
        key = build_offline_run_key(
            assistant_id="x",
            task_id=1,
            activation_revision="rev",
            source_type="scheduled",
            scheduled_for=ts,
        )
        assert key.endswith(":20300410T090000Z")

    def test_invalid_scheduled_for_string_is_dropped(self):
        key = build_offline_run_key(
            assistant_id="x",
            task_id=1,
            activation_revision="rev",
            source_type="scheduled",
            scheduled_for="not-a-date",
        )
        # Falls through to the default tail.
        assert key.endswith(":once")


# --------------------------------------------------------------------------- #
# normalize_run_key_component                                                 #
# --------------------------------------------------------------------------- #


class TestNormalizeRunKeyComponent:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("sms_message", "sms-message"),
            ("SMS_Message", "sms-message"),
            ("foo bar", "foo-bar"),
            ("foo  bar", "foo-bar"),  # collapse runs of non-dash chars
            # Note: consecutive dashes in the SOURCE are preserved (the
            # regex matches non-dash special chars only). This matches
            # Communication's _normalize_task_id_component exactly.
            ("foo--bar", "foo--bar"),
            ("---foo---", "foo"),  # strip leading/trailing dashes
            ("github_review!", "github-review"),  # special chars
            ("123abc", "123abc"),  # digits ok
        ],
    )
    def test_normalises_to_run_key_safe_form(self, raw, expected):
        assert normalize_run_key_component(raw) == expected

    def test_empty_input_returns_assistant_fallback(self):
        assert normalize_run_key_component("") == "assistant"
        assert normalize_run_key_component("---") == "assistant"
