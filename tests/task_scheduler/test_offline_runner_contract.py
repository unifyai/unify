"""Tests for the shared offline_runner contract module.

These tests pin down the env-var and run-key shapes that both the local
in-process subprocess path and the hosted Kubernetes job path depend on.
If either side drifts from this contract, ``offline_runner._load_config_from_env``
breaks for that topology.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone  # noqa: F401  (timezone used by golden ref)

import pytest

from unify.task_scheduler.offline_runner_contract import (
    build_offline_run_key,
    build_offline_runner_env,
    build_provider_event_run_key,
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
            "UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM",
            "UNITY_OFFLINE_TASK_REQUIRES_COMPUTER",
            "ASSISTANT_ID",
        }
        assert required - set(env.keys()) == set()

    def test_resource_flags_default_to_zero(self):
        env = self._make_env()
        assert env["UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM"] == "0"
        assert env["UNITY_OFFLINE_TASK_REQUIRES_COMPUTER"] == "0"

    def test_resource_flags_emit_one_when_true(self):
        env = self._make_env(requires_filesystem=True, requires_computer=True)
        assert env["UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM"] == "1"
        assert env["UNITY_OFFLINE_TASK_REQUIRES_COMPUTER"] == "1"

    def test_mode_is_actor(self):
        env = self._make_env()
        assert env["UNITY_OFFLINE_TASK_MODE"] == "actor"

    def test_eventbus_not_forced_off(self):
        """Offline runners inherit EventBus settings from the full pod env."""
        env = self._make_env()
        assert "EVENTBUS_PUBLISHING_ENABLED" not in env
        assert "EVENTBUS_PUBSUB_STREAMING" not in env

    def test_function_id_blank_without_entrypoint(self):
        env = self._make_env(entrypoint=None)
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == ""

    def test_function_id_serialised_with_entrypoint(self):
        env = self._make_env(entrypoint=42)
        assert env["UNITY_OFFLINE_TASK_FUNCTION_ID"] == "42"

    def test_destination_omitted_without_destination(self):
        env = self._make_env()
        assert "TASK_DESTINATION" not in env

    def test_destination_set_when_provided(self):
        env = self._make_env(destination="team:41001")
        assert env["TASK_DESTINATION"] == "team:41001"

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

    def test_provider_event_env_requires_dispatch_fields(self):
        with pytest.raises(ValueError, match="provider_event offline runs require"):
            self._make_env(source_type="provider_event")

    def test_provider_event_env_includes_dispatch_identity(self):
        env = self._make_env(
            source_type="provider_event",
            provider_event_operation_id="op-1",
            provider_event_run_id=42,
            provider_event_binding_id="binding-1",
            provider_event_receipt_id="receipt-1",
            provider_event_context_ref="blob://binding-1/receipt-1",
            provider_event_issued_at="2030-04-10T09:00:00+00:00",
        )
        assert env["UNITY_OFFLINE_PROVIDER_EVENT_OPERATION_ID"] == "op-1"
        assert env["UNITY_OFFLINE_PROVIDER_EVENT_RUN_ID"] == "42"
        assert env["UNITY_OFFLINE_PROVIDER_EVENT_BINDING_ID"] == "binding-1"
        assert env["UNITY_OFFLINE_PROVIDER_EVENT_RECEIPT_ID"] == "receipt-1"
        assert (
            env["UNITY_OFFLINE_PROVIDER_EVENT_CONTEXT_REF"]
            == "blob://binding-1/receipt-1"
        )
        assert env["UNITY_OFFLINE_PROVIDER_EVENT_ISSUED_AT"] == (
            "2030-04-10T09:00:00+00:00"
        )


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

    def test_provider_event_run_key_includes_full_hmac_digest(self) -> None:
        identity = "abcdef0123456789deadbeefcafebabe"  # pragma: allowlist secret
        key = build_provider_event_run_key(
            assistant_id="assistant-1",
            task_id=101,
            binding_id="binding-1",
            activation_revision="rev-shared",
            event_identity_hmac=identity,
            execution_mode="live",
        )
        revision_digest = hashlib.sha256(b"rev-shared").hexdigest()[:12]
        assert key == (
            f"live:provider_event:assistant-1:101:binding-1:"
            f"{revision_digest}:{identity}"
        )

    def test_provider_event_run_key_prefix_collision_does_not_collide(self) -> None:
        first = build_provider_event_run_key(
            assistant_id="assistant-1",
            task_id=101,
            binding_id="binding-1",
            activation_revision="rev-shared",
            event_identity_hmac="abcdef0123456789aaa111",  # pragma: allowlist secret
        )
        second = build_provider_event_run_key(
            assistant_id="assistant-1",
            task_id=101,
            binding_id="binding-1",
            activation_revision="rev-shared",
            event_identity_hmac="abcdef0123456789bbb222",  # pragma: allowlist secret
        )
        assert first != second
        assert first.endswith("abcdef0123456789aaa111")  # pragma: allowlist secret
        assert second.endswith("abcdef0123456789bbb222")  # pragma: allowlist secret


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


# --------------------------------------------------------------------------- #
# Equivalence with Communication's pre-refactor _build_offline_runner_env     #
# --------------------------------------------------------------------------- #
#                                                                             #
# These tests pin the property that motivates the existence of the shared    #
# contract: for any given request + activation + assistant_data + run_key,   #
# the original Communication-side function and the new shared+hosted-layer   #
# composition produce identical env dicts. If they ever diverged, the same   #
# task would execute differently across topologies. These tests reproduce    #
# the field shape exactly so any drift fails loudly here, in Unity's test    #
# suite, before reaching Communication's deployment.                         #
# --------------------------------------------------------------------------- #


class _FakeOfflineRequest:
    """Stand-in for ``OfflineTaskDispatchRequest`` for equivalence testing."""

    def __init__(self, **kwargs):
        self.assistant_id = kwargs.get("assistant_id", "assistant-123")
        self.task_id = kwargs.get("task_id", 101)
        self.source_task_log_id = kwargs.get("source_task_log_id", 555)
        self.activation_revision = kwargs.get("activation_revision", "rev-1")
        self.source_type = kwargs.get("source_type", "scheduled")
        self.execution_mode = kwargs.get("execution_mode", "offline")
        self.entrypoint = kwargs.get("entrypoint")
        self.scheduled_for = kwargs.get("scheduled_for")
        self.source_ref = kwargs.get("source_ref")
        self.source_medium = kwargs.get("source_medium")
        self.source_contact_id = kwargs.get("source_contact_id")


def _original_communication_env_builder(
    *,
    request,
    activation: dict,
    assistant_data: dict,
    run_key: str,
    job_name: str,
) -> dict[str, str]:
    """Verbatim copy of Communication's pre-refactor _build_offline_runner_env.

    Used as a golden reference: every assertion below confirms the new
    shared+hosted-layer composition produces exactly the same dict.
    """

    team_ids = assistant_data.get("team_ids") or []
    task_request = (
        str(activation.get("task_description") or "").strip()
        or str(activation.get("task_name") or "").strip()
        or f"Execute task {request.task_id}"
    )
    entrypoint = activation.get("entrypoint") or request.entrypoint

    def _request_scheduled_for_iso(req):
        if req.scheduled_for is None:
            return None
        return req.scheduled_for.astimezone(timezone.utc).isoformat()

    return {
        "UNITY_OFFLINE_TASK_MODE": "actor",
        "UNITY_OFFLINE_TASK_RUN_KEY": run_key,
        "UNITY_OFFLINE_TASK_JOB_NAME": job_name,
        "UNITY_OFFLINE_TASK_ID": str(request.task_id),
        "UNITY_OFFLINE_TASK_SOURCE_TASK_LOG_ID": str(request.source_task_log_id),
        "UNITY_OFFLINE_TASK_ACTIVATION_REVISION": request.activation_revision,
        "UNITY_OFFLINE_TASK_FUNCTION_ID": str(int(entrypoint)) if entrypoint else "",
        "UNITY_OFFLINE_TASK_REQUEST": task_request,
        "UNITY_OFFLINE_TASK_NAME": str(activation.get("task_name") or ""),
        "UNITY_OFFLINE_TASK_DESCRIPTION": str(activation.get("task_description") or ""),
        "UNITY_OFFLINE_TASK_SOURCE_TYPE": request.source_type,
        "UNITY_OFFLINE_TASK_SCHEDULED_FOR": _request_scheduled_for_iso(request) or "",
        "UNITY_OFFLINE_TASK_SOURCE_REF": request.source_ref or "",
        "UNITY_OFFLINE_TASK_SOURCE_MEDIUM": (
            request.source_medium or str(activation.get("trigger_medium") or "")
        ),
        "UNITY_OFFLINE_TASK_SOURCE_CONTACT_ID": (
            str(request.source_contact_id)
            if request.source_contact_id is not None
            else ""
        ),
        "UNITY_OFFLINE_TASK_REQUIRES_FILESYSTEM": "0",
        "UNITY_OFFLINE_TASK_REQUIRES_COMPUTER": "0",
        "UNIFY_KEY": str(assistant_data.get("api_key") or ""),
        "ASSISTANT_ID": str(assistant_data.get("assistant_id") or request.assistant_id),
        "ASSISTANT_FIRST_NAME": str(assistant_data.get("assistant_first_name") or ""),
        "ASSISTANT_SURNAME": str(assistant_data.get("assistant_surname") or ""),
        "ASSISTANT_AGE": str(assistant_data.get("assistant_age") or ""),
        "ASSISTANT_NATIONALITY": str(assistant_data.get("assistant_nationality") or ""),
        "ASSISTANT_TIMEZONE": str(assistant_data.get("assistant_timezone") or "UTC"),
        "ASSISTANT_ABOUT": str(assistant_data.get("assistant_about") or ""),
        "ASSISTANT_JOB_TITLE": str(assistant_data.get("assistant_job_title") or ""),
        "ASSISTANT_NUMBER": str(assistant_data.get("assistant_number") or ""),
        "ASSISTANT_EMAIL": str(assistant_data.get("assistant_email") or ""),
        "ASSISTANT_WHATSAPP_NUMBER": str(
            assistant_data.get("assistant_whatsapp_number") or "",
        ),
        "ASSISTANT_DESKTOP_MODE": str(
            assistant_data.get("desktop_mode") or "ubuntu",
        ),
        "ASSISTANT_USER_DESKTOPS": json.dumps(
            assistant_data.get("user_desktops") or [],
        ),
        "USER_ID": str(assistant_data.get("user_id") or ""),
        "USER_FIRST_NAME": str(assistant_data.get("user_first_name") or ""),
        "USER_SURNAME": str(assistant_data.get("user_surname") or ""),
        "USER_NUMBER": str(assistant_data.get("user_number") or ""),
        "USER_EMAIL": str(assistant_data.get("user_email") or ""),
        "USER_WHATSAPP_NUMBER": str(assistant_data.get("user_whatsapp_number") or ""),
        "VOICE_PROVIDER": str(assistant_data.get("voice_provider") or "cartesia"),
        "VOICE_ID": str(assistant_data.get("voice_id") or ""),
        "VOICE_MODE": "tts",
        "TEAM_IDS": ",".join(str(team_id) for team_id in team_ids),
        "ORG_ID": (
            str(assistant_data.get("org_id"))
            if assistant_data.get("org_id") is not None
            else ""
        ),
    }


def _new_communication_env_builder(
    *,
    request,
    activation: dict,
    assistant_data: dict,
    run_key: str,
    job_name: str,
) -> dict[str, str]:
    """Reproduces Communication's NEW _build_offline_runner_env composition.

    Layer 1: shared Unity contract. Layer 2: hosted-only assistant identity.
    Mirrors the refactored Communication function exactly.
    """

    entrypoint = activation.get("entrypoint") or request.entrypoint
    env = build_offline_runner_env(
        assistant_id=(str(assistant_data.get("assistant_id") or request.assistant_id)),
        task_id=request.task_id,
        source_task_log_id=request.source_task_log_id,
        activation_revision=request.activation_revision,
        source_type=request.source_type,
        run_key=run_key,
        task_name=str(activation.get("task_name") or ""),
        task_description=str(activation.get("task_description") or ""),
        scheduled_for=request.scheduled_for,
        source_ref=request.source_ref,
        source_medium=(
            request.source_medium or str(activation.get("trigger_medium") or "")
        ),
        source_contact_id=request.source_contact_id,
        entrypoint=entrypoint,
        job_name=job_name,
    )
    team_ids = assistant_data.get("team_ids") or []
    env.update(
        {
            "UNIFY_KEY": str(assistant_data.get("api_key") or ""),
            "ASSISTANT_FIRST_NAME": str(
                assistant_data.get("assistant_first_name") or "",
            ),
            "ASSISTANT_SURNAME": str(assistant_data.get("assistant_surname") or ""),
            "ASSISTANT_AGE": str(assistant_data.get("assistant_age") or ""),
            "ASSISTANT_NATIONALITY": str(
                assistant_data.get("assistant_nationality") or "",
            ),
            "ASSISTANT_TIMEZONE": str(
                assistant_data.get("assistant_timezone") or "UTC",
            ),
            "ASSISTANT_ABOUT": str(assistant_data.get("assistant_about") or ""),
            "ASSISTANT_JOB_TITLE": str(
                assistant_data.get("assistant_job_title") or "",
            ),
            "ASSISTANT_NUMBER": str(assistant_data.get("assistant_number") or ""),
            "ASSISTANT_EMAIL": str(assistant_data.get("assistant_email") or ""),
            "ASSISTANT_WHATSAPP_NUMBER": str(
                assistant_data.get("assistant_whatsapp_number") or "",
            ),
            "ASSISTANT_DESKTOP_MODE": str(
                assistant_data.get("desktop_mode") or "ubuntu",
            ),
            "ASSISTANT_USER_DESKTOPS": json.dumps(
                assistant_data.get("user_desktops") or [],
            ),
            "USER_ID": str(assistant_data.get("user_id") or ""),
            "USER_FIRST_NAME": str(assistant_data.get("user_first_name") or ""),
            "USER_SURNAME": str(assistant_data.get("user_surname") or ""),
            "USER_NUMBER": str(assistant_data.get("user_number") or ""),
            "USER_EMAIL": str(assistant_data.get("user_email") or ""),
            "USER_WHATSAPP_NUMBER": str(
                assistant_data.get("user_whatsapp_number") or "",
            ),
            "VOICE_PROVIDER": str(
                assistant_data.get("voice_provider") or "cartesia",
            ),
            "VOICE_ID": str(assistant_data.get("voice_id") or ""),
            "VOICE_MODE": "tts",
            "TEAM_IDS": ",".join(str(team_id) for team_id in team_ids),
            "ORG_ID": (
                str(assistant_data.get("org_id"))
                if assistant_data.get("org_id") is not None
                else ""
            ),
        },
    )
    return env


class TestCommunicationEnvBuilderEquivalence:
    """The new shared+hosted composition matches the old monolithic builder."""

    @staticmethod
    def _scenario(**overrides):
        from datetime import datetime as _dt

        request_kwargs = {
            "assistant_id": "assistant-123",
            "task_id": 101,
            "source_task_log_id": 555,
            "activation_revision": "rev-1",
            "source_type": "scheduled",
            "scheduled_for": _dt(2026, 4, 10, 9, 0, 0, tzinfo=timezone.utc),
        }
        request_kwargs.update(overrides.get("request", {}))
        request = _FakeOfflineRequest(**request_kwargs)
        activation = {
            "task_name": "Daily summary",
            "task_description": "Send the daily summary email.",
            "entrypoint": None,
            **overrides.get("activation", {}),
        }
        assistant_data = {
            "assistant_id": "assistant-123",
            "api_key": "key-abc",
            "assistant_first_name": "Ada",
            "assistant_surname": "Lovelace",
            "assistant_age": 35,
            "assistant_nationality": "UK",
            "assistant_timezone": "Europe/London",
            "assistant_about": "I write programs.",
            "assistant_job_title": "Mathematician",
            "assistant_number": "+44-7000000000",
            "assistant_email": "ada@example.com",
            "assistant_whatsapp_number": "+44-7000000001",
            "user_id": "user-7",
            "user_first_name": "Alice",
            "user_surname": "Smith",
            "user_number": "+15555555555",
            "user_email": "alice@example.com",
            "user_whatsapp_number": "+15555555556",
            "voice_provider": "cartesia",
            "voice_id": "voice-xyz",
            "team_ids": [1, 2, 3],
            "org_id": 42,
            **overrides.get("assistant_data", {}),
        }
        return request, activation, assistant_data

    def test_scheduled_attempt_envs_match_field_for_field(self):
        request, activation, assistant_data = self._scenario()
        old = _original_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="offline:scheduled:assistant-123:101:rev:once",
            job_name="unity-offline-abc",
        )
        new = _new_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="offline:scheduled:assistant-123:101:rev:once",
            job_name="unity-offline-abc",
        )
        assert new == old

    def test_triggered_attempt_envs_match_field_for_field(self):
        request, activation, assistant_data = self._scenario(
            request={
                "source_type": "triggered",
                "scheduled_for": None,
                "source_medium": "sms_message",
                "source_ref": "message-xyz",
                "source_contact_id": 77,
            },
            activation={"trigger_medium": "sms_message"},
        )
        old = _original_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="offline:triggered:assistant-123:101:rev:contact-77",
            job_name="unity-offline-xyz",
        )
        new = _new_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="offline:triggered:assistant-123:101:rev:contact-77",
            job_name="unity-offline-xyz",
        )
        assert new == old

    def test_entrypoint_override_envs_match(self):
        request, activation, assistant_data = self._scenario(
            request={"entrypoint": 999},
            activation={"entrypoint": 777},  # activation wins
        )
        old = _original_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="k",
            job_name="j",
        )
        new = _new_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=assistant_data,
            run_key="k",
            job_name="j",
        )
        assert new == old
        assert old["UNITY_OFFLINE_TASK_FUNCTION_ID"] == "777"

    def test_missing_assistant_identity_envs_match(self):
        request, activation, _ = self._scenario()
        sparse = {"assistant_id": "assistant-123"}
        old = _original_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=sparse,
            run_key="k",
            job_name="j",
        )
        new = _new_communication_env_builder(
            request=request,
            activation=activation,
            assistant_data=sparse,
            run_key="k",
            job_name="j",
        )
        assert new == old
