import hashlib

from unify.task_scheduler import machine_state
from unify.task_scheduler.machine_state import (
    TASK_MACHINE_STATE_PROJECT,
    TaskExecutionSnapshot,
    TaskOutboundOperationProvenance,
    TaskRunProvenance,
    build_task_executions_context_name,
    build_task_outbound_operation_key,
    build_task_run_key,
    create_or_adopt_task_outbound_operation,
    create_or_adopt_live_task_run,
    consume_live_task_run_provenance,
    get_open_task_execution,
    remember_live_task_run_provenance,
    validate_task_due_execution,
)
from unify.task_scheduler.types.execution import Delivery, Wake


def _scheduled_execution(**overrides) -> TaskExecutionSnapshot:
    base = dict(
        run_key="live:scheduled:42:101:rev-digest:once",
        assistant_id="42",
        task_id=101,
        source_task_log_id=555,
        wake=Wake.scheduled.value,
        delivery=Delivery.live.value,
        scheduled_for="2026-04-10T09:00:00+00:00",
        revision="rev-1",
    )
    base.update(overrides)
    return TaskExecutionSnapshot(**base)


def test_validate_task_due_execution_accepts_current_execution(monkeypatch):
    execution = _scheduled_execution(
        scheduled_for="2026-04-10T10:00:00+01:00",
    )
    monkeypatch.setattr(
        machine_state,
        "get_open_task_execution",
        lambda **_: execution,
    )

    current_execution, stale_reason = validate_task_due_execution(
        assistant_id="42",
        task_id=101,
        revision="rev-1",
        source_task_log_id=555,
        scheduled_for="2026-04-10T09:00:00+00:00",
    )

    assert current_execution == execution
    assert stale_reason is None


def test_validate_task_due_execution_rejects_revision_mismatch(monkeypatch):
    execution = _scheduled_execution()
    monkeypatch.setattr(
        machine_state,
        "get_open_task_execution",
        lambda **_: execution,
    )

    current_execution, stale_reason = validate_task_due_execution(
        assistant_id="42",
        task_id=101,
        revision="rev-stale",
        source_task_log_id=555,
        scheduled_for="2026-04-10T09:00:00+00:00",
    )

    assert current_execution is None
    assert stale_reason == "revision_mismatch"


def test_validate_task_due_execution_rejects_invalid_destination():
    current_execution, stale_reason = validate_task_due_execution(
        assistant_id="42",
        task_id=101,
        revision="rev-1",
        source_task_log_id=555,
        scheduled_for="2026-04-10T09:00:00+00:00",
        destination="org_default",
    )

    assert current_execution is None
    assert stale_reason == "invalid_destination"


def test_validate_task_due_execution_rejects_offline_execution(monkeypatch):
    execution = _scheduled_execution(delivery=Delivery.offline.value)
    monkeypatch.setattr(
        machine_state,
        "get_open_task_execution",
        lambda **_: execution,
    )

    current_execution, stale_reason = validate_task_due_execution(
        assistant_id="42",
        task_id=101,
        revision="rev-1",
        source_task_log_id=555,
        scheduled_for="2026-04-10T09:00:00+00:00",
    )

    assert current_execution is None
    assert stale_reason == "delivery_changed"


def test_validate_task_due_execution_rejects_departed_space(monkeypatch):
    execution = _scheduled_execution(destination="team:7")
    monkeypatch.setattr(
        machine_state,
        "get_open_task_execution",
        lambda **_: execution,
    )
    monkeypatch.setattr(machine_state.SESSION_DETAILS, "team_ids", [8])

    current_execution, stale_reason = validate_task_due_execution(
        assistant_id="42",
        task_id=101,
        revision="rev-1",
        source_task_log_id=555,
        scheduled_for="2026-04-10T09:00:00+00:00",
        destination="team:7",
    )

    assert current_execution is None
    assert stale_reason == "destination_membership_revoked"


def test_get_open_task_execution_queries_open_scheduled_row(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_get_logs(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr("unify.task_scheduler.storage.unisdk.get_logs", _fake_get_logs)
    monkeypatch.setattr(machine_state.SESSION_DETAILS.user, "id", "user-1")
    monkeypatch.setattr(machine_state.SESSION_DETAILS.assistant, "agent_id", 2069)

    execution = get_open_task_execution(
        assistant_id="2069",
        task_id=0,
        destination=None,
        wake=Wake.scheduled,
    )

    assert execution is None
    assert "task_id == 0" in str(captured["filter"])
    assert "wake == 'scheduled'" in str(captured["filter"])
    assert captured["project"] == TASK_MACHINE_STATE_PROJECT
    assert captured["context"] == build_task_executions_context_name(
        user_context="user-1",
        assistant_context="2069",
    )


def test_get_open_task_execution_skips_query_for_invalid_destination():
    execution = get_open_task_execution(
        assistant_id="2069",
        task_id=0,
        destination="org_default",
    )

    assert execution is None


def test_get_open_task_execution_queries_assistants_machine_state_project(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeRow:
        entries = {
            "assistant_id": "42",
            "run_key": "live:scheduled:42:team-7:101:rev:once",
            "task_id": 101,
            "destination": "team:7",
            "wake": "scheduled",
            "delivery": "live",
            "revision": "rev-1",
            "state": "scheduled",
        }

    def _fake_get_logs(**kwargs):
        captured.update(kwargs)
        return [_FakeRow()]

    monkeypatch.setattr("unify.task_scheduler.storage.unisdk.get_logs", _fake_get_logs)
    monkeypatch.setattr(machine_state.SESSION_DETAILS.user, "id", "user-1")
    monkeypatch.setattr(machine_state.SESSION_DETAILS.assistant, "agent_id", 42)

    execution = get_open_task_execution(
        assistant_id="42",
        task_id=101,
        destination="team:7",
        wake=Wake.scheduled,
    )

    assert execution is not None
    assert execution.task_id == 101
    assert "destination == 'team:7'" in str(captured["filter"])
    assert captured["project"] == TASK_MACHINE_STATE_PROJECT
    assert captured["context"] == build_task_executions_context_name(
        user_context="user-1",
        assistant_context="42",
    )


def test_trigger_provenance_keeps_attempts_separate(monkeypatch):
    monkeypatch.setattr(machine_state, "_PENDING_LIVE_TASK_RUNS", {})
    monkeypatch.setattr(machine_state, "_PENDING_TRIGGER_LIVE_TASK_RUNS", {})

    execution = TaskExecutionSnapshot(
        run_key="live:triggered:42:301:rev:once",
        assistant_id="42",
        task_id=301,
        source_task_log_id=555,
        wake=Wake.triggered.value,
        delivery=Delivery.live.value,
        trigger_medium="sms_message",
        revision="rev-1",
    )
    monkeypatch.setattr(
        machine_state,
        "get_open_task_execution",
        lambda **_: execution,
    )

    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=301,
            wake=Wake.triggered,
            source_medium="sms_message",
            source_ref="message-1",
            source_contact_id="2",
            attempt_token="attempt-a",
        ),
    )
    remember_live_task_run_provenance(
        TaskRunProvenance(
            assistant_id="42",
            task_id=301,
            wake=Wake.triggered,
            source_medium="sms_message",
            source_ref="message-2",
            source_contact_id="2",
            attempt_token="attempt-b",
        ),
    )

    first = consume_live_task_run_provenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        trigger_attempt_token="attempt-a",
    )
    second = consume_live_task_run_provenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        trigger_attempt_token="attempt-b",
    )
    fallback = consume_live_task_run_provenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
    )

    assert first is not None
    assert first.source_ref == "message-1"
    assert second is not None
    assert second.source_ref == "message-2"
    assert fallback is not None
    assert fallback.source_ref is None
    assert fallback.source_contact_id is None
    assert fallback.source_medium == "sms_message"


def test_build_task_run_key_ignores_trigger_attempt_token():
    revision_digest = hashlib.sha256(b"rev-1").hexdigest()[:12]
    source_ref_digest = hashlib.sha256(b"message-1").hexdigest()[:12]
    with_attempt = TaskRunProvenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        delivery=Delivery.live,
        revision="rev-1",
        source_medium="sms_message",
        source_ref="message-1",
        source_contact_id="2",
        attempt_token="attempt-a",
    )
    without_attempt = TaskRunProvenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        delivery=Delivery.live,
        revision="rev-1",
        source_medium="sms_message",
        source_ref="message-1",
        source_contact_id="2",
    )

    expected = (
        f"live:triggered:42:301:{revision_digest}:"
        f"contact-2-sms-message-{source_ref_digest}"
    )

    assert build_task_run_key(with_attempt) == expected
    assert build_task_run_key(without_attempt) == expected

    with_attempt = TaskRunProvenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        delivery=Delivery.live,
        revision="rev-1",
        destination="team:7",
        source_medium="sms_message",
        source_ref="message-1",
        source_contact_id="2",
        attempt_token="attempt-a",
    )
    expected = (
        f"live:triggered:42:team-7:301:{revision_digest}:"
        f"contact-2-sms-message-{source_ref_digest}"
    )
    assert build_task_run_key(with_attempt) == expected


def test_create_or_adopt_live_task_run_persists_display_fields(monkeypatch):
    captured: dict[str, object] = {}
    provenance = TaskRunProvenance(
        assistant_id="42",
        task_id=301,
        wake=Wake.triggered,
        delivery=Delivery.live,
        revision="rev-1",
        source_medium="sms_message",
        source_ref="message-1",
        source_contact_id="2",
        source_contact_display_name="Alice Owner",
        task_name="Follow up on invoice",
        task_description="Ask Alice for the missing invoice details.",
    )

    def _fake_post(path: str, payload: dict[str, object]):
        captured["path"] = path
        captured["payload"] = payload
        return {"run": {"run_key": "live:triggered:42:301:rev-1:once"}}

    monkeypatch.setattr(machine_state, "_orchestra_admin_post", _fake_post)

    reference = create_or_adopt_live_task_run(
        provenance,
        started_at="2026-04-16T10:15:00+00:00",
    )

    assert reference is not None
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["source_contact_display_name"] == "Alice Owner"
    assert payload["task_name"] == "Follow up on invoice"
    assert payload["task_description"] == "Ask Alice for the missing invoice details."
    assert payload["started_at"] == "2026-04-16T10:15:00+00:00"
    assert payload["wake"] == "triggered"
    assert payload["delivery"] == "live"
    assert payload["revision"] == "rev-1"


def test_build_task_outbound_operation_key_is_stable_for_same_target():
    provenance = TaskOutboundOperationProvenance(
        assistant_id="42",
        task_run_key="offline:scheduled:42:301:rev:once",
        operation_index=2,
        method_name="send_sms",
        medium="sms_message",
        target_kind="contact",
        contact_id=7,
        target_metadata={"phone_number": "+15555550123"},
    )
    equivalent = TaskOutboundOperationProvenance(
        assistant_id="42",
        task_run_key="offline:scheduled:42:301:rev:once",
        operation_index=2,
        method_name="send_sms",
        medium="sms_message",
        target_kind="contact",
        contact_id=7,
        target_metadata={"phone_number": "+15555550123"},
    )

    assert build_task_outbound_operation_key(
        provenance,
    ) == build_task_outbound_operation_key(
        equivalent,
    )


def test_create_or_adopt_task_outbound_operation_persists_target_metadata(monkeypatch):
    captured: dict[str, object] = {}
    provenance = TaskOutboundOperationProvenance(
        assistant_id="42",
        task_run_key="offline:scheduled:42:301:rev:once",
        operation_index=1,
        method_name="send_email",
        medium="email",
        target_kind="contact",
        contact_id=17,
        task_id=301,
        source_task_log_id=555,
        target_metadata={
            "email_address": "alice@example.com",
            "display_name": "Alice Owner",
        },
    )

    def _fake_post(path: str, payload: dict[str, object]):
        captured["path"] = path
        captured["payload"] = payload
        return {
            "created": True,
            "operation": {
                "operation_key": payload["operation_key"],
                "status": "pending",
            },
        }

    monkeypatch.setattr(machine_state, "_orchestra_admin_post", _fake_post)

    record = create_or_adopt_task_outbound_operation(
        provenance,
        created_at="2026-04-16T10:15:00+00:00",
    )

    assert record is not None
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["task_run_key"] == provenance.task_run_key
    assert payload["contact_id"] == 17
    assert payload["target_metadata"] == {
        "email_address": "alice@example.com",
        "display_name": "Alice Owner",
    }
    assert payload["created_at"] == "2026-04-16T10:15:00+00:00"


def test_update_task_outbound_operation_record_posts_partial_updates(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_post(path: str, payload: dict[str, object]):
        captured["path"] = path
        captured["payload"] = payload
        return {"operation": {"operation_key": "offline:op:1"}}

    monkeypatch.setattr(machine_state, "_orchestra_admin_post", _fake_post)

    machine_state.update_task_outbound_operation_record(
        machine_state.TaskOutboundOperationReference(
            assistant_id="42",
            operation_key="offline:op:1",
        ),
        {
            "status": "completed",
            "provider_message_id": "msg-123",
            "history_message_id": 9,
        },
    )

    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["assistant_id"] == "42"
    assert payload["operation_key"] == "offline:op:1"
    assert payload["updates"] == {
        "status": "completed",
        "provider_message_id": "msg-123",
        "history_message_id": 9,
    }


def test_task_machine_contexts_route_to_owner_team_for_team_owned():
    """Team-owned assistants keep task-machine state on the team Tasks tree."""
    from unify.session_details import SESSION_DETAILS

    original_owner = SESSION_DETAILS.owner_team_id
    SESSION_DETAILS.owner_team_id = 5
    try:
        assert build_task_executions_context_name() == "Teams/5/Tasks/Executions"
    finally:
        SESSION_DETAILS.owner_team_id = original_owner

    explicit = build_task_executions_context_name(
        user_context="user123",
        assistant_context="42",
    )
    assert explicit == "user123/42/Tasks/Executions"
