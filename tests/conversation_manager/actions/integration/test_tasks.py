"""
Task-focused ConversationManager → CodeActActor integration tests.

These tests validate that natural-language task operations routed through CM→Actor:
- perform the expected mutation/read on TaskScheduler
- are robust to clarifications
- persist side effects in the underlying Tasks store
"""

import pytest

from tests.helpers import _handle_project
from tests.conversation_manager.conftest import BOSS
from tests.conversation_manager.actions.integration.helpers import (
    assert_no_errors,
    answer_clarification_and_continue,
    extract_actor_handle,
    find_task_id_by_exact_name,
    find_task_id_by_name_contains,
    get_actor_started_event,
    wait_for_clarification,
    verify_task_in_db,
    wait_for_actor_completion,
)
from unify.conversation_manager.events import SMSReceived

pytestmark = [pytest.mark.integration, pytest.mark.eval]


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_task_create_persists_in_db(initialized_cm_codeact):
    """Create a task via CM→Actor and verify it persisted."""
    cm = initialized_cm_codeact
    task_name = "Review Q3 report (integration)"
    task_desc = "Review the Q3 report and send feedback to the team."

    result = await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content=f"Create a new task named '{task_name}' with description '{task_desc}'.",
        ),
    )

    actor_event = get_actor_started_event(result)
    handle_id = actor_event.handle_id
    _final = await wait_for_actor_completion(cm, handle_id, timeout=300)

    task_id = find_task_id_by_exact_name(name=task_name)
    verify_task_in_db(
        cm,
        task_id,
        expected_fields={
            "name": task_name,
            "description": task_desc,
        },
    )
    assert_no_errors(result)


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_task_lookup_by_name_returns_description(initialized_cm_codeact):
    """Query an existing task via CM→Actor and get back the correct description."""
    cm = initialized_cm_codeact
    import os

    uniq = os.getpid()
    task_name = f"Ping Alice (query smoke {uniq})"
    # Include an unguessable token so the only way to answer correctly is to actually
    # look up the task (not hallucinate from the task name).
    task_desc = f"Send Alice a quick note about the meeting. Ref: TASK-QUERY-{uniq}."

    # Create deterministically out-of-band so CM can't answer from recent chat history.
    from unify.manager_registry import ManagerRegistry

    scheduler = ManagerRegistry.get_task_scheduler()
    assert scheduler is not None, "TaskScheduler is not available"
    scheduler.create_task(name=task_name, description=task_desc)
    task_id = find_task_id_by_exact_name(name=task_name)
    verify_task_in_db(
        cm,
        task_id,
        expected_fields={"name": task_name, "description": task_desc},
    )

    # Now query it.
    result2 = await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content=(
                f"Can you check my task list and tell me the description for the task named '{task_name}'? "
                "Please include the description verbatim."
            ),
        ),
    )
    h2 = get_actor_started_event(result2).handle_id
    handle = extract_actor_handle(cm, h2)

    # Task queries can occasionally trigger a manager clarification (e.g., if multiple
    # plausible matches exist). Make the test robust by answering with the exact name.
    for _ in range(3):
        if handle.done():
            break
        try:
            clar = await wait_for_clarification(handle, timeout=300)
        except Exception:
            break
        await answer_clarification_and_continue(
            handle,
            call_id=clar.call_id,
            answer=task_name,
            timeout=300,
        )

    final = await wait_for_actor_completion(cm, h2, timeout=300)

    lower = final.lower()
    assert (
        "could not" not in lower and "unable" not in lower
    ), f"Actor reported lookup failure instead of returning the task description: {final}"
    assert (
        f"task-query-{uniq}".lower() in lower
    ), f"Expected unguessable ref token from task lookup, got: {final}"
    assert_no_errors(result2)


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_task_update_description_persists(initialized_cm_codeact):
    """Update a task's description via CM→Actor and verify the change persisted.

    Tests two sequential CM→Actor round-trips where the second mutates
    state created by the first.
    """
    import uuid

    cm = initialized_cm_codeact
    cm.cm.vm_ready = True
    cm.cm.file_sync_complete = True
    uniq = uuid.uuid4().hex[:12]
    task_name = f"Close loop with Bob (integration {uniq})"
    original_desc = f"Reply to Bob with the final decision. Ref: TASK-UPD-ORIG-{uniq}."
    updated_desc = (
        f"Reply to Bob with the final decision and attach the Q3 summary. "
        f"Ref: TASK-UPD-NEW-{uniq}."
    )

    result1 = await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content=(
                f"Create a task named '{task_name}' with description '{original_desc}'. "
                "Use that exact task name verbatim."
            ),
        ),
    )
    h1 = get_actor_started_event(result1).handle_id
    final1 = await wait_for_actor_completion(cm, h1, timeout=300)

    # The test driver only processes events inside step_until_wait, so the
    # ActorResult from the completed action sits in the broker unprocessed.
    # Explicitly step the CM with the ActorResult so it moves the action
    # from in_flight_actions to completed_actions before the next message.
    from unify.conversation_manager.events import ActorResult

    await cm.step(
        ActorResult(handle_id=h1, success=True, result=final1),
        run_llm=False,
    )

    result2 = await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content=(
                f"Update the description of the task named '{task_name}' to: "
                f"'{updated_desc}'. Use that exact task name verbatim."
            ),
        ),
    )
    h2 = get_actor_started_event(result2).handle_id
    _ = await wait_for_actor_completion(cm, h2, timeout=300)

    task_id = find_task_id_by_name_contains(name=uniq)
    row = verify_task_in_db(
        cm,
        task_id,
        expected_fields={"description": ...},
    )
    desc = str(row.get("description") or "")
    assert (
        f"TASK-UPD-NEW-{uniq}" in desc
    ), f"Expected updated description token, got: {desc!r}"
    assert_no_errors(result2)


@pytest.mark.asyncio
@pytest.mark.timeout(300)
@_handle_project
async def test_single_message_task_create_then_query(initialized_cm_codeact):
    """
    Single-message multi-step: create a task, then query it to confirm fields.

    Contract: CodeActActor can perform a mutation and then a read-back in one request.
    """
    import os

    cm = initialized_cm_codeact
    uniq = os.getpid()
    task_name = f"Prepare notes for Alice (single msg {uniq})"
    task_desc = f"Draft meeting notes. Ref: TASK-CREATE-QUERY-{uniq}."

    result = await cm.step_until_wait(
        SMSReceived(
            contact=BOSS,
            content=(
                f"Create a task named '{task_name}' with description '{task_desc}'. "
                "Use that exact task name verbatim (including the parentheses). "
                "Then check the task list and tell me the task's status and description."
            ),
        ),
    )
    handle_id = get_actor_started_event(result).handle_id
    final = await wait_for_actor_completion(cm, handle_id, timeout=300)

    # Side-effect verification: task exists with the unguessable description token.
    # Lookup by description token (not exact name) so a dropped parenthetical suffix
    # does not hide a successful create+query.
    from unify.manager_registry import ManagerRegistry

    scheduler = ManagerRegistry.get_task_scheduler()
    assert scheduler is not None, "TaskScheduler is not available"
    store = getattr(scheduler, "_store", None)
    assert store is not None, "TaskScheduler missing _store"
    token = f"TASK-CREATE-QUERY-{uniq}"
    rows = store.get_rows(
        limit=50,
        include_fields=["task_id", "name", "description", "status"],
    )
    match = next(
        (
            r
            for r in rows or []
            if token
            in str((getattr(r, "entries", None) or {}).get("description") or "")
        ),
        None,
    )
    assert match is not None, f"Expected a task whose description contains {token!r}"
    task_id = int((match.entries or {}).get("task_id"))
    row = verify_task_in_db(
        cm,
        task_id,
        expected_fields={"description": ...},
    )
    desc = str(row.get("description") or "")
    name = str(row.get("name") or "")
    assert token in desc, f"Expected description to contain {token!r}, got: {desc!r}"
    assert (
        "Prepare notes for Alice" in name
    ), f"Expected task name to reference Alice notes, got: {name!r}"

    # Response verification: must include the ref token (stable).
    assert f"task-create-query-{uniq}".lower() in final.lower()
    assert_no_errors(result)
