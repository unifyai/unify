from datetime import datetime, timedelta, timezone

from tests.helpers import _handle_project
import pytest
import unify

from droid.common.context_registry import ContextRegistry
from droid.common.tool_outcome import ToolErrorException
from droid.session_details import SESSION_DETAILS
from droid.task_scheduler.task_scheduler import TaskScheduler
from droid.task_scheduler.types.priority import Priority
from droid.task_scheduler.types.repetition import Frequency, RepeatPattern
from droid.task_scheduler.types.schedule import Schedule
from droid.task_scheduler.types.status import Status


@_handle_project
def test_create_task():
    task_scheduler = TaskScheduler()
    task_scheduler._create_task(
        name="Promote Jeff Smith",
        description="Send an email to Jeff Smith, kindly congratulating him and explaining that he has been promoted from sales rep to sales manager.",
        entrypoint=101,
    )
    task_list = task_scheduler._filter_tasks()
    assert len(task_list) == 1
    row = task_list[0]
    # After refactor, _filter_tasks returns raw JSON-serialisable values (enums as strings)
    assert row.name == "Promote Jeff Smith"
    assert (
        row.description
        == "Send an email to Jeff Smith, kindly congratulating him and explaining that he has been promoted from sales rep to sales manager."
    )
    assert row.status == Status.scheduled
    assert row.trigger is None
    assert row.schedule is None
    assert row.deadline is None
    assert row.repeat is None
    assert row.priority == Priority.normal
    assert row.task_id == 0
    assert row.instance_id == 0
    assert row.response_policy is None
    assert row.entrypoint == 101
    assert row.activated_by is None


@_handle_project
def test_delete_task():
    task_scheduler = TaskScheduler()

    # create
    task_scheduler._create_task(
        name="Promote Jeff Smith",
        description="Send an email to Jeff Smith, kindly congratulating him and explaining that he has been promoted from sales rep to sales manager.",
    )
    task_list = task_scheduler._filter_tasks()
    assert len(task_list) == 1

    # delete
    task_scheduler._delete_task(task_id=0)
    task_list = task_scheduler._filter_tasks()
    assert task_list == []


@_handle_project
def test_create_task_with_response_policy():
    ts = TaskScheduler()

    policy = (
        "During this task, only the project manager may issue instructions. "
        "The client may view progress updates but cannot steer decisions."
    )

    ts._create_task(
        name="PM-only control",
        description="Carry out project setup steps.",
        response_policy=policy,
    )

    rows = ts._filter_tasks()
    assert len(rows) == 1
    assert rows[0].response_policy == policy


@_handle_project
def test_create_team_task_routes_to_shared_root():
    ts = TaskScheduler()
    SESSION_DETAILS.team_ids = [987654]

    try:
        out = ts._create_task(
            name="Shared follow-up",
            description="Coordinate with the shared project room.",
            destination="team:987654",
        )
        task_id = out["details"]["task_id"]

        rows = ts._filter_tasks(filter=f"task_id == {task_id}")
        assert len(rows) == 1
        assert rows[0].destination == "team:987654"
        assert rows[0].assistant_id == SESSION_DETAILS.assistant_context
    finally:
        try:
            unify.delete_context("Teams/987654/Tasks")
        except Exception:
            pass
        SESSION_DETAILS.team_ids = []
        ContextRegistry.forget_departed_team_roots([])


@_handle_project
def test_invalid_space_destination_raises_tool_error():
    ts = TaskScheduler()
    SESSION_DETAILS.team_ids = [987655]

    try:
        with pytest.raises(ToolErrorException):
            ts._create_task(
                name="Bad shared task",
                description="Should not write outside membership.",
                destination="team:987656",
            )
    finally:
        SESSION_DETAILS.team_ids = []


@_handle_project
def test_clone_recurring_task_instance_uses_space_destination_root():
    """Re-arming a recurring task must clone into the same Tasks root as the template row."""
    ts = TaskScheduler()
    team_id = 987658
    SESSION_DETAILS.team_ids = [team_id]

    try:
        initial_start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
            hours=1,
        )
        out = ts._create_task(
            name="Daily shared alert",
            description="Check KPI thresholds for the patch team.",
            status=Status.scheduled,
            schedule=Schedule(start_at=initial_start.isoformat()),
            repeat=[RepeatPattern(frequency=Frequency.DAILY)],
            destination=f"team:{team_id}",
        )
        task_id = out["details"]["task_id"]
        current = ts._filter_tasks(filter=f"task_id == {task_id}")[0]
        assert current.destination == f"team:{team_id}"

        ts._clone_task_instance(current)

        rows = ts._filter_tasks(filter=f"task_id == {task_id}")
        assert len(rows) == 2
        assert {row.instance_id for row in rows} == {0, 1}
        assert all(row.destination == f"team:{team_id}" for row in rows)
    finally:
        try:
            unify.delete_context(f"Teams/{team_id}/Tasks")
        except Exception:
            pass
        SESSION_DETAILS.team_ids = []
        ContextRegistry.forget_departed_team_roots([])


@_handle_project
def test_duplicate_task_id_update_requires_destination():
    ts = TaskScheduler()
    SESSION_DETAILS.team_ids = [987657]

    try:
        ts._create_task(name="Personal duplicate", description="Personal root.")
        ts._create_task(
            name="Shared duplicate",
            description="Shared root.",
            destination="team:987657",
        )

        with pytest.raises(ValueError, match="provide destination"):
            ts._update_task(task_id=0, name="Ambiguous update")

        ts._update_task(
            task_id=0,
            name="Shared duplicate updated",
            destination="team:987657",
        )
        rows = ts._filter_tasks(filter="task_id == 0")
        by_destination = {row.destination or "personal": row.name for row in rows}
        assert by_destination["personal"] == "Personal duplicate"
        assert by_destination["team:987657"] == "Shared duplicate updated"
    finally:
        try:
            unify.delete_context("Teams/987657/Tasks")
        except Exception:
            pass
        SESSION_DETAILS.team_ids = []
        ContextRegistry.forget_departed_team_roots([])


@_handle_project
def test_create_tasks_batch_returns_ordered_ids():
    ts = TaskScheduler()

    out = ts._create_tasks(
        tasks=[
            {"name": "A", "description": "a"},
            {"name": "B", "description": "b"},
            {"name": "C", "description": "c"},
        ],
    )

    assert out["details"]["task_ids"] == [0, 1, 2]
    rows = sorted(ts._filter_tasks(), key=lambda task: task.task_id)
    assert [row.name for row in rows] == ["A", "B", "C"]
    assert all(row.status == Status.scheduled for row in rows)


@_handle_project
def test_create_tasks_preserves_offline_delivery_flag():
    ts = TaskScheduler()

    out = ts._create_tasks(
        tasks=[
            {"name": "Offline A", "description": "a", "offline": True},
            {"name": "Offline B", "description": "b", "offline": True},
        ],
    )

    assert out["details"]["task_ids"] == [0, 1]
    rows = sorted(ts._filter_tasks(), key=lambda task: task.task_id)
    assert [row.offline for row in rows] == [True, True]
    assert [row.entrypoint for row in rows] == [None, None]
    assert rows[0].status == Status.scheduled


@_handle_project
def test_task_scheduler_clear():
    ts = TaskScheduler()

    # Seed a couple of tasks
    out1 = ts._create_task(name="Alpha", description="alpha desc")
    out2 = ts._create_task(name="Beta", description="beta desc")
    id1 = out1["details"]["task_id"]
    id2 = out2["details"]["task_id"]
    # Fresh contexts start from 0 and increment
    assert id1 == 0 and id2 == 1

    # Sanity: tasks present before clear
    before = ts._filter_tasks()
    assert before and len(before) == 2

    # Execute clear
    ts.clear()

    # After clear: no tasks
    after = ts._filter_tasks()
    assert after == []

    # Creating again should work and ids should restart from 0
    out3 = ts._create_task(name="Gamma", description="gamma desc")
    assert out3["details"]["task_id"] == 0
