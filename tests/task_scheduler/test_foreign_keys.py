"""
Foreign Key Tests for TaskScheduler

Coverage
========
✓ entrypoint → Functions.function_id (direct FK)
  - Valid reference creation
  - SET NULL: Task survives function deletion with entrypoint=null
  - Read after SET NULL without breaking
  - Write with None
  - Clone after SET NULL


"""

from __future__ import annotations

import unisdk
from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager
from unify.task_scheduler.task_scheduler import TaskScheduler

# --------------------------------------------------------------------------- #
#  Unit Tests: entrypoint → Functions.function_id                            #
# --------------------------------------------------------------------------- #


@_handle_project
def test_entrypoint_valid_reference():
    """Test that tasks can reference valid function IDs as entrypoint."""
    fm = FunctionManager()
    ts = TaskScheduler()

    # Create function
    src = "def process_data():\n    return 'processed'\n"
    fm.add_functions(implementations=src)

    # Get function ID
    funcs = unisdk.get_logs(
        context=fm._compositional_ctx,
        from_fields=["function_id", "name"],
    )
    assert len(funcs) == 1
    func_id = int(funcs[0].entries["function_id"])

    # Create task with valid entrypoint
    ts._create_task(
        name="Process Task",
        description="Task that processes data",
        entrypoint=func_id,
    )

    # Verify task was created with entrypoint. Avoid projecting only the FK
    # column in from_fields — nullable/FK projections can omit rows.
    tasks = unisdk.get_logs(
        context=ts._ctx,
        filter="name == 'Process Task'",
    )
    assert len(tasks) == 1
    assert tasks[0].entries["entrypoint"] == func_id


@_handle_project
def test_entrypoint_set_null_on_delete():
    """Test SET NULL: Task survives function deletion with entrypoint becoming null."""
    fm = FunctionManager()
    ts = TaskScheduler()

    # Create function
    src = "def my_func():\n    return 'result'\n"
    fm.add_functions(implementations=src)

    # Get function ID
    funcs = unisdk.get_logs(context=fm._compositional_ctx, from_fields=["function_id"])
    func_id = int(funcs[0].entries["function_id"])

    # Create task with this function as entrypoint
    ts._create_task(
        name="Task 1",
        description="Task with entrypoint",
        entrypoint=func_id,
    )

    # Verify task has entrypoint
    tasks = unisdk.get_logs(context=ts._ctx, from_fields=["task_id", "entrypoint"])
    assert len(tasks) == 1
    assert tasks[0].entries["entrypoint"] == func_id

    # Delete the function
    fm.delete_function(function_id=func_id)

    # Verify task still exists but entrypoint is null (SET NULL behavior)
    tasks_after = unisdk.get_logs(
        context=ts._ctx,
        from_fields=["task_id", "name", "entrypoint"],
    )
    assert len(tasks_after) == 1  # Task still exists
    assert tasks_after[0].entries["name"] == "Task 1"
    assert tasks_after[0].entries.get("entrypoint") is None  # Entrypoint nulled


@_handle_project
def test_entrypoint_null_does_not_break_scheduler_init():
    """Test that tasks with null entrypoint (after FK SET NULL) load without errors."""
    fm = FunctionManager()
    ts = TaskScheduler()

    # Create function
    src = "def my_function():\n    return 'result'\n"
    fm.add_functions(implementations=src)

    # Get function ID
    funcs = unisdk.get_logs(context=fm._compositional_ctx, from_fields=["function_id"])
    func_id = int(funcs[0].entries["function_id"])

    # Create task with entrypoint
    result = ts._create_task(
        name="Task with Entrypoint",
        description="Task that will lose its entrypoint",
        entrypoint=func_id,
    )
    tid = result["details"]["task_id"]

    # Verify task has entrypoint
    tasks = unisdk.get_logs(context=ts._ctx, from_fields=["task_id", "entrypoint"])
    assert len(tasks) == 1
    assert tasks[0].entries["entrypoint"] == func_id

    # Delete function (triggers FK SET NULL)
    fm.delete_function(function_id=func_id)

    # Verify entrypoint is now null
    tasks_after = unisdk.get_logs(
        context=ts._ctx,
        from_fields=["task_id", "entrypoint"],
    )
    assert len(tasks_after) == 1
    assert tasks_after[0].entries.get("entrypoint") is None

    # Create NEW TaskScheduler instance (tests read path on init)
    ts_new = TaskScheduler()

    # Verify the new scheduler can read tasks with null entrypoint without errors
    tasks_from_new = unisdk.get_logs(
        context=ts_new._ctx,
    )
    assert len(tasks_from_new) == 1
    assert tasks_from_new[0].entries["task_id"] == tid
    assert tasks_from_new[0].entries.get("entrypoint") is None

    # Verify Task model construction succeeds (critical test for Orchestra NULL omission)
    from unify.task_scheduler.types.task import Task

    task_dict = tasks_from_new[0].entries
    task_obj = Task(**task_dict)
    assert task_obj.entrypoint is None
    assert task_obj.task_id == tid


@_handle_project
def test_entrypoint_explicit_none_on_create():
    """Test that tasks can be created with entrypoint=None explicitly."""
    ts = TaskScheduler()

    # Create task with explicit None entrypoint
    result = ts._create_task(
        name="Task without Entrypoint",
        description="Task that never had an entrypoint",
        entrypoint=None,
    )
    tid = result["details"]["task_id"]

    # Verify task was created with null entrypoint
    tasks = unisdk.get_logs(
        context=ts._ctx,
    )
    assert len(tasks) == 1
    assert tasks[0].entries["task_id"] == tid
    assert tasks[0].entries.get("entrypoint") is None

    # Verify it can be read back successfully
    from unify.task_scheduler.types.task import Task

    task_dict = tasks[0].entries
    task_obj = Task(**task_dict)
    assert task_obj.entrypoint is None


@_handle_project
def test_entrypoint_clone_after_set_null():
    """Test that cloning a task after its entrypoint is nulled works correctly."""
    fm = FunctionManager()
    ts = TaskScheduler()

    # Create function
    src = "def cloneable():\n    return 'clone me'\n"
    fm.add_functions(implementations=src)
    funcs = unisdk.get_logs(context=fm._compositional_ctx, from_fields=["function_id"])
    func_id = int(funcs[0].entries["function_id"])

    # Create recurring task with entrypoint
    from unify.task_scheduler.types.repetition import RepeatPattern, Frequency

    repeat_pattern = RepeatPattern(frequency=Frequency.DAILY)
    result = ts._create_task(
        name="Recurring Task",
        description="Task that repeats daily",
        entrypoint=func_id,
        repeat=[repeat_pattern],
    )
    tid = result["details"]["task_id"]

    # Verify task has entrypoint
    tasks = unisdk.get_logs(
        context=ts._ctx,
        filter=f"task_id == {tid}",
        from_fields=["task_id", "entrypoint"],
    )
    assert len(tasks) == 1
    assert tasks[0].entries["entrypoint"] == func_id

    # Delete function (triggers FK SET NULL)
    fm.delete_function(function_id=func_id)

    # Verify entrypoint is now null (include task_id to avoid NULL-only field issue)
    tasks_after_delete = unisdk.get_logs(
        context=ts._ctx,
        filter=f"task_id == {tid}",
        from_fields=["task_id", "entrypoint"],
    )
    assert len(tasks_after_delete) == 1
    assert tasks_after_delete[0].entries.get("entrypoint") is None

    # Re-arm the definition after the FK SET NULL and verify entrypoint stays null.
    from unify.task_scheduler.types.task import Task

    task_entries = unisdk.get_logs(
        context=ts._ctx,
        filter=f"task_id == {tid}",
    )[0].entries
    task_obj = Task(**task_entries)

    ts._rearm_task_definition(task_obj)

    all_instances = unisdk.get_logs(
        context=ts._ctx,
        filter=f"task_id == {tid}",
    )
    assert len(all_instances) == 1
    assert all_instances[0].entries.get("entrypoint") is None


@_handle_project
def test_offline_task_can_be_agentic_on_create():
    """Offline delivery can execute agentically when no entrypoint is set."""

    ts = TaskScheduler()

    result = ts._create_task(
        name="Offline task",
        description="Run in the hidden lane without a function id",
        offline=True,
    )

    task = ts._get_task_or_raise(result["details"]["task_id"])
    assert task.offline is True
    assert task.entrypoint is None


@_handle_project
def test_offline_task_can_be_agentic_on_update():
    """Moving a task offline should not require adding a function id."""

    ts = TaskScheduler()
    result = ts._create_task(
        name="Normal task",
        description="Starts in the live lane",
    )
    task_id = result["details"]["task_id"]

    ts._update_task(task_id=task_id, offline=True)

    task = ts._get_task_or_raise(task_id)
    assert task.offline is True
    assert task.entrypoint is None


@_handle_project
def test_create_task_can_start_disabled():
    """Tasks may be created with enabled=False and stay disabled until toggled."""

    ts = TaskScheduler()
    result = ts._create_task(
        name="Paused task",
        description="Created disabled so it will not fire",
        enabled=False,
    )
    task = ts._get_task_or_raise(result["details"]["task_id"])
    assert task.enabled is False


@_handle_project
def test_update_task_toggles_enabled():
    """enabled can be flipped via update without changing other fields."""

    ts = TaskScheduler()
    result = ts._create_task(
        name="Toggleable task",
        description="Starts enabled and can be paused",
    )
    task_id = result["details"]["task_id"]
    assert ts._get_task_or_raise(task_id).enabled is True

    ts._update_task(task_id=task_id, enabled=False)
    assert ts._get_task_or_raise(task_id).enabled is False

    ts._update_task(task_id=task_id, enabled=True)
    assert ts._get_task_or_raise(task_id).enabled is True
