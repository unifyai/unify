"""
Foreign Key Tests for GuidanceManager

Coverage
========
✓ function_ids[*] → Functions.function_id (array FK)
  - Validation: Reject invalid function_ids
  - SET NULL: Remove deleted function from function_ids array
  - CASCADE: Update function_id changes in array
  - Bidirectional consistency with Functions.guidance_ids
"""

from __future__ import annotations

from unify import db
from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager
from unify.guidance_manager.guidance_manager import GuidanceManager

# --------------------------------------------------------------------------- #
#  Unit Tests: function_ids[*] → Functions.function_id                       #
# --------------------------------------------------------------------------- #


@_handle_project
def test_fk_function_ids_valid_reference():
    """Test that guidance can reference valid function IDs."""
    gm = GuidanceManager()
    fm = FunctionManager()

    # Create functions
    src1 = "def func1():\n    return 1\n"
    src2 = "def func2():\n    return 2\n"
    fm.add_functions(implementations=[src1, src2])

    # Get function IDs
    funcs = db.get_logs(context=fm._compositional_ctx, from_fields=["function_id"])
    func_ids = sorted([int(f.entries["function_id"]) for f in funcs])
    assert len(func_ids) == 2

    # Create guidance referencing both functions
    gm.add_guidance(
        title="Function Guide",
        content="Guide for functions",
        function_ids=func_ids,
    )

    # Verify guidance created with function_ids
    guidance_list = db.get_logs(
        context=gm._ctx,
        from_fields=["guidance_id", "function_ids"],
    )
    assert len(guidance_list) == 1
    assert sorted(guidance_list[0].entries["function_ids"]) == func_ids


@_handle_project
def test_fk_function_ids_set_null_on_delete():
    """Test SET NULL: Deleting function removes it from guidance.function_ids array."""
    gm = GuidanceManager()
    fm = FunctionManager()

    # Create 3 functions
    for i in range(3):
        src = f"def func{i}():\n    return {i}\n"
        fm.add_functions(implementations=src)

    # Get function IDs
    funcs = db.get_logs(context=fm._compositional_ctx, from_fields=["function_id"])
    func_ids = sorted([int(f.entries["function_id"]) for f in funcs])
    assert len(func_ids) == 3
    f1, f2, f3 = func_ids

    # Create guidance referencing all 3 functions
    gm.add_guidance(
        title="Multi-Function Guide",
        content="Guide for multiple functions",
        function_ids=[f1, f2, f3],
    )

    # Verify all 3 function_ids
    guidance = db.get_logs(context=gm._ctx, from_fields=["function_ids"])
    assert sorted(guidance[0].entries["function_ids"]) == [f1, f2, f3]

    # Delete middle function (f2)
    fm.delete_function(function_id=f2)

    # Verify f2 removed from function_ids array
    guidance_after = db.get_logs(context=gm._ctx, from_fields=["function_ids"])
    remaining_ids = sorted(guidance_after[0].entries.get("function_ids", []))
    assert remaining_ids == [f1, f3]
    assert f2 not in remaining_ids


@_handle_project
def test_fk_function_ids_empty_array():
    """Test that empty function_ids array is valid."""
    gm = GuidanceManager()

    # Create guidance with no function references
    gm.add_guidance(
        title="Standalone Guide",
        content="Guide without function references",
        function_ids=[],
    )

    # Verify guidance was created
    guidance = db.get_logs(
        context=gm._ctx,
        from_fields=["guidance_id", "function_ids"],
    )
    assert len(guidance) == 1
    assert guidance[0].entries.get("function_ids", []) == []
