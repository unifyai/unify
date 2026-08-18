"""
Tests for the derived 'verify' flag on stored functions.

``verify`` is never authored: it is derived from the verification ledger by
the trust policy. A freshly stored function is untrusted, and rewriting its
implementation puts it back on the ramp.
"""

import inspect

from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager


def test_add_functions_has_no_verify_parameter():
    """Trust cannot be granted through the storage API."""
    assert "verify" not in inspect.signature(FunctionManager.add_functions).parameters


@_handle_project
def test_add_default_verify():
    """A newly stored function is untrusted."""
    fm = FunctionManager()
    src = "def default_verify_func():\n    pass\n"
    fm.add_functions(implementations=src)

    listing = fm.list_functions()
    assert "default_verify_func" in listing
    assert listing["default_verify_func"]["verify"] is True


@_handle_project
def test_overwrite_keeps_function_untrusted():
    """Rewriting an implementation never grants trust."""
    fm = FunctionManager()
    src = "def update_verify_func():\n    pass\n"

    fm.add_functions(implementations=src)
    assert fm.list_functions()["update_verify_func"]["verify"] is True

    fm.add_functions(
        implementations="def update_verify_func():\n    return 1\n",
        overwrite=True,
    )
    row = fm.filter_functions(filter="name == 'update_verify_func'")[0]
    assert row["verify"] is True
    assert row["side_effect_class"] == "safe_noop"
