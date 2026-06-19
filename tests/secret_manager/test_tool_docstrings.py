from __future__ import annotations

import os
import sys
import subprocess
import textwrap

from droid.secret_manager.secret_manager import SecretManager
from droid.session_details import UNASSIGNED_USER_CONTEXT, UNASSIGNED_ASSISTANT_CONTEXT
from tests.assertion_helpers import first_diff_block
from tests.helpers import _handle_project


def _unwrap_callable(tool):
    """Return the underlying callable from either a ToolSpec or a function."""
    return getattr(tool, "fn", tool)


@_handle_project
def test_all_ask_tools_have_sufficient_docstrings():
    sm = SecretManager()
    tools = sm.get_tools("ask")

    assert tools, "SecretManager.ask should expose at least one tool"

    for name, value in tools.items():
        fn = _unwrap_callable(value)
        doc = (getattr(fn, "__doc__", None) or "").strip()
        assert doc, f"Tool '{name}' is missing a docstring"
        assert (
            len(doc) >= 100
        ), f"Docstring for tool '{name}' is too short (len={len(doc)})"


def _build_tools_schema_in_subprocess(method: str, test_context: str) -> str:
    """
    Build tools→schema JSON in a fresh Python process to catch cross-session drift.

    The test_context is passed via environment variable to ensure the subprocess
    uses an isolated context rather than the shared default context.

    Round-trip the JSON via a temp file rather than stdout. SecretManager
    init now emits "[integrations] assistant secret sync complete
    reason=secret_manager_init" (added 2026-05-08 in 243b136d65) on every
    instantiation, and that log line goes to stdout with a wall-clock
    timestamp. The cross-session comparison would see the log line at
    index 0 with different timestamps each invocation and fail spuriously.
    Same fix as 51b90d1fb (sibling test_sys_msgs.py).
    """
    assert method in {"ask", "update"}
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="r",
        suffix=".tools_schema.json",
        delete=False,
    ) as out_file:
        out_path = out_file.name
    try:
        code = textwrap.dedent(
            f"""
            import os, sys, json
            sys.path.insert(0, os.getcwd())
            import unify
            # Activate the test project before setting context
            project_name = os.environ.get("DROID_TEST_PROJECT_NAME", "UnityTests")
            unify.activate(project_name, overwrite=False)
            # Set test-specific context before creating SecretManager to avoid races
            test_ctx = os.environ.get("_TEST_CONTEXT")
            if test_ctx:
                unify.set_context(test_ctx, relative=False)
            from droid.common.llm_helpers import method_to_schema
            def _unwrap_callable(tool):
                return getattr(tool, "fn", tool)
            from droid.secret_manager.secret_manager import SecretManager
            sm = SecretManager()
            tools = sm.get_tools("{method}")
            if not tools:
                raise AssertionError("SecretManager.{method} should expose at least one tool")
            mapping = {{
                name: method_to_schema(_unwrap_callable(value), name)
                for name, value in tools.items()
            }}
            out_path = os.environ["_TOOL_SCHEMA_OUT_PATH"]
            with open(out_path, "w", encoding="utf-8") as _f:
                _f.write(json.dumps(mapping, sort_keys=True, indent=2))
            """,
        )
        env = os.environ.copy()
        env["_TEST_CONTEXT"] = test_context
        env["_TOOL_SCHEMA_OUT_PATH"] = out_path
        subprocess.run(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=env,
        )
        with open(out_path, "r", encoding="utf-8") as f:
            return f.read()
    finally:
        try:
            os.unlink(out_path)
        except OSError:
            pass


@_handle_project
def test_all_update_tools_have_sufficient_docstrings():
    sm = SecretManager()
    tools = sm.get_tools("update")

    assert tools, "SecretManager.update should expose at least one tool"

    for name, value in tools.items():
        fn = _unwrap_callable(value)
        doc = (getattr(fn, "__doc__", None) or "").strip()
        assert doc, f"Tool '{name}' is missing a docstring"
        assert (
            len(doc) >= 100
        ), f"Docstring for tool '{name}' is too short (len={len(doc)})"


@_handle_project
def test_secret_write_tools_expose_destination_guidance():
    sm = SecretManager()
    tools = sm.get_tools("update")

    for tool_name in ("create_secret", "update_secret", "delete_secret"):
        fn = _unwrap_callable(tools[tool_name])
        doc = (getattr(fn, "__doc__", None) or "").strip()

        assert "destination : str | None" in doc
        assert "Accessible shared" in doc
        assert "teams" in doc
        assert "team:<id>" in doc
        assert "personal" in doc

    create_doc = (
        getattr(_unwrap_callable(tools["create_secret"]), "__doc__", None) or ""
    )
    assert "sharing a credential is harder to undo" in create_doc
    assert "request_clarification" in create_doc


@_handle_project
def test_ask_tool_schemas_are_stable_across_python_sessions():
    # Build a test-specific context path matching _handle_project pattern
    test_ctx = f"tests/secret_manager/test_tool_docstrings/test_ask_tool_schemas_are_stable_across_python_sessions/{UNASSIGNED_USER_CONTEXT}/{UNASSIGNED_ASSISTANT_CONTEXT}"
    p1 = _build_tools_schema_in_subprocess("ask", test_ctx)
    p2 = _build_tools_schema_in_subprocess("ask", test_ctx)
    if p1 != p2:
        snippet = first_diff_block(
            p1,
            p2,
            context=3,
            label_a="First JSON",
            label_b="Second JSON",
        )
        raise AssertionError(
            "Tool schemas for ask-tools changed between separate Python sessions.\n\n"
            + snippet,
        )


@_handle_project
def test_update_tool_schemas_are_stable_across_python_sessions():
    # Build a test-specific context path matching _handle_project pattern
    test_ctx = f"tests/secret_manager/test_tool_docstrings/test_update_tool_schemas_are_stable_across_python_sessions/{UNASSIGNED_USER_CONTEXT}/{UNASSIGNED_ASSISTANT_CONTEXT}"
    p1 = _build_tools_schema_in_subprocess("update", test_ctx)
    p2 = _build_tools_schema_in_subprocess("update", test_ctx)
    if p1 != p2:
        snippet = first_diff_block(
            p1,
            p2,
            context=3,
            label_a="First JSON",
            label_b="Second JSON",
        )
        raise AssertionError(
            "Tool schemas for update-tools changed between separate Python sessions.\n\n"
            + snippet,
        )
