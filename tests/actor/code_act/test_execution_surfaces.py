"""Symbolic tests for the execution-surface abstraction.

These exercise the surface resolver, the target factory, the LocalTarget, the
normalized ExecResult, and the actor's ``_execute_on_surface`` routing — all
without an LLM, a real desktop, or a real sandbox. They guard the programmatic
contract that lets ``execute_code`` run on local, assistant-desktop, or
user-desktop surfaces.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from droid.actor.code_act_actor import CodeActActor
from droid.actor.execution.surface import (
    ExecutionSurface,
    resolve_all,
    resolve_surface,
)
from droid.actor.execution.targets import (
    ExecResult,
    LocalTarget,
    TargetUnavailableError,
    get_target,
)
from droid.actor.execution.targets.base import coerce_output
from droid.session_details import SESSION_DETAILS

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeExecutor:
    """Records ``execute`` calls and returns a canned result dict."""

    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._result = result or {
            "stdout": "",
            "stderr": "",
            "result": None,
            "error": None,
        }

    async def execute(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return dict(self._result)


class _Dummy:
    """Minimal stand-in for ``self`` in ``CodeActActor._execute_on_surface``."""

    def __init__(self, session_executor: Any = None, function_manager: Any = None):
        self._session_executor = session_executor
        self.function_manager = function_manager


@pytest.fixture
def no_desktops(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force a session with no managed VM and no linked user desktops."""
    monkeypatch.setattr(SESSION_DETAILS.assistant, "desktop_url", None)
    monkeypatch.setattr(SESSION_DETAILS.assistant, "user_desktops", {})


# ---------------------------------------------------------------------------
# coerce_output / ExecResult
# ---------------------------------------------------------------------------


def test_coerce_output_handles_none_str_parts_and_other() -> None:
    class _Part:
        def __init__(self, text: str | None) -> None:
            self.text = text

    assert coerce_output(None) == ""
    assert coerce_output("hi") == "hi"
    assert coerce_output([_Part("a"), _Part(None), _Part("b")]) == "ab"
    assert coerce_output(7) == "7"


def test_exec_result_ok_semantics() -> None:
    assert ExecResult(surface=ExecutionSurface.LOCAL, returncode=0).ok is True
    assert ExecResult(surface=ExecutionSurface.LOCAL, returncode=None).ok is True
    assert ExecResult(surface=ExecutionSurface.LOCAL, returncode=2).ok is False
    assert ExecResult(surface=ExecutionSurface.LOCAL, error="boom").ok is False


def test_exec_result_to_dict_and_from_agent_payload() -> None:
    d = ExecResult(
        surface=ExecutionSurface.ASSISTANT_DESKTOP,
        stdout="x",
        returncode=0,
    ).to_dict()
    assert d["surface"] == "assistant_desktop"
    assert d["stdout"] == "x"
    assert d["returncode"] == 0

    res = ExecResult.from_agent_payload(
        {"stdout": "out", "stderr": None, "exitCode": 0},
        ExecutionSurface.USER_DESKTOP,
    )
    assert res.surface is ExecutionSurface.USER_DESKTOP
    assert res.stdout == "out"
    assert res.stderr == ""
    assert res.returncode == 0


# ---------------------------------------------------------------------------
# Surface resolver
# ---------------------------------------------------------------------------


def test_local_surface_always_available_and_ready() -> None:
    caps = resolve_surface(ExecutionSurface.LOCAL)
    assert caps.available and caps.ready
    assert caps.can_python and caps.can_shell and caps.can_files


def test_resolve_all_marks_remote_surfaces_unavailable_without_desktops(
    no_desktops: None,
) -> None:
    caps = resolve_all()
    assert set(caps) == set(ExecutionSurface)
    assert caps[ExecutionSurface.LOCAL].available is True
    assert caps[ExecutionSurface.ASSISTANT_DESKTOP].available is False
    assert caps[ExecutionSurface.USER_DESKTOP].available is False
    assert caps[ExecutionSurface.USER_DESKTOP].reason


def test_unknown_surface_value_raises() -> None:
    with pytest.raises(ValueError):
        ExecutionSurface("nope")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_factory_builds_local_target() -> None:
    target = get_target(
        ExecutionSurface.LOCAL,
        session_executor=_FakeExecutor(),
    )
    assert isinstance(target, LocalTarget)
    assert target.surface is ExecutionSurface.LOCAL


def test_factory_local_requires_executor() -> None:
    with pytest.raises(ValueError):
        get_target(ExecutionSurface.LOCAL, session_executor=None)


def test_factory_user_desktop_unavailable_raises(no_desktops: None) -> None:
    with pytest.raises(TargetUnavailableError):
        get_target(ExecutionSurface.USER_DESKTOP)


def test_factory_assistant_desktop_unavailable_raises(no_desktops: None) -> None:
    with pytest.raises(TargetUnavailableError):
        get_target(
            ExecutionSurface.ASSISTANT_DESKTOP,
            function_manager=object(),
        )


# ---------------------------------------------------------------------------
# LocalTarget
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_target_run_python_is_stateless_and_coerces_output() -> None:
    executor = _FakeExecutor(
        {"stdout": "hello\n", "stderr": "", "result": 42, "error": None},
    )
    target = LocalTarget(executor)
    res = await target.run_python("print('hello')")

    assert res.surface is ExecutionSurface.LOCAL
    assert res.stdout == "hello\n"
    assert res.result == 42
    call = executor.calls[-1]
    assert call["language"] == "python"
    assert call["state_mode"] == "stateless"
    assert call["session_id"] is None


@pytest.mark.asyncio
async def test_local_target_run_shell_prefixes_cwd_and_maps_returncode() -> None:
    executor = _FakeExecutor(
        {"stdout": "out", "stderr": "", "result": 0, "error": None},
    )
    target = LocalTarget(executor)
    res = await target.run_shell("ls", cwd="/tmp/some dir")

    assert res.returncode == 0
    assert res.stdout == "out"
    code = executor.calls[-1]["code"]
    assert code.startswith("cd ")
    assert "ls" in code
    assert executor.calls[-1]["state_mode"] == "stateless"


@pytest.mark.asyncio
async def test_local_target_put_and_get_file_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "root"
    src = tmp_path / "src.txt"
    src.write_text("payload")
    target = LocalTarget(_FakeExecutor(), local_root=root)

    await target.put_file(src, "nested/dest.txt")
    assert (root / "nested" / "dest.txt").read_text() == "payload"

    back = tmp_path / "back.txt"
    await target.get_file("nested/dest.txt", back)
    assert back.read_text() == "payload"


# ---------------------------------------------------------------------------
# Actor routing: _execute_on_surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_on_surface_rejects_unknown_surface() -> None:
    out = await CodeActActor._execute_on_surface(
        _Dummy(),
        surface_name="mainframe",
        code="print(1)",
        language="python",
        state_mode="stateless",
        session_id=None,
        session_name=None,
        venv_id=None,
        user_id=None,
    )
    assert out["error"] and "Unknown surface" in out["error"]
    assert out["surface"] == "mainframe"


@pytest.mark.asyncio
async def test_execute_on_surface_rejects_stateful_request() -> None:
    out = await CodeActActor._execute_on_surface(
        _Dummy(),
        surface_name="user_desktop",
        code="print(1)",
        language="python",
        state_mode="stateful",
        session_id=None,
        session_name=None,
        venv_id=None,
        user_id=None,
    )
    assert out["error"] and "stateless" in out["error"]


@pytest.mark.asyncio
async def test_execute_on_surface_rejects_session_and_venv_params() -> None:
    out = await CodeActActor._execute_on_surface(
        _Dummy(),
        surface_name="assistant_desktop",
        code="print(1)",
        language="python",
        state_mode="stateless",
        session_id=7,
        session_name=None,
        venv_id=None,
        user_id=None,
    )
    assert out["error"] and "stateless" in out["error"]


@pytest.mark.asyncio
async def test_execute_on_surface_gates_unavailable_user_desktop(
    no_desktops: None,
) -> None:
    out = await CodeActActor._execute_on_surface(
        _Dummy(session_executor=_FakeExecutor(), function_manager=object()),
        surface_name="user_desktop",
        code="print(1)",
        language="python",
        state_mode="stateless",
        session_id=None,
        session_name=None,
        venv_id=None,
        user_id=None,
    )
    assert out["error"]
    assert out["suggestion"]
    assert out["surface"] == "user_desktop"
    assert out["stdout"] == ""
