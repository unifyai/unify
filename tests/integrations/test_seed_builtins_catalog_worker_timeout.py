"""The direct-worker executor must be bounded and must not hide its progress.

An unbounded ``subprocess.run`` blocked a self-host ``stack.sh up`` in
``communicate()`` indefinitely, with the worker's output held in a pipe so
nothing indicated what it was waiting on.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest


def _seed_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "seed_builtins_catalog.py"
    spec = importlib.util.spec_from_file_location("seed_builtins_catalog", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_command_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A worker that never exits must raise, not block forever."""
    module = _seed_module()
    monkeypatch.setenv("UNIFY_INTEGRATION_BOOTSTRAP_WORKER_TIMEOUT", "1")

    with pytest.raises(RuntimeError, match="timed out after 1s"):
        module._run_json_command(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            request_payload={"backend_id": "composio"},
        )


def test_timeout_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _seed_module()
    monkeypatch.setenv("UNIFY_INTEGRATION_BOOTSTRAP_WORKER_TIMEOUT", "7")
    seen: dict[str, float | None] = {}

    def fake_run(*_args: object, **kwargs: object):
        seen["timeout"] = kwargs.get("timeout")
        raise subprocess.TimeoutExpired(cmd="worker", timeout=7)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError):
        module._run_json_command(["worker"], request_payload={})

    assert seen["timeout"] == 7.0


def test_worker_stderr_is_not_captured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Streaming stderr keeps the sync's progress visible while it runs."""
    module = _seed_module()
    seen: dict[str, object] = {}

    def fake_run(*_args: object, **kwargs: object):
        seen.update(kwargs)
        return subprocess.CompletedProcess(
            args=["worker"],
            returncode=0,
            stdout='{"status": "success"}',
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    assert module._run_json_command(["worker"], request_payload={}) == {
        "status": "success",
    }
    assert seen.get("capture_output") is None
    assert seen.get("stderr") is None
    assert seen.get("stdout") is subprocess.PIPE


def test_failure_without_captured_stderr_still_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`completed.stderr` is None now, so the error path must not dereference it."""
    module = _seed_module()

    def fake_run(*_args: object, **_kwargs: object):
        return subprocess.CompletedProcess(
            args=["worker"],
            returncode=2,
            stdout='{"error": "backend rejected the batch"}',
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="backend rejected the batch"):
        module._run_json_command(["worker"], request_payload={})


def test_failure_with_no_output_reports_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _seed_module()

    def fake_run(*_args: object, **_kwargs: object):
        return subprocess.CompletedProcess(args=["worker"], returncode=3, stdout="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="exit code 3"):
        module._run_json_command(["worker"], request_payload={})
