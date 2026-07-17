"""SIGTERM writeback for offline_runner Tasks/Runs terminalization."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from unify.task_scheduler.offline_runner import (
    OfflineTaskConfig,
    _SIGTERM_EXIT_CODE,
    _install_sigterm_handler,
)


def _config() -> OfflineTaskConfig:
    return OfflineTaskConfig(
        assistant_id="1406",
        run_key="offline:explicit:1406:9:ref",
        task_id=9,
        function_id=1,
        request="run poll",
        source_type="explicit",
        source_task_log_id=555,
        activation_revision="rev",
    )


def test_sigterm_handler_terminalizes_active_source_and_exits():
    config = _config()
    captured = {}

    def _fake_signal(sig, handler):
        captured["handler"] = handler

    with (
        patch("unify.task_scheduler.offline_runner.signal.signal", _fake_signal),
        patch(
            "unify.task_scheduler.offline_runner._mark_source_task_failed",
        ) as mark_failed,
        patch(
            "unify.task_scheduler.offline_runner._update_task_run",
        ) as update_run,
    ):
        _install_sigterm_handler(config)
        with pytest.raises(SystemExit) as exc_info:
            captured["handler"](15, None)

    assert exc_info.value.code == _SIGTERM_EXIT_CODE
    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[0] is config
    update_run.assert_called_once()
    assert update_run.call_args.kwargs["updates"]["state"] == "failed"
