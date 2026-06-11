from __future__ import annotations

import logging

from unity.conversation_manager.conversation_manager import (
    _log_slow_brain_single_shot_failure,
)
from unity.logger import LOGGER
from unity.settings import SETTINGS


def test_staging_slow_brain_single_shot_failure_logs_traceback(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(SETTINGS, "DEPLOY_ENV", "staging")
    LOGGER.addHandler(caplog.handler)
    caplog.set_level(logging.ERROR, logger="unity")

    try:
        try:
            raise TypeError("string indices must be integers, not 'str'")
        except TypeError:
            _log_slow_brain_single_shot_failure(
                run_id="llmrun-000001",
                request_id="llmreq-000001",
                origin_event_name="IntegrationToolsSyncRequested",
                message_count=4,
                tool_count=12,
                state_chars=3456,
            )
    finally:
        LOGGER.removeHandler(caplog.handler)

    assert "Slow-brain single-shot failed" in caplog.text
    assert "run_id=llmrun-000001" in caplog.text
    assert "request_id=llmreq-000001" in caplog.text
    assert "origin_event=IntegrationToolsSyncRequested" in caplog.text
    assert any(record.exc_info for record in caplog.records)
