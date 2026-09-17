import importlib
from datetime import datetime, timedelta, timezone

import pytest

import unify.common.prompt_helpers as prompt_helpers
from unify.session_details import SESSION_DETAILS

pytestmark = pytest.mark.no_unify_context


def _real_prompt_helpers(monkeypatch):
    module = importlib.reload(prompt_helpers)
    monkeypatch.setattr(SESSION_DETAILS.assistant, "timezone", "")
    return module


def test_now_full_format():
    # Human-readable format with day, month, date, time, and timezone
    assert prompt_helpers.now() == "Friday, June 13, 2025 at 12:00 PM UTC"


def test_now_time_only():
    assert prompt_helpers.now(time_only=True) == "12:00 PM UTC"


def test_now_as_datetime():
    # When as_string=False, returns a datetime object
    result = prompt_helpers.now(as_string=False)
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 6
    assert result.day == 13


def test_assistant_timezone_reads_the_session_profile(monkeypatch):
    module = _real_prompt_helpers(monkeypatch)

    assert module.get_assistant_timezone() is None
    monkeypatch.setattr(SESSION_DETAILS.assistant, "timezone", "Asia/Karachi")
    assert module.get_assistant_timezone() == "Asia/Karachi"


def test_now_converts_to_the_assistant_timezone(monkeypatch):
    module = _real_prompt_helpers(monkeypatch)
    monkeypatch.setattr(SESSION_DETAILS.assistant, "timezone", "Asia/Karachi")
    monkeypatch.setattr(
        module,
        "_utc_now",
        lambda: datetime(2026, 5, 7, 8, 0, 0, tzinfo=timezone.utc),
    )

    assert module.now() == "Thursday, May 07, 2026 at 01:00 PM Asia/Karachi"
    assert module.now(time_only=True) == "01:00 PM Asia/Karachi"


def test_now_recomputes_current_time(monkeypatch):
    module = _real_prompt_helpers(monkeypatch)
    current_times = iter(
        [
            datetime(2026, 5, 7, 8, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 5, 7, 8, 0, 1, tzinfo=timezone.utc),
        ],
    )
    monkeypatch.setattr(module, "_utc_now", lambda: next(current_times))

    first = module.now(as_string=False)
    second = module.now(as_string=False)

    assert first == datetime(2026, 5, 7, 8, 0, 0, tzinfo=timezone.utc)
    assert second == first + timedelta(seconds=1)


def test_now_falls_back_to_utc_when_timezone_is_unset_or_invalid(monkeypatch):
    module = _real_prompt_helpers(monkeypatch)
    monkeypatch.setattr(
        module,
        "_utc_now",
        lambda: datetime(2026, 5, 7, 8, 0, 0, tzinfo=timezone.utc),
    )

    assert module.now() == "Thursday, May 07, 2026 at 08:00 AM UTC"

    monkeypatch.setattr(SESSION_DETAILS.assistant, "timezone", "Not/AZone")
    assert module.now() == "Thursday, May 07, 2026 at 08:00 AM UTC"
