"""
tests/conversation_manager/core/test_console_script_result.py
=============================================================

Reading back what the console did with the moves that were asked for.

The point of the report is that the assistant stops saying "and there it is"
when the control was not there. So these are mostly about the difference
between a move that failed, a move that correctly did not happen, and a move
that landed -- because the assistant should say something different about each,
and only one of them is worth a turn.
"""

from __future__ import annotations

import pytest

from unify.conversation_manager.domains.console_script_result import (
    summarize_console_script,
)
from unify.conversation_manager.events import ConsoleScriptResult

pytestmark = pytest.mark.no_unify_context


def result(*outcomes: tuple[str, str]) -> ConsoleScriptResult:
    return ConsoleScriptResult(
        script_id="s1",
        outcomes=[{"target": t, "outcome": o} for t, o in outcomes],
    )


class TestWhatWakesTheBrain:
    def test_a_landed_move_does_not(self):
        """A turn per successful click is cost with nothing to decide."""
        _, needs_attention = summarize_console_script(
            result(("section:integrations", "done")),
        )

        assert needs_attention is False

    def test_a_missing_control_does(self):
        """The assistant may have just described this as done."""
        _, needs_attention = summarize_console_script(
            result(("leaf:contact:42", "not-found")),
        )

        assert needs_attention is True

    @pytest.mark.parametrize("outcome", ["unknown", "not-found", "not-interactive"])
    def test_every_failure_does(self, outcome):
        _, needs_attention = summarize_console_script(result(("x", outcome)))

        assert needs_attention is True

    @pytest.mark.parametrize("outcome", ["skipped", "blocked"])
    def test_a_move_that_correctly_did_not_happen_does_not(self, outcome):
        """Being interrupted, or told not to navigate, is not a fault to fix."""
        _, needs_attention = summarize_console_script(result(("x", outcome)))

        assert needs_attention is False

    def test_one_failure_among_successes_still_does(self):
        _, needs_attention = summarize_console_script(
            result(
                ("section:integrations", "done"),
                ("leaf:integration:github", "not-found"),
            ),
        )

        assert needs_attention is True


class TestWhatTheAssistantIsTold:
    def test_a_landed_move_is_named_so_it_can_be_referred_to(self):
        summary, _ = summarize_console_script(
            result(("section:integrations", "done")),
        )

        assert "section:integrations" in summary

    def test_a_failure_says_not_to_claim_otherwise(self):
        summary, _ = summarize_console_script(
            result(("leaf:contact:42", "not-found")),
        )

        assert "leaf:contact:42" in summary
        assert "not there" in summary
        assert "should not say otherwise" in summary

    def test_an_interruption_is_explained_as_such(self):
        summary, _ = summarize_console_script(result(("route:/billing", "skipped")))

        assert "interrupted" in summary
        assert "should not say otherwise" not in summary

    def test_being_switched_off_is_explained_as_such(self):
        summary, _ = summarize_console_script(result(("route:/billing", "blocked")))

        assert "turned off" in summary

    def test_successes_and_failures_are_both_reported(self):
        summary, _ = summarize_console_script(
            result(
                ("section:integrations", "done"),
                ("leaf:integration:github", "not-found"),
            ),
        )

        assert "section:integrations" in summary
        assert "leaf:integration:github" in summary


class TestMalformedReports:
    def test_an_empty_report_says_nothing_and_wakes_nothing(self):
        summary, needs_attention = summarize_console_script(result())

        assert summary == ""
        assert needs_attention is False

    def test_entries_that_are_not_dicts_are_ignored(self):
        event = ConsoleScriptResult(script_id="s1", outcomes=["nonsense", 42, None])
        summary, needs_attention = summarize_console_script(event)

        assert summary == ""
        assert needs_attention is False

    def test_an_entry_without_a_target_is_ignored(self):
        event = ConsoleScriptResult(
            script_id="s1",
            outcomes=[{"outcome": "not-found"}],
        )
        summary, needs_attention = summarize_console_script(event)

        assert summary == ""
        assert needs_attention is False

    def test_an_unrecognized_outcome_is_ignored_rather_than_guessed_at(self):
        summary, needs_attention = summarize_console_script(
            result(("x", "something-new")),
        )

        assert summary == ""
        assert needs_attention is False


class TestEventShape:
    def test_the_report_is_not_logged_as_conversation(self):
        """Ambient, like presence; it must not land in the transcript."""
        assert ConsoleScriptResult.loggable is False

    def test_outcomes_default_to_empty(self):
        assert ConsoleScriptResult().outcomes == []
