"""
tests/conversation_manager/core/test_console_actions.py
=======================================================

Pairing a spoken line with the console moves it narrates.

The offsets these produce are what Console counts synchronized transcript
characters against, so an off-by-one here moves the page on the wrong word. The
positional assertions below slice the spoken text at each offset and check what
was said up to that point, which is the property that actually matters.
"""

from __future__ import annotations

import pytest

from unify.conversation_manager.console_actions import (
    catalogue_form,
    parse_console_actions,
    strip_markers,
)

pytestmark = pytest.mark.no_unify_context

TARGETS = ["section:integrations", "route:/billing"]


class TestMarkerStripping:
    def test_markers_never_reach_the_spoken_text(self):
        """A marker that survives is read aloud, so this is the load-bearing one."""
        parsed = parse_console_actions("Open Integrations [[1]] now.", TARGETS)

        assert "[[" not in parsed.spoken_text
        assert "]]" not in parsed.spoken_text

    def test_gap_left_by_a_marker_is_closed(self):
        parsed = parse_console_actions("Open Integrations [[1]] and wait.", TARGETS)

        assert parsed.spoken_text == "Open Integrations and wait."

    def test_space_before_punctuation_is_closed(self):
        parsed = parse_console_actions("Open Integrations [[1]], then wait.", TARGETS)

        assert parsed.spoken_text == "Open Integrations, then wait."

    def test_a_line_opening_with_a_marker_keeps_no_leading_space(self):
        parsed = parse_console_actions("[[1]] Here we are.", TARGETS)

        assert parsed.spoken_text == "Here we are."

    def test_strip_markers_leaves_a_speakable_line(self):
        assert strip_markers("Open it [[1]] now.") == "Open it now."

    def test_a_line_without_markers_is_untouched(self):
        assert strip_markers("Nothing to strip.") == "Nothing to strip."


class TestOffsets:
    def test_offset_lands_after_the_words_it_follows(self):
        parsed = parse_console_actions(
            "I'll open Integrations [[1]], and billing is here [[2]].",
            TARGETS,
        )

        first, second = parsed.steps
        assert parsed.spoken_text[: first.after_chars].endswith("Integrations")
        assert parsed.spoken_text[: second.after_chars].endswith("here")

    def test_every_offset_indexes_inside_the_spoken_text(self):
        parsed = parse_console_actions(
            "One [[1]] and two [[2]] done.",
            TARGETS,
        )

        for step in parsed.steps:
            assert 0 <= step.after_chars <= len(parsed.spoken_text)

    def test_offsets_follow_marker_order(self):
        parsed = parse_console_actions("A [[1]] B [[2]] C.", TARGETS)

        offsets = [step.after_chars for step in parsed.steps]
        assert offsets == sorted(offsets)

    def test_targets_are_taken_in_marker_order_not_text_order(self):
        """``[[2]]`` takes the second target even when it is spoken first."""
        parsed = parse_console_actions("First [[2]] then [[1]].", TARGETS)

        assert [step.target for step in parsed.steps] == [
            "route:/billing",
            "section:integrations",
        ]


class TestMismatches:
    def test_marker_without_a_target_is_dropped_not_shifted(self):
        """Miscounting must not silently move the user somewhere else."""
        parsed = parse_console_actions("A [[1]] B [[2]].", ["section:integrations"])

        assert [step.target for step in parsed.steps] == ["section:integrations"]
        assert any("[[2]]" in note for note in parsed.dropped)

    def test_target_without_a_marker_is_reported(self):
        parsed = parse_console_actions("Just talking [[1]].", TARGETS)

        assert len(parsed.steps) == 1
        assert any("route:/billing" in note for note in parsed.dropped)

    def test_line_is_still_speakable_when_everything_mismatches(self):
        parsed = parse_console_actions("Nothing lines up [[9]].", [])

        assert parsed.spoken_text == "Nothing lines up."
        assert parsed.steps == ()

    def test_no_targets_yields_a_clean_line_and_no_moves(self):
        """The off-console path: speak the line, make no moves."""
        parsed = parse_console_actions("Open Integrations [[1]] now.", [])

        assert parsed.spoken_text == "Open Integrations now."
        assert parsed.steps == ()


class TestCatalogueLookup:
    """Checking a target against the list Console published.

    A control that takes a name is listed once, under a placeholder, because
    there is one per connectable app. Looking up the specific name has to find
    that line rather than an id the catalogue could never hold.
    """

    def test_a_named_control_looks_up_under_its_placeholder(self):
        assert catalogue_form("leaf:integration:github") == "leaf:integration:<name>"

    def test_a_fixed_control_looks_itself_up(self):
        assert catalogue_form("leaf:add-integration") == "leaf:add-integration"

    @pytest.mark.parametrize(
        "target",
        ["section:integrations", "route:/billing", "account:security"],
    )
    def test_navigation_targets_are_untouched(self, target):
        assert catalogue_form(target) == target

    def test_an_empty_name_is_left_to_fail_the_lookup(self):
        """Not rewritten into a valid placeholder, so it is rejected upstream."""
        assert catalogue_form("leaf:integration:") == "leaf:integration:"

    def test_extra_segments_are_left_to_fail_the_lookup(self):
        assert catalogue_form("leaf:integration:a:b") == "leaf:integration:a:b"
