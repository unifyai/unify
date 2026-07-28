"""Tests for what a canvas is allowed to display, and how app data reaches one.

A canvas differs from a chat answer in one way that drives both concerns here:
it is a rendered surface with a URL that can be shared. So "the owner could read
this" is not the question — "is this safe to project" is.

The connected-app path is tested from the same angle. There is deliberately no
canvas-side integration machinery: app data reaches a view by being stored in a
table first, like every other kind of data, and the guarantee that the actor
does so is the binding validation failing loudly when it has not.
"""

import pytest

from unify.canvas_manager.policy import (
    CANVAS_READABLE_MANAGERS,
    check_readable,
    readable_tables,
)
from unify.canvas_manager.types import PrimitiveBinding


class TestPolicySurface:
    def test_table_names_come_from_the_owning_manager(self):
        # The policy names managers; the tables are read from each manager's own
        # Config.required_contexts, so a table name is declared exactly once.
        assert "Tasks" in readable_tables("tasks")
        assert "Contacts" in readable_tables("contacts")

    def test_a_manager_not_opted_in_exposes_nothing(self):
        assert readable_tables("secrets") == frozenset()
        assert readable_tables("comms") == frozenset()

    @pytest.mark.parametrize("sensitive", ["secrets", "blacklist"])
    def test_sensitive_managers_stay_out_of_the_allowlist(self, sensitive):
        # Guarding the list itself, not just its effect: this is the assertion
        # that fails if someone adds one later without thinking about sharing.
        assert sensitive not in CANVAS_READABLE_MANAGERS

    def test_an_unknown_manager_is_refused_rather_than_ignored(self):
        assert check_readable("nonexistent", "Whatever") is not None


class TestBindingChecks:
    def test_an_opted_in_table_passes(self):
        assert check_readable("tasks", "Tasks") is None

    def test_secrets_are_refused_with_the_reason(self):
        problem = check_readable("secrets", "Secrets")
        assert problem is not None
        # The message should say why, so the actor does not simply retry.
        assert "shareable URL" in problem

    def test_a_table_from_another_manager_is_refused(self):
        problem = check_readable("tasks", "Contacts")
        assert problem is not None
        assert "does not declare" in problem or "not declared" in problem

    def test_dynamic_sub_tables_inherit_their_declared_root(self):
        # Data tables are created per user, so the policy has to admit
        # `Data/<anything>` without being able to enumerate it.
        assert check_readable("data", "Data/GitHubIssues") is None
        assert check_readable("data", "Data/Sales") is None


class TestThroughTheManager:
    def test_a_permitted_binding_is_accepted(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="Tasks",
            bindings=[
                PrimitiveBinding(
                    alias="t",
                    manager="tasks",
                    table="Tasks",
                    args={"operation": "filter"},
                ),
            ],
        )
        assert result.error is None

    def test_a_canvas_cannot_bind_to_secrets(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="Leak",
            bindings=[
                PrimitiveBinding(
                    alias="s",
                    manager="secrets",
                    table="Secrets",
                    args={"operation": "filter"},
                ),
            ],
        )
        assert result.error is not None
        assert "shareable URL" in result.error


class TestConnectedAppData:
    """App data reaches a canvas through a table, not through canvas machinery."""

    def test_stored_app_data_binds_like_any_other_table(
        self,
        canvas_manager,
        valid_tsx,
    ):
        # Once GitHub issues live in Data/*, the canvas has no idea they came
        # from an integration — which is the point. No special binding kind.
        result = canvas_manager.create_view(
            valid_tsx,
            title="Delivery",
            bindings=[
                PrimitiveBinding(
                    alias="issues",
                    manager="data",
                    table="Data/GitHubIssues",
                    args={"operation": "filter", "limit": 200},
                ),
            ],
        )
        assert result.error is None

    def test_two_apps_are_just_two_tables(self, canvas_manager, valid_tsx):
        # Providers cannot be joined against each other directly; once both are
        # stored, showing them together is unremarkable.
        result = canvas_manager.create_view(
            valid_tsx,
            title="Delivery and pipeline",
            bindings=[
                PrimitiveBinding(
                    alias="issues",
                    manager="data",
                    table="Data/GitHubIssues",
                    args={"operation": "filter"},
                ),
                PrimitiveBinding(
                    alias="deals",
                    manager="data",
                    table="Data/HubSpotDeals",
                    args={"operation": "filter"},
                ),
            ],
        )
        assert result.error is None
        contexts = canvas_manager.get_view(result.token).binding_contexts
        assert "Data/GitHubIssues" in contexts
        assert "Data/HubSpotDeals" in contexts


def _flat(text: str | None) -> str:
    """Collapse whitespace so assertions survive the docstring being rewrapped."""
    return " ".join((text or "").split())


class TestContractDocumentation:
    """The docstrings are how the actor learns the sequence."""

    def test_the_store_first_rule_is_stated_with_its_reason(self):
        from unify.canvas_manager.base import BaseCanvasManager

        doc = _flat(BaseCanvasManager.__doc__)
        assert "stored first, displayed second" in doc
        # The reason has to be there too, or the rule reads as arbitrary and the
        # actor will route around it when it feels inconvenient.
        assert "cannot call a connected app while someone is looking at it" in doc
        assert "Providers cannot be joined against each other directly" in doc

    def test_a_worked_two_app_example_is_present(self):
        from unify.canvas_manager.base import BaseCanvasManager

        doc = _flat(BaseCanvasManager.create_view.__doc__)
        assert "primitives.integrations.github.list_issues" in doc
        assert "primitives.data.ingest" in doc
        assert "Data/GitHubIssues" in doc

    def test_anti_patterns_name_the_tempting_shortcuts(self):
        from unify.canvas_manager.base import BaseCanvasManager

        doc = _flat(BaseCanvasManager.create_view.__doc__)
        assert "Do not call a connected app from the canvas source" in doc
        assert "Do not paste app data into" in doc

    def test_no_canvas_side_integration_machinery_is_advertised(self):
        from unify.canvas_manager.base import BaseCanvasManager

        # There is no IntegrationBinding, and the contract must not imply one:
        # app data is DataManager's job, and blurring that is what this design
        # deliberately avoids.
        doc = _flat(BaseCanvasManager.create_view.__doc__)
        assert "IntegrationBinding" not in doc
