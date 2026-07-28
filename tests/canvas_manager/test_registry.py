"""Tests that the canvas surface is fully wired.

Half-wiring is the failure this guards. Every step below is in a different file,
and missing any one of them produces no error — the manager simply never appears
to the actor, or appears without contexts, or without team routing. Each
assertion here corresponds to one of those files.
"""

import pytest

from unify.canvas_manager.canvas_manager import (
    ACTIONS_TABLE,
    INVOCATIONS_TABLE,
    VIEWS_TABLE,
    CanvasManager,
)
from unify.function_manager.primitives.registry import get_registry
from unify.function_manager.primitives.scope import (
    VALID_MANAGER_ALIASES,
    PrimitiveScope,
)

CANVAS_TABLES = (VIEWS_TABLE, ACTIONS_TABLE, INVOCATIONS_TABLE)

CANVAS_SCOPE = PrimitiveScope.single("canvas")

PUBLIC_METHODS = (
    "create_view",
    "update_view",
    "refresh_props",
    "get_view",
    "list_views",
    "delete_view",
    "preview",
    "list_invocations",
)


class TestPrimitiveSurface:
    def test_alias_is_in_scope(self):
        # Without this the sandbox refuses `primitives.canvas` outright.
        assert "canvas" in VALID_MANAGER_ALIASES

    def test_spec_is_registered(self):
        spec = get_registry().get_manager_spec("canvas")
        assert spec is not None
        assert spec.primitive_class_path.endswith("CanvasManager")

    def test_every_public_method_is_discovered(self):
        # Discovery walks the Base* MRO for abstract methods, so a method that
        # is not abstract on the base silently never reaches the actor.
        discovered = set(get_registry().primitive_methods(manager_alias="canvas"))
        assert discovered == set(PUBLIC_METHODS)

    def test_the_spec_teaches_the_store_first_rule(self):
        # This note is the actor's only in-prompt hint that connected-app data
        # cannot be read live, which is the mistake it would otherwise make.
        note = get_registry().get_manager_spec("canvas").special_note or ""
        assert "STORED FIRST" in note
        assert "primitives.data.ingest" in note

    def test_the_spec_warns_off_colour(self):
        note = get_registry().get_manager_spec("canvas").special_note or ""
        assert "NEVER write a colour" in note


class TestRuntimeWiring:
    def test_alias_maps_to_a_registry_getter(self):
        from unify.function_manager.primitives.runtime import _ALIAS_TO_GETTER

        assert _ALIAS_TO_GETTER.get("canvas") == "get_canvas_manager"

    def test_the_getter_exists(self):
        from unify.manager_registry import ManagerRegistry

        assert hasattr(ManagerRegistry, "get_canvas_manager")

    def test_the_manager_is_async_wrapped(self):
        # CanvasManager is synchronous, so without this the actor would have to
        # call it without `await` — inconsistent with every other primitive.
        from unify.function_manager.primitives.runtime import _SYNC_MANAGERS

        assert "canvas" in _SYNC_MANAGERS

    def test_settings_are_registered(self):
        from unify.settings import SETTINGS

        assert SETTINGS.canvas.IMPL in {"real", "simulated"}


class TestContextRegistration:
    def test_the_manager_declares_its_tables(self):
        declared = {context.name for context in CanvasManager.Config.required_contexts}
        assert declared == set(CANVAS_TABLES)

    def test_the_manager_is_provisioned(self):
        # Provisioning reads a hard-coded list; omission means the contexts are
        # never created and every write fails at runtime rather than here.
        import inspect

        from unify.common.context_registry import ContextRegistry

        source = inspect.getsource(ContextRegistry._get_managers)
        assert "CanvasManager" in source

    @pytest.mark.parametrize("table", CANVAS_TABLES)
    def test_every_table_supports_team_destinations(self, table):
        # The dashboards equivalent omitted its Actions table, which made
        # `destination="team:N"` fail at runtime for tiles. Asserting per table
        # rather than in aggregate is what would have caught it.
        from unify.common.authorship import SHARED_SCOPED_TABLES

        assert table in SHARED_SCOPED_TABLES


class TestPromptExamples:
    def test_examples_are_registered_for_the_alias(self):
        examples = get_registry().prompt_examples(CANVAS_SCOPE)
        assert examples

    def test_the_connected_app_example_shows_the_full_sequence(self):
        examples = get_registry().prompt_examples(CANVAS_SCOPE)
        # Fetch, store, schedule, bind. An example missing the schedule step
        # teaches a canvas that goes stale silently.
        assert "primitives.integrations." in examples
        assert "primitives.data.ingest" in examples
        assert "primitives.tasks" in examples
        assert "PrimitiveBinding" in examples

    def test_examples_never_show_a_colour(self):
        from unify.canvas_manager.ops.build_ops import lint_source

        examples = get_registry().prompt_examples(CANVAS_SCOPE)
        # An example that would fail our own linter teaches the actor to write
        # code we then reject.
        assert lint_source(examples) == [] or all(
            "not available at view time" in problem or "single module" in problem
            for problem in lint_source(examples)
        )


class TestDocstringContract:
    @pytest.mark.parametrize("name", PUBLIC_METHODS)
    def test_the_real_manager_inherits_the_base_contract(self, name):
        from unify.canvas_manager.base import BaseCanvasManager

        concrete = getattr(CanvasManager, name)
        base = getattr(BaseCanvasManager, name)
        assert concrete.__doc__
        assert concrete.__doc__ == base.__doc__
