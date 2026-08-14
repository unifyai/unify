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
    "run_invocation",
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
        assert "primitives.ingestion.submit" in note

    def test_the_spec_warns_off_colour(self):
        note = get_registry().get_manager_spec("canvas").special_note or ""
        assert "COLOUR enters only through semantic token utilities" in note
        assert "rejected" in note


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
        # An earlier manager once omitted one of its tables here, which made
        # `destination="team:N"` fail at runtime for that table only. Asserting
        # per table rather than in aggregate is what catches that.
        from unify.common.authorship import SHARED_SCOPED_TABLES

        assert table in SHARED_SCOPED_TABLES


class TestPromptCanvasBlock:
    """The inline canvas block: kit reference + compressed call forms."""

    def test_the_block_is_rendered_for_the_canvas_scope(self):
        context = get_registry().prompt_context(CANVAS_SCOPE)
        assert "Canvas call forms" in context

    def test_the_call_forms_show_the_full_connected_app_sequence(self):
        from unify.function_manager.primitives.registry import (
            canvas_kit_prompt_block,
        )

        block = canvas_kit_prompt_block()
        # Fetch, store, schedule, bind. A form missing the schedule step
        # teaches a canvas that goes stale silently.
        assert "primitives.integrations." in block
        assert "primitives.ingestion.submit" in block
        assert "primitives.tasks" in block
        assert "PrimitiveBinding" in block

    def test_the_block_never_shows_a_colour(self):
        from unify.canvas_manager.ops.build_ops import lint_source
        from unify.function_manager.primitives.registry import (
            canvas_kit_prompt_block,
        )

        block = canvas_kit_prompt_block()
        # A block that would fail our own linter teaches the actor to write
        # code we then reject.
        assert lint_source(block) == [] or all(
            "not available at view time" in problem or "single module" in problem
            for problem in lint_source(block)
        )


class TestKitReference:
    """The generated component digest the actor authors against.

    Without it the actor guesses component names, and a guess is not a graceful
    degradation: the typechecker rejects the canvas and the whole plan takes
    another round trip.
    """

    def test_the_digest_is_committed_and_documents_components(self):
        from unify.function_manager.primitives.registry import (
            get_canvas_kit_reference,
        )

        digest = get_canvas_kit_reference()
        assert "# @unity/canvas-kit" in digest
        # A parse regression would emit a plausible-looking file with no
        # components in it, which is worse than none because it reads as coverage.
        # The kit is protocol-only, so the surface is small and every entry is
        # load-bearing.
        assert digest.count("\n### ") >= 5
        for name in ("<Canvas>", "<ActionButton>", "<ActionForm>", "<ActionResult>"):
            assert name in digest

    def test_the_digest_reaches_the_prompt(self):
        context = get_registry().prompt_context(CANVAS_SCOPE)
        assert "@unity/canvas-kit" in context
        # The authoring-model preamble is the digest's distinctive text: seeing
        # it proves the digest itself reached the prompt, not just a note
        # that happens to name the kit.
        assert "The kit carries no presentational vocabulary" in context

    def test_the_digest_never_shows_a_colour(self):
        from unify.canvas_manager.ops.build_ops import lint_source
        from unify.function_manager.primitives.registry import (
            get_canvas_kit_reference,
        )

        # The digest is prose the actor imitates, so a colour literal in it would
        # teach precisely what the linter then rejects.
        assert [
            problem
            for problem in lint_source(get_canvas_kit_reference())
            if "colour" in problem
        ] == []


class TestDocstringContract:
    @pytest.mark.parametrize("name", PUBLIC_METHODS)
    def test_the_real_manager_inherits_the_base_contract(self, name):
        from unify.canvas_manager.base import BaseCanvasManager

        concrete = getattr(CanvasManager, name)
        base = getattr(BaseCanvasManager, name)
        assert concrete.__doc__
        assert concrete.__doc__ == base.__doc__
