"""Tests for the CanvasManager contract, against the in-memory implementation.

These cover the behaviours a caller depends on and that are easy to break
silently: that a failed build never replaces a working canvas, that action
declarations are checked before anything is stored, and that listings stay cheap.
"""

import pytest

from unify.canvas_manager.types import CanvasAction, PrimitiveBinding


def _binding(alias: str = "tasks", table: str = "Tasks") -> PrimitiveBinding:
    return PrimitiveBinding(
        alias=alias,
        manager="tasks",
        table=table,
        args={"operation": "filter", "limit": 25},
    )


class TestAuthoring:
    def test_create_returns_a_shareable_url(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(valid_tsx, title="Tracker")
        assert result.error is None
        assert result.token
        assert "/canvas/view/" in result.url
        assert result.build.ok

    def test_bindings_are_resolved_to_context_paths(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="Tracker",
            bindings=[_binding()],
        )
        record = canvas_manager.get_view(result.token)
        # The resolved path is what executes at view time; storing only the
        # logical table would let a stored canvas resolve differently later.
        assert "Tasks" in record.binding_contexts

    def test_duplicate_aliases_are_rejected(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="Tracker",
            bindings=[_binding("tasks"), _binding("tasks", table="Tasks")],
        )
        assert result.error is not None
        assert "unique" in result.error

    def test_lint_failure_blocks_publication(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            'const c = "#ff0000";\n' + valid_tsx,
            title="Bad",
        )
        assert result.error is not None
        assert result.build.failed_stage == "lint"
        assert not result.token
        assert canvas_manager.list_views() == []


class TestRevision:
    def test_url_survives_an_edit(self, canvas_manager, valid_tsx):
        created = canvas_manager.create_view(valid_tsx, title="V1")
        updated = canvas_manager.update_view(created.token, title="V2")
        # A URL already shared has to keep working, which is the whole reason
        # update exists rather than delete-and-recreate.
        assert updated.url == created.url
        assert canvas_manager.get_view(created.token).title == "V2"

    def test_failed_build_leaves_the_published_version_intact(
        self,
        canvas_manager,
        valid_tsx,
    ):
        created = canvas_manager.create_view(valid_tsx, title="V1")
        result = canvas_manager.update_view(
            created.token,
            tsx='const c = "#abc";\n' + valid_tsx,
        )

        assert result.error is not None
        assert result.build.failed_stage == "lint"
        # The user's working canvas must not go down because a revision failed.
        assert canvas_manager.get_view(created.token).tsx_source == valid_tsx

    def test_omitted_fields_are_left_alone(self, canvas_manager, valid_tsx):
        created = canvas_manager.create_view(
            valid_tsx,
            title="V1",
            description="original",
        )
        canvas_manager.update_view(created.token, title="V2")
        assert canvas_manager.get_view(created.token).description == "original"

    def test_actions_can_be_cleared(self, canvas_manager, valid_tsx):
        created = canvas_manager.create_view(
            valid_tsx,
            title="V1",
            actions=[CanvasAction(name="refresh", label="Refresh", function_name="f")],
        )
        canvas_manager.update_view(created.token, actions=[])
        # This is how a canvas is made read-only.
        assert canvas_manager.declared_actions(created.token) == []

    def test_update_of_unknown_token_is_an_error_not_a_crash(self, canvas_manager):
        assert canvas_manager.update_view("nope", title="x").error is not None


class TestActionDeclaration:
    def test_exactly_one_target_is_required(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[
                CanvasAction(name="a", label="A", function_name="f", function_id=3),
            ],
        )
        assert result.error is not None
        assert "exactly one" in result.error

    def test_a_target_is_required(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[CanvasAction(name="a", label="A")],
        )
        assert result.error is not None

    def test_unbounded_array_input_is_rejected(self, canvas_manager, valid_tsx):
        # The bound is the blast radius: without maxItems a viewer could submit
        # an arbitrarily long recipient list and the action would honour it.
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[
                CanvasAction(
                    name="send",
                    label="Send",
                    function_name="f",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "to": {
                                "type": "array",
                                "items": {"type": "string", "maxLength": 80},
                            },
                        },
                    },
                ),
            ],
        )
        assert result.error is not None
        assert "maxItems" in result.error

    def test_bounded_array_input_is_accepted(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[
                CanvasAction(
                    name="send",
                    label="Send",
                    function_name="f",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "to": {
                                "type": "array",
                                "maxItems": 50,
                                "items": {"type": "string", "maxLength": 80},
                            },
                        },
                    },
                ),
            ],
        )
        assert result.error is None

    def test_destructive_action_must_carry_confirmation_text(
        self,
        canvas_manager,
        valid_tsx,
    ):
        # Console renders this outside the frame with the actual arguments, so
        # a destructive action without it would be consented to blind.
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[
                CanvasAction(
                    name="send",
                    label="Send",
                    function_name="f",
                    destructive=True,
                ),
            ],
        )
        assert result.error is not None
        assert "confirm" in result.error

    def test_kind_must_match_the_target(self, canvas_manager, valid_tsx):
        result = canvas_manager.create_view(
            valid_tsx,
            title="V",
            actions=[CanvasAction(name="a", label="A", kind="task", function_name="f")],
        )
        assert result.error is not None


class TestRetrieval:
    def test_listing_omits_source(self, canvas_manager, valid_tsx):
        canvas_manager.create_view(valid_tsx, title="Tracker")
        listed = canvas_manager.list_views()
        assert len(listed) == 1
        # Listings are for discovery; carrying every canvas's source would make
        # "what views do I have?" expensive.
        assert listed[0].tsx_source == ""
        assert canvas_manager.get_view(listed[0].token).tsx_source == valid_tsx

    def test_delete_is_idempotent(self, canvas_manager, valid_tsx):
        created = canvas_manager.create_view(valid_tsx, title="Tracker")
        assert canvas_manager.delete_view(created.token) is True
        assert canvas_manager.delete_view(created.token) is True
        assert canvas_manager.get_view(created.token) is None

    def test_refresh_props_does_not_touch_the_code(self, canvas_manager, valid_tsx):
        created = canvas_manager.create_view(
            valid_tsx,
            title="Tracker",
            props={"summary": "old"},
        )
        result = canvas_manager.refresh_props(created.token, props={"summary": "new"})

        assert result.error is None
        # No rebuild: this is the cheap path a scheduled refresh uses.
        assert result.build is None
        assert '"new"' in canvas_manager.get_view(created.token).props_json


class TestDocstringContract:
    """The base docstrings are what a caller reads before writing code."""

    def test_public_methods_inherit_their_documentation(self, canvas_manager):
        from unify.canvas_manager.base import BaseCanvasManager

        for name in (
            "create_view",
            "update_view",
            "refresh_props",
            "get_view",
            "list_views",
            "delete_view",
            "preview",
            "list_invocations",
        ):
            concrete = getattr(type(canvas_manager), name)
            base = getattr(BaseCanvasManager, name)
            assert concrete.__doc__, f"{name} lost its docstring"
            assert (
                concrete.__doc__ == base.__doc__
            ), f"{name} duplicates rather than inherits its docstring"

    @pytest.mark.parametrize(
        "section",
        ["Parameters", "Returns", "Examples", "Anti-patterns", "Notes", "See Also"],
    )
    def test_create_view_documents_every_required_section(self, section):
        from unify.canvas_manager.base import BaseCanvasManager

        assert section in (BaseCanvasManager.create_view.__doc__ or "")


class TestRunInvocation:
    """Bookkeeping around one action run.

    The lanes themselves need a real FunctionManager and venv, which is the
    infrastructure the simulated manager exists to avoid. What is worth testing
    without them is everything around the dispatch — and every case here is a way
    a viewer's action could be run twice, or run against something the canvas no
    longer declares.
    """

    def _canvas_with_action(self, canvas_manager, valid_tsx):
        from unify.canvas_manager.types.action import CanvasAction

        result = canvas_manager.create_view(
            valid_tsx,
            title="Actionable",
            actions=[
                CanvasAction(
                    name="notify",
                    label="Send the update",
                    function_name="send_update",
                ),
            ],
        )
        assert result.token, result.error
        return result.token

    def test_a_pending_run_settles(self, canvas_manager, valid_tsx):
        token = self._canvas_with_action(canvas_manager, valid_tsx)
        run = canvas_manager.record_invocation(token, action_name="notify")

        settled = canvas_manager.run_invocation(run.invocation_id, token=token)

        assert settled.status == "succeeded"
        assert settled.finished_at

    def test_a_completed_run_is_not_repeated(self, canvas_manager, valid_tsx):
        """A redelivered event must not send the same email twice.

        At-least-once delivery makes this the normal case rather than an edge one,
        so the run has to be idempotent on its own terms and not only behind the
        idempotency key that created it.
        """
        token = self._canvas_with_action(canvas_manager, valid_tsx)
        run = canvas_manager.record_invocation(token, action_name="notify")

        first = canvas_manager.run_invocation(run.invocation_id, token=token)
        finished_at = first.finished_at
        again = canvas_manager.run_invocation(run.invocation_id, token=token)

        assert again.status == "succeeded"
        # Unchanged, so nothing ran a second time.
        assert again.finished_at == finished_at

    def test_a_run_in_flight_is_left_alone(self, canvas_manager, valid_tsx):
        token = self._canvas_with_action(canvas_manager, valid_tsx)
        run = canvas_manager.record_invocation(
            token,
            action_name="notify",
            status="running",
        )

        assert canvas_manager.run_invocation(run.invocation_id, token=token).status == (
            "running"
        )

    def test_an_action_the_canvas_dropped_fails_rather_than_runs(
        self,
        canvas_manager,
        valid_tsx,
    ):
        """A revision that removed the action must not still execute its target.

        Recording the refusal is better than running something the current canvas
        disowns — the viewer pressed a control the author has since taken away.
        """
        token = self._canvas_with_action(canvas_manager, valid_tsx)
        run = canvas_manager.record_invocation(token, action_name="notify")
        canvas_manager.update_view(token, actions=[])

        settled = canvas_manager.run_invocation(run.invocation_id, token=token)

        assert settled.status == "failed"
        assert "no longer declares" in (settled.error or "")

    def test_an_unknown_run_is_an_error_not_a_silent_pass(
        self,
        canvas_manager,
        valid_tsx,
    ):
        token = self._canvas_with_action(canvas_manager, valid_tsx)

        with pytest.raises(ValueError, match="no invocation"):
            canvas_manager.run_invocation(9999, token=token)
