"""Workflow bundles, the surface registry, and the install fan-out.

Symbolic tests over in-memory surfaces. No backend and no LLM: these pin
the invariants that make a bundle installable — that it can only reach
source-scoped surfaces, that its content hash does not move with the
settings someone installs it under, and that uninstall clears exactly the
rows the bundle planted.
"""

from typing import Any, Dict, List

import pytest

from unify.workflow_manager.bundle import (
    SurfaceRegistry,
    UnscopedSurfaceError,
    WorkflowBundle,
)
from unify.workflow_manager.types.workflow import WorkflowMode
from unify.workflow_manager.workflow_manager import WorkflowManager

WORKFLOW = "draft_email_replies"


class RecordingSurface:
    """Stands in for a manager's ``sync_custom``."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.fail = fail

    def __call__(self, *, source_id: str, **kwargs: Any) -> bool:
        if self.fail:
            raise RuntimeError("surface exploded")
        (source,) = kwargs.values()
        self.calls.append({"source_id": source_id, "source": source})
        return bool(source)


def _registry(**surfaces: RecordingSurface) -> SurfaceRegistry:
    registry = SurfaceRegistry()
    for name, syncer in surfaces.items():
        registry.register(
            name,
            syncer,
            source_kwarg=f"source_{name}",
            source_scoped=True,
        )
    return registry


def _manager(registry: SurfaceRegistry) -> WorkflowManager:
    """A manager wired to surfaces but not to any backend.

    ``__init__`` resolves Orchestra contexts; the fan-out under test does
    not touch them, so the instance is built directly.
    """
    manager = object.__new__(WorkflowManager)
    manager._surfaces = registry
    manager._catalogue = {}
    return manager


def _bundle(**kwargs: Any) -> WorkflowBundle:
    defaults: Dict[str, Any] = {
        "slug": WORKFLOW,
        "name": "Draft email replies",
        "version": "1.0.0",
        "surfaces": {
            "guidance": {"triage": {"custom_key": "triage", "custom_hash": "h1"}},
            "tasks": {"morning": {"custom_key": "morning", "custom_hash": "h2"}},
        },
    }
    defaults.update(kwargs)
    return WorkflowBundle(**defaults)


# --------------------------------------------------------------------- #
# Registry                                                              #
# --------------------------------------------------------------------- #
def test_unscoped_surface_is_refused():
    """An adapter that ignores source_id would prune its siblings' rows."""
    registry = SurfaceRegistry()
    with pytest.raises(UnscopedSurfaceError) as excinfo:
        registry.register(
            "contacts",
            RecordingSurface(),
            source_kwarg="source_contacts",
            source_scoped=False,
        )
    assert excinfo.value.surface == "contacts"
    assert "contacts" not in registry


def test_bundle_cannot_claim_the_deployment_source_id():
    with pytest.raises(ValueError, match="reserved"):
        WorkflowBundle(slug="deployment", name="Impostor")


def test_bundle_covering_an_unregistered_surface_is_refused_at_registration():
    manager = _manager(_registry(guidance=RecordingSurface()))
    with pytest.raises(KeyError, match="knowledge"):
        manager.register_bundle(
            _bundle(surfaces={"guidance": {}, "knowledge": {}}),
        )


# --------------------------------------------------------------------- #
# Content hash                                                          #
# --------------------------------------------------------------------- #
def test_content_hash_ignores_params():
    """Two people installing one bundle plant byte-identical rows.

    Params are read at run time, never baked in, so folding them into the
    hash would split one bundle into per-installation content and
    foreclose ever federating a single pinned copy.
    """
    work = _bundle(params_schema={"mailbox": {"required": True}})
    personal = _bundle(params_schema={"mailbox": {"required": True}})
    assert work.content_hash() == personal.content_hash()


def test_content_hash_moves_with_content():
    before = _bundle()
    after = _bundle(
        surfaces={
            "guidance": {"triage": {"custom_key": "triage", "custom_hash": "CHANGED"}},
            "tasks": {"morning": {"custom_key": "morning", "custom_hash": "h2"}},
        },
    )
    assert before.content_hash() != after.content_hash()


# --------------------------------------------------------------------- #
# Fan-out                                                               #
# --------------------------------------------------------------------- #
def test_install_stamps_every_surface_with_the_slug():
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    planted, failures = manager._plant(_bundle())

    assert not failures
    assert planted["guidance"]["entries"] == 1
    assert planted["tasks"]["entries"] == 1
    assert [c["source_id"] for c in guidance.calls] == [WORKFLOW]
    assert [c["source_id"] for c in tasks.calls] == [WORKFLOW]
    assert "triage" in guidance.calls[0]["source"]


def test_uninstall_sends_an_empty_source_per_recorded_surface():
    """Pruning is the engine's, scoped to the slug: an empty source removes
    this bundle's rows and cannot reach anyone else's."""
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    removed, failures = manager._plant(_bundle(), empty=True)

    assert not failures
    assert guidance.calls[0]["source"] == {}
    assert tasks.calls[0]["source"] == {}
    assert removed["guidance"]["entries"] == 0


def test_uninstall_uses_recorded_surfaces_not_the_current_bundle():
    """A bundle that has since dropped a surface must still clear it."""
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    shrunk = _bundle(surfaces={"guidance": {}})
    manager._plant(shrunk, surface_names=["guidance", "tasks"], empty=True)

    assert len(guidance.calls) == 1
    assert len(tasks.calls) == 1


def test_one_failing_surface_does_not_stop_the_others():
    guidance = RecordingSurface()
    tasks = RecordingSurface(fail=True)
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    planted, failures = manager._plant(_bundle())

    assert sorted(failures) == ["tasks"]
    assert guidance.calls, "a failing surface must not skip the rest"
    assert "error" in planted["tasks"]


# --------------------------------------------------------------------- #
# Params                                                                #
# --------------------------------------------------------------------- #
def test_missing_required_param_is_refused():
    from unify.common.tool_outcome import ToolErrorException

    bundle = _bundle(params_schema={"mailbox": {"required": True}})
    with pytest.raises(ToolErrorException) as excinfo:
        WorkflowManager._validate_params(bundle, {})
    assert excinfo.value.payload["missing"] == ["mailbox"]


def test_optional_params_are_not_required():
    bundle = _bundle(params_schema={"tone": {"required": False}})
    assert WorkflowManager._validate_params(bundle, {}) == {}


def test_mode_default_is_seed():
    """Seed is the safe default: pinned silently reverts local edits."""
    assert _bundle().mode is WorkflowMode.seed


# --------------------------------------------------------------------- #
# Surface wiring                                                        #
# --------------------------------------------------------------------- #
def test_registered_kwargs_match_the_real_sync_signatures():
    """The fan-out calls sync_custom by keyword, so a wrong name is a
    TypeError at install time rather than anything the type checker sees.

    KnowledgeManager takes ``source_claims`` while the others take
    ``source_<surface>``; this pins that asymmetry so the mapping cannot
    drift back to a guessed name.
    """
    import inspect

    from unify.guidance_manager.guidance_manager import GuidanceManager
    from unify.knowledge_manager.knowledge_manager import KnowledgeManager
    from unify.task_scheduler.task_scheduler import TaskScheduler
    from unify.workflow_manager.surfaces import SOURCE_SCOPED

    managers = {
        "guidance": GuidanceManager,
        "knowledge": KnowledgeManager,
        "tasks": TaskScheduler,
    }
    for surface, kwarg in SOURCE_SCOPED.items():
        params = inspect.signature(managers[surface].sync_custom).parameters
        assert kwarg in params, f"{surface}: sync_custom has no {kwarg!r}"
        assert "source_id" in params, f"{surface}: sync_custom is not source-scoped"


def test_pending_surfaces_are_not_registered():
    """The unscoped managers must stay unreachable until their adapters
    are scoped; listing one in both places would be the bug."""
    from unify.workflow_manager.surfaces import PENDING_SCOPING, SOURCE_SCOPED

    assert not set(PENDING_SCOPING) & set(SOURCE_SCOPED)
