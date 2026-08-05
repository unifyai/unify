"""Workflow bundles, the surface registry, and the install fan-out.

Symbolic tests over in-memory surfaces. No backend and no LLM: these pin
the invariants that make a bundle installable — that it can only reach
managed-scoped surfaces, that its content hash does not move with the
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
    """Stands in for a manager's per-destination custom sync.

    Shape-only: it pins what the fan-out sends, not what a real manager
    does with it. Receiver semantics (an empty source genuinely pruning,
    the destination genuinely landing content there) are covered by the
    live install/uninstall tests, which drive the real ``sync_custom_*``
    methods — a recording double is structurally blind to them.
    """

    def __init__(self, fail: bool = False) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.fail = fail

    def __call__(
        self,
        *,
        managed_by: str,
        destination: str | None,
        **kwargs: Any,
    ) -> bool:
        if self.fail:
            raise RuntimeError("surface exploded")
        (source,) = kwargs.values()
        self.calls.append(
            {
                "managed_by": managed_by,
                "destination": destination,
                "source": source,
            },
        )
        return bool(source)


def _registry(
    shared: tuple[str, ...] = (),
    **surfaces: RecordingSurface,
) -> SurfaceRegistry:
    registry = SurfaceRegistry()
    for name, syncer in surfaces.items():
        registry.register(
            name,
            syncer,
            source_kwarg=f"source_{name}",
            source_scoped=True,
            shared=name in shared,
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
    """An adapter that ignores managed_by would prune its siblings' rows."""
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


def test_bundle_cannot_claim_the_deployment_managed_by():
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

    planted, failures = manager._plant(_bundle(), destination=None, installed=[])

    assert not failures
    assert planted["guidance"]["entries"] == 1
    assert planted["tasks"]["entries"] == 1
    assert [c["managed_by"] for c in guidance.calls] == [WORKFLOW]
    assert [c["managed_by"] for c in tasks.calls] == [WORKFLOW]
    assert "triage" in guidance.calls[0]["source"]


def test_the_install_destination_reaches_every_surface():
    """A team install plants team content: the installation's destination
    is passed to each surface, never derived from per-entry fields."""
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    manager._plant(_bundle(), destination="team:7", installed=[])

    assert [c["destination"] for c in guidance.calls] == ["team:7"]
    assert [c["destination"] for c in tasks.calls] == ["team:7"]


def test_uninstall_sends_an_empty_source_per_recorded_surface():
    """Pruning is the engine's, scoped to the slug: an empty source removes
    this bundle's rows and cannot reach anyone else's."""
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    removed, failures = manager._plant(
        _bundle(),
        destination=None,
        installed=[],
        empty=True,
    )

    assert not failures
    assert guidance.calls[0]["source"] == {}
    assert tasks.calls[0]["source"] == {}
    assert removed["guidance"]["entries"] == 0


def test_uninstall_uses_recorded_surfaces_not_the_current_bundle():
    """A bundle that has since dropped a surface must still clear it."""
    guidance, tasks = RecordingSurface(), RecordingSurface()
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    shrunk = _bundle(surfaces={"guidance": {}})
    manager._plant(
        shrunk,
        destination=None,
        installed=[],
        surface_names=["guidance", "tasks"],
        empty=True,
    )

    assert len(guidance.calls) == 1
    assert len(tasks.calls) == 1


def test_one_failing_surface_does_not_stop_the_others():
    guidance = RecordingSurface()
    tasks = RecordingSurface(fail=True)
    manager = _manager(_registry(guidance=guidance, tasks=tasks))

    planted, failures = manager._plant(_bundle(), destination=None, installed=[])

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
def test_registered_specs_match_the_real_sync_signatures():
    """The fan-out calls the per-destination syncs by keyword, so a wrong
    name is a TypeError at install time rather than anything the type
    checker sees.

    Pins three things per surface: the method exists, it takes the
    declared source kwarg (KnowledgeManager's ``source_claims`` is the
    asymmetry that motivated this), and it takes ``managed_by`` and
    ``destination`` — the two arguments that make an empty-source
    uninstall prune the right rows in the right place.

    The ``destination`` requirement is the load-bearing one: the
    destination-grouping wrappers (guidance/knowledge/tasks
    ``sync_custom``) have no such parameter because they derive
    destinations from the entries, which is exactly why they discard an
    empty source before the engine and must never be registered.
    FunctionManager's ``sync_custom`` is destination-explicit and passes.
    """
    import inspect

    from unify.function_manager.function_manager import FunctionManager
    from unify.guidance_manager.guidance_manager import GuidanceManager
    from unify.knowledge_manager.knowledge_manager import KnowledgeManager
    from unify.task_scheduler.task_scheduler import TaskScheduler
    from unify.workflow_manager.surfaces import SCOPED_SURFACES

    managers = {
        "guidance": GuidanceManager,
        "knowledge": KnowledgeManager,
        "tasks": TaskScheduler,
        "functions": FunctionManager,
    }
    for surface, spec in SCOPED_SURFACES.items():
        method = getattr(managers[surface], spec.method)
        params = inspect.signature(method).parameters
        kwarg = spec.source_kwarg
        assert kwarg in params, f"{surface}: {spec.method} has no {kwarg!r}"
        assert "managed_by" in params, f"{surface}: {spec.method} is not managed-scoped"
        assert "destination" in params, (
            f"{surface}: {spec.method} has no destination parameter — a "
            "grouping wrapper that derives destinations from its entries "
            "discards an empty source and must never be registered"
        )


def test_pending_surfaces_are_not_registered():
    """The unscoped managers must stay unreachable until their adapters
    are scoped; listing one in both places would be the bug."""
    from unify.workflow_manager.surfaces import PENDING_SCOPING, SCOPED_SURFACES

    assert not set(PENDING_SCOPING) & set(SCOPED_SURFACES)


# --------------------------------------------------------------------- #
# Shared surfaces                                                       #
# --------------------------------------------------------------------- #
def _shared_bundle(slug: str, atoms: Dict[str, str]) -> WorkflowBundle:
    """A bundle shipping only functions; *atoms* maps name -> content hash."""
    return WorkflowBundle(
        slug=slug,
        name=slug,
        surfaces={
            "functions": {
                name: {"custom_key": name, "name": name, "custom_hash": h}
                for name, h in atoms.items()
            },
        },
    )


def test_shared_surface_syncs_the_union_under_the_library_source():
    """Functions key on their name, so a shared atom must be one row: the
    fan-out sends the union of installed bundles under WORKFLOW_LIBRARY,
    never the bundle's own entries under its slug."""
    from unify.workflow_manager.bundle import WORKFLOW_LIBRARY

    functions = RecordingSurface()
    manager = _manager(_registry(shared=("functions",), functions=functions))

    installed = [_shared_bundle("wf_a", {"send_email": "h1", "only_a": "h2"})]
    incoming = _shared_bundle("wf_b", {"send_email": "h1", "only_b": "h3"})

    manager._plant(incoming, destination=None, installed=installed)

    (call,) = functions.calls
    assert call["managed_by"] == WORKFLOW_LIBRARY
    union = call["source"]
    assert set(union) == {"send_email", "only_a", "only_b"}
    assert union["send_email"]["workflows"] == ["wf_a", "wf_b"]
    assert union["only_a"]["workflows"] == ["wf_a"]
    assert union["only_b"]["workflows"] == ["wf_b"]


def test_uninstall_of_a_shared_surface_keeps_the_other_workflows_atoms():
    """Uninstall re-syncs the union minus the leaving bundle: the shared
    atom survives with the remaining membership, and only the atoms no
    workflow references any more leave the source (the engine then prunes
    exactly those rows)."""
    functions = RecordingSurface()
    manager = _manager(_registry(shared=("functions",), functions=functions))

    leaving = _shared_bundle("wf_a", {"send_email": "h1", "only_a": "h2"})
    staying = _shared_bundle("wf_b", {"send_email": "h1", "only_b": "h3"})
    manager._catalogue = {"wf_a": leaving, "wf_b": staying}

    manager._plant(
        leaving,
        destination=None,
        installed=[leaving, staying],
        empty=True,
    )

    (call,) = functions.calls
    union = call["source"]
    assert set(union) == {"send_email", "only_b"}
    assert union["send_email"]["workflows"] == ["wf_b"]


def test_conflicting_shared_content_refuses_before_anything_syncs():
    """Two bundles shipping one name with different content is a curation
    error: whichever copy won, the other workflow would run someone
    else's code under its own name. The union raises before any surface
    is reached, so nothing half-plants."""
    from unify.common.tool_outcome import ToolErrorException

    functions, guidance = RecordingSurface(), RecordingSurface()
    manager = _manager(
        _registry(shared=("functions",), functions=functions, guidance=guidance),
    )

    installed = [_shared_bundle("wf_a", {"send_email": "OLD"})]
    incoming = WorkflowBundle(
        slug="wf_b",
        name="wf_b",
        surfaces={
            "guidance": {"g": {"custom_key": "g", "custom_hash": "hg"}},
            "functions": {
                "send_email": {
                    "custom_key": "send_email",
                    "name": "send_email",
                    "custom_hash": "NEW",
                },
            },
        },
    )

    with pytest.raises(ToolErrorException) as excinfo:
        manager._plant(incoming, destination=None, installed=installed)

    assert excinfo.value.payload["error"] == "conflicting_content"
    assert excinfo.value.payload["workflows"] == ["wf_a", "wf_b"]
    assert not functions.calls, "nothing may sync after a conflict"
    assert not guidance.calls, "no other surface may half-plant either"


def test_bundle_cannot_claim_the_library_source():
    with pytest.raises(ValueError, match="reserved"):
        WorkflowBundle(slug="workflow_library", name="Impostor")


# --------------------------------------------------------------------- #
# Requirements                                                          #
# --------------------------------------------------------------------- #
def test_requirement_connection_is_presence_of_its_secrets():
    from unify.workflow_manager.bundle import WorkflowRequirement

    gmail = WorkflowRequirement(slug="gmail", required_secrets=("GMAIL_TOKEN",))
    assert not gmail.connected(frozenset())
    assert gmail.connected(frozenset({"GMAIL_TOKEN"}))
    assert gmail.missing_secrets(frozenset()) == ["GMAIL_TOKEN"]


def test_requirement_with_nothing_to_gate_on_reads_as_met():
    """A requirement declaring no secrets has no checkable signal yet, so
    it must not hold the workflow's jobs hostage to an uncheckable state."""
    from unify.workflow_manager.bundle import WorkflowRequirement

    web = WorkflowRequirement(slug="web")
    assert web.connected(frozenset())


def test_unmet_requirements_read_the_keyset(monkeypatch):
    from unify.workflow_manager import workflow_manager as wm_module
    from unify.workflow_manager.bundle import WorkflowRequirement

    manager = _manager(_registry(guidance=RecordingSurface()))
    bundle = _bundle(
        requirements=(
            WorkflowRequirement(slug="gmail", required_secrets=("GMAIL_TOKEN",)),
            WorkflowRequirement(slug="slack", required_secrets=("SLACK_TOKEN",)),
        ),
    )

    monkeypatch.setattr(
        wm_module,
        "_read_local_secret_keyset",
        lambda: {"SLACK_TOKEN"},
    )
    unmet = manager._unmet_requirements(bundle)
    assert [r["slug"] for r in unmet] == ["gmail"]
    assert unmet[0]["missing_secrets"] == ["GMAIL_TOKEN"]

    report = manager._requirements_report(bundle)
    assert {r["slug"]: r["connected"] for r in report} == {
        "gmail": False,
        "slack": True,
    }


def test_derived_status_folds_requirements_at_read_time():
    """needs_connection is never stored: connections change without the
    installation row being touched. partial outranks it — something that
    failed to plant must not be masked by a missing connection."""
    derived = WorkflowManager._derived_status
    unmet = [{"slug": "gmail"}]

    assert derived("active", []) == "active"
    assert derived("active", unmet) == "needs_connection"
    assert derived("partial", unmet) == "partial"
    assert derived(None, unmet) is None


def test_arming_uses_the_tasks_surface_armer():
    """Install arms through the registered armer with the slug and the
    install destination — planted definitions are born disarmed, so a
    missing arm call means nothing a workflow sets up ever fires."""
    calls = []

    def armer(*, managed_by, enabled, destination):
        calls.append((managed_by, enabled, destination))
        return [41, 42]

    registry = SurfaceRegistry()
    tasks = RecordingSurface()
    registry.register(
        "tasks",
        tasks,
        source_kwarg="source_tasks",
        source_scoped=True,
        armer=armer,
    )
    manager = _manager(registry)

    armed = manager._arm_workflow_tasks(
        _bundle(),
        destination="team:7",
        enabled=True,
    )
    assert armed == [41, 42]
    assert calls == [(WORKFLOW, True, "team:7")]


def test_arming_a_surface_without_an_armer_is_a_no_op():
    manager = _manager(_registry(tasks=RecordingSurface()))
    assert manager._arm_workflow_tasks(_bundle(), destination=None, enabled=True) == []
