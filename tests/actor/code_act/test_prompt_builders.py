"""
Tests for CodeActActor prompt builder quality.

These tests are intentionally "high-signal string assertions" rather than
snapshot tests. They verify that:
- The prompt teaches the JSON-call convention and defers each tool's
  contract to its schema (the loop renders every callable's docstring and
  signature into the tool list riding each request) — no second rendering.
- The prompt contains diverse examples: sessions, computer, primitives, mixed.
- The prompt contains no legacy `execute_python_code` references.
"""

from __future__ import annotations

from typing import Any, Mapping

import pytest

from unify.actor.code_act_actor import CodeActActor
from unify.actor.prompt_builders import build_code_act_prompt
from unify.session_details import SESSION_DETAILS, AssistantDetails


@pytest.fixture()
def desktop_entitled_assistant():
    """A session whose assistant holds the managed Computer Use add-on.

    The computer prompt sections are only rendered for an entitled
    assistant; without this, ambient session details default to
    unentitled and the environment context is the unavailability stub.
    """
    original = SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = AssistantDetails(
        desktop_mode="ubuntu",
        managed_desktop_status="active",
    )
    yield SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = original


class _DummyEnv:
    """Minimal environment stub for build_code_act_prompt (prompt-context only)."""

    def __init__(self, prompt_context: str):
        self._prompt_context = prompt_context

    def get_prompt_context(self) -> str:
        return self._prompt_context

    def get_tools(self) -> dict:
        return {}


class _DummyToolEnv(_DummyEnv):
    """Minimal environment stub with configurable tool names."""

    def __init__(self, prompt_context: str, tools: dict[str, Any]):
        super().__init__(prompt_context)
        self._tools = tools

    def get_tools(self) -> dict:
        return self._tools


def _real_envs_mixed() -> Mapping[str, Any]:
    """Real environments that produce self-contained prompt context."""
    from unify.actor.environments.state_managers import StateManagerEnvironment
    from unify.actor.environments.base import _CompositeEnvironment

    composite = _CompositeEnvironment(
        [
            StateManagerEnvironment(),
        ],
    )
    return {"primitives": composite}


@pytest.mark.timeout(30)
def test_code_act_prompt_defers_tool_contracts_to_schemas_and_no_legacy_name():
    """The prompt teaches the JSON-call convention only; each tool's contract
    lives in its schema (docstring → description via the loop), never in a
    second in-prompt rendering of signatures/docstrings."""
    actor = CodeActActor()
    tools = dict(actor.get_tools("act"))
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=tools,
    )

    assert "execute_python_code" not in prompt
    assert "### Tools" in prompt
    assert "structured JSON tool calls" in prompt
    assert "is its schema" in prompt

    # No second rendering of the contracts the tool list already carries.
    assert '"signature":' not in prompt
    assert "#### Execution & Session Tools" not in prompt
    assert "#### Additional Tools" not in prompt
    assert "Tools (name → argspec):" not in prompt

    # The contracts still reach the model: the loop converts each callable's
    # docstring into its schema description on every request.
    import inspect as _inspect

    from unify.common.prompt_helpers import unwrap_tool_callable

    for name in (
        "execute_function",
        "execute_code",
        "list_sessions",
        "inspect_state",
        "close_session",
        "close_all_sessions",
    ):
        assert name in tools
        assert _inspect.getdoc(unwrap_tool_callable(tools[name]))
    ec_doc = _inspect.getdoc(unwrap_tool_callable(tools["execute_code"])) or ""
    assert "Execute arbitrary code in a specified language and state mode." in ec_doc
    assert "multi-step composition" in ec_doc.lower()

    # Selection policy (not contract) stays inline in the prompt.
    assert "multi-step composition" in prompt.lower()


@pytest.mark.timeout(30)
def test_code_act_prompt_includes_task_guidance_only_with_task_primitives():
    prompt_with_tasks = build_code_act_prompt(
        environments={
            "primitives": _DummyToolEnv(
                "Task primitives are available.",
                {"primitives.tasks.update": object()},
            ),
        },
        tools={},
    )
    prompt_without_tasks = build_code_act_prompt(
        environments={
            "primitives": _DummyToolEnv(
                "Only contact primitives are available.",
                {"primitives.contacts.ask": object()},
            ),
        },
        tools={},
    )

    # The section is a compact contract stub; depth (task types, entrypoint
    # conventions, binding resolution) lives in the platform/durable-tasks
    # guidance entry behind the consult pointer.
    assert "Durable Scheduled And Triggered Tasks" in prompt_with_tasks
    assert "primitives.tasks.update" in prompt_with_tasks
    assert "live (armed)" in prompt_with_tasks
    assert "status armed AND trigger bindings" in prompt_with_tasks
    # When the primitive confirmed the fields, the actor must report them —
    # never claim failure or uncertainty.
    assert "claim failure or uncertainty" in prompt_with_tasks
    # Consult pointer phrased to hit the platform/durable-tasks title.
    assert "creating verifying arming and running" in prompt_with_tasks
    assert "Durable Scheduled And Triggered Tasks" not in prompt_without_tasks
    # The prompt must not carry incident-specific workaround phrasing.
    assert "loop to re-discover the" not in prompt_with_tasks
    assert "person-like token" not in prompt_with_tasks
    assert "Never fake" not in prompt_with_tasks


@pytest.mark.timeout(30)
def test_incremental_execution_present_and_execution_rules_not_duplicated():
    """Incremental Execution section is present; _EXECUTION_RULES appears exactly once."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
    )

    assert "### Incremental Execution" in prompt
    assert "Verify before scaling" in prompt
    assert "Read-only for exploration" in prompt

    exec_rules_marker = "### Tool Selection: `execute_function` vs `execute_code`"
    assert (
        prompt.count(exec_rules_marker) == 1
    ), f"Expected _EXECUTION_RULES exactly once, found {prompt.count(exec_rules_marker)}"


@pytest.mark.timeout(30)
def test_python_first_principle_present():
    """The Python-first principle is included in the execution rules."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
    )

    assert "Python-first principle" in prompt
    assert "install_python_packages" in prompt
    assert "install_shell_packages" in prompt


@pytest.mark.timeout(30)
def test_code_act_prompt_includes_reasoning_helper_decision_guidance():
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
    )

    # The full query_llm teaching (good uses, anti-patterns, model choice)
    # lives in query_llm.__doc__ behind help(); the prompt keeps a compact
    # when-to-use block, folded into the Sandbox Environment section.
    assert "### Deterministic Code With LLM-Native Semantic Processing" not in prompt
    assert "### Sandbox Environment" in prompt
    assert "async def query_llm(" in prompt
    assert "def list_llms(provider: 'str | None' = None) -> 'list[str]'" in prompt
    assert "unstructured -> structured" in prompt
    assert "unstructured -> unstructured" in prompt
    assert "draft, respond, rewrite" in prompt
    assert "keep it deterministic" in prompt
    assert "Semantic downgrades are bugs" in prompt
    assert "templates pretending" in prompt
    assert "keyword ladders" in prompt
    assert "keep the query_llm(...) call" in prompt
    assert "inside the stored function" in prompt
    # Statelessness and the code -> query_llm -> sub-agent dial are prompt-level
    # doctrine; the full frame lives in the storage-review prompt.
    assert "stateless — a memoryless" in prompt
    assert "never in the model" in prompt
    assert "a dial, not a mode switch" in prompt
    assert "lowest notch that preserves the judgment" in prompt
    assert "whose plan must be discovered at runtime" in prompt
    # The consult path replaces the inline model-selection tables.
    assert "help(query_llm)" in prompt
    assert "Choosing A Model For `query_llm(...)`" not in prompt
    assert "Artificial Analysis (https://artificialanalysis.ai/)" not in prompt
    assert "LLM Query Helpers: `query_llm(...)` And `list_llms(...)`" not in prompt

    # ...while help(query_llm) still carries the full contract.
    import inspect as _inspect

    from unify.common.reasoning import query_llm

    doc = _inspect.getdoc(query_llm) or ""
    assert "Choosing A Model For `query_llm(...)`" in doc
    assert "Artificial Analysis (https://artificialanalysis.ai/)" in doc
    assert "ARC Prize leaderboard: https://arcprize.org/leaderboard" in doc


@pytest.mark.timeout(30)
def test_code_act_prompt_includes_compressed_reasoning_contracts():
    """The semantic-vs-deterministic teaching renders as compact prose."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
    )

    assert "count unread emails from Alice" in prompt
    assert "do not call query_llm(...)" in prompt
    assert "deterministic pre-filter" in prompt


@pytest.mark.timeout(30)
def test_code_act_prompt_does_not_make_reason_mandatory_for_every_loop():
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
    )

    assert "use" in prompt
    assert (
        "``query_llm(...)`` only where meaning-based judgment is doing real work"
        in prompt
    )
    assert "exact logic is enough" in prompt
    assert "freely mix deterministic substeps and semantic substeps" in prompt
    assert "do not call query_llm(...)" in prompt
    assert "Use query_llm(...) for every loop" not in prompt
    assert "Always call query_llm(...)" not in prompt


@pytest.mark.timeout(30)
def test_discovery_first_guidance_separates_search_from_execution_choice():
    """Discovery-first should not imply that a missing library hit means execute_code."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
        discovery_first_policy=True,
    )

    assert "Discovery index scope" in prompt
    # Primitives are searchable now; only prompt-documented callables
    # (computer methods, prompt-injected functions/guidance) stay out.
    assert "the built-in `primitives.*` catalogue" in prompt
    assert "they never appear in search" in prompt
    assert "Search is a discovery step" in prompt
    assert "not an execution decision." in prompt
    assert (
        "if the request or discovery step already identifies one exact function"
        in prompt
    )


@pytest.mark.timeout(30)
def test_discovery_first_examples_no_longer_model_execute_code_as_default_fallback():
    """Discovery-first examples should not teach no-hit => write custom code."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=dict(actor.get_tools("act")),
        discovery_first_policy=True,
    )

    assert (
        "If no function exists, THEN fall back to composing with primitives directly in Python."
        not in prompt
    )
    assert (
        "FunctionManager-discovered functions are available in all execute_code calls"
        not in prompt
    )
    assert "Use `execute_code` for *everything* (Python + shell)" not in prompt
    assert (
        "if one exact function or primitive call is enough, use execute_function"
        in prompt
    )


@pytest.mark.timeout(30)
def test_python_first_principle_absent_without_execute_code():
    """The principle is absent when execute_code is not available."""
    actor = CodeActActor()
    all_tools = dict(actor.get_tools("act"))
    tools = {k: v for k, v in all_tools.items() if k != "execute_code"}
    prompt = build_code_act_prompt(
        environments={},
        tools=tools,
    )

    assert "Python-first principle" not in prompt


@pytest.mark.timeout(30)
def test_multiple_custom_environments_all_included():
    """Multiple custom environments should each have their prompt context included."""
    actor = CodeActActor()

    marker_a = "### Alpha Environment\nAlpha-specific guidance for the LLM."
    marker_b = "### Beta Environment\nBeta-specific guidance for the LLM."
    envs: Mapping[str, Any] = {
        "alpha": _DummyEnv(marker_a),
        "beta": _DummyEnv(marker_b),
    }

    prompt = build_code_act_prompt(
        environments=envs,
        tools=dict(actor.get_tools("act")),
    )

    assert marker_a in prompt
    assert marker_b in prompt


@pytest.mark.timeout(30)
def test_custom_environment_empty_prompt_context_excluded():
    """Custom environments returning empty prompt context should not inject noise."""
    actor = CodeActActor()

    envs: Mapping[str, Any] = {
        "empty_env": _DummyEnv(""),
        "whitespace_env": _DummyEnv("   \n  "),
    }

    prompt = build_code_act_prompt(
        environments=envs,
        tools=dict(actor.get_tools("act")),
    )

    # The prompt should still be valid (no crash) and not contain stray whitespace blocks.
    assert "empty_env" not in prompt
    assert "whitespace_env" not in prompt


# ────────────────────────────────────────────────────────────────────────────
# External app integration section
# ────────────────────────────────────────────────────────────────────────────


# ────────────────────────────────────────────────────────────────────────────
# Guidelines composition (constructor baseline + per-invocation overlay)
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.timeout(30)
def test_guidelines_neither_specified():
    """No guidelines at all -> no ### Guidelines section in the prompt."""
    actor = CodeActActor()
    prompt = build_code_act_prompt(
        environments={},
        tools=dict(actor.get_tools("act")),
        guidelines=None,
    )
    assert "### Guidelines" not in prompt


@pytest.mark.timeout(30)
def test_guidelines_constructor_only():
    """Constructor-level guidelines appear in a single ### Guidelines section."""
    actor = CodeActActor(guidelines="Always respond in formal English.")
    base = actor._base_guidelines
    effective = "\n\n".join(filter(None, [base, None])) or None

    prompt = build_code_act_prompt(
        environments={},
        tools=dict(actor.get_tools("act")),
        guidelines=effective,
    )
    assert prompt.count("### Guidelines") == 1
    assert "Always respond in formal English." in prompt


@pytest.mark.timeout(30)
def test_guidelines_per_invocation_only():
    """Per-invocation guidelines appear in a single ### Guidelines section."""
    actor = CodeActActor()
    per_invocation = "Check all contact fields."
    effective = (
        "\n\n".join(filter(None, [actor._base_guidelines, per_invocation])) or None
    )

    prompt = build_code_act_prompt(
        environments={},
        tools=dict(actor.get_tools("act")),
        guidelines=effective,
    )
    assert prompt.count("### Guidelines") == 1
    assert "Check all contact fields." in prompt


@pytest.mark.timeout(30)
def test_guidelines_both_compose():
    """Constructor + per-invocation guidelines compose into one ### Guidelines section."""
    actor = CodeActActor(guidelines="Always respond in formal English.")
    per_invocation = "Check all contact fields."
    effective = (
        "\n\n".join(
            filter(None, [actor._base_guidelines, per_invocation]),
        )
        or None
    )

    prompt = build_code_act_prompt(
        environments={},
        tools=dict(actor.get_tools("act")),
        guidelines=effective,
    )
    assert prompt.count("### Guidelines") == 1
    assert "Always respond in formal English." in prompt
    assert "Check all contact fields." in prompt
    # Constructor guidelines come first
    idx_base = prompt.index("Always respond in formal English.")
    idx_overlay = prompt.index("Check all contact fields.")
    assert idx_base < idx_overlay


@pytest.mark.timeout(30)
def test_storage_notice_matches_session_mode():
    """The skill-storage notice must describe the schedule the run actually
    gets: one-shot acts consolidate after the final result; persistent
    sessions consolidate after each completed turn and surface background
    notes the model should act on for repeat deliverables."""
    actor = CodeActActor()
    tools = dict(actor.get_tools("act"))

    one_shot = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=tools,
        can_store=True,
    )
    assert "after you return your result" in one_shot
    assert "after each completed turn" not in one_shot

    session = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=tools,
        can_store=True,
        persist=True,
    )
    assert "after each completed turn" in session
    assert "after you return your result" not in session
    # The convergence contract: repeat requests are one execution and a
    # report, and amendments edit the stored function in place rather than
    # triggering a fresh replan.
    assert "do not re-derive the procedure inline" in session
    assert "one execution and a report" in session
    assert "`overwrite=True` edit" in session

    without_store = build_code_act_prompt(
        environments=_real_envs_mixed(),
        tools=tools,
        can_store=False,
        persist=True,
    )
    assert "Skill Storage" not in without_store
