"""Tests for ToolSurfaceRegistry."""

import hashlib
import inspect

import pytest

from unify.actor.environments.actor import _ActorRunner
from unify.function_manager.hash_utils import stable_hash_for_rows
from unify.function_manager.primitives.scope import (
    PrimitiveScope,
    VALID_MANAGER_ALIASES,
)
from unify.function_manager.primitives.registry import (
    get_registry,
    get_primitive_sources,
    ManagerSpec,
    _COMMON_EXCLUDED_METHODS,
    _MANAGER_SPECS,
)

_ACTOR_CLASS_PATH = "unify.actor.environments.actor._ActorRunner"

# ────────────────────────────────────────────────────────────────────────────
# Singleton and basic registry tests
# ────────────────────────────────────────────────────────────────────────────


def test_singleton_registry():
    """get_registry() returns singleton."""
    r1 = get_registry()
    r2 = get_registry()
    assert r1 is r2


def test_manager_spec_frozen():
    """ManagerSpec is frozen (immutable)."""
    spec = ManagerSpec(
        manager_alias="test",
        primitive_class_path="test.TestManager",
    )
    with pytest.raises(AttributeError):
        spec.manager_alias = "changed"  # type: ignore


# ────────────────────────────────────────────────────────────────────────────
# Manager spec lookup tests
# ────────────────────────────────────────────────────────────────────────────


def test_manager_specs_includes_all_aliases():
    """_MANAGER_SPECS covers every alias in VALID_MANAGER_ALIASES, and
    manager_specs() over the full scope returns them all in registry order."""
    spec_aliases = {s.manager_alias for s in _MANAGER_SPECS}
    assert spec_aliases == VALID_MANAGER_ALIASES

    registry = get_registry()
    specs = registry.manager_specs(PrimitiveScope.all_managers())
    assert specs == list(_MANAGER_SPECS)


def test_get_manager_spec_valid():
    """get_manager_spec() returns spec for valid alias."""
    registry = get_registry()
    spec = registry.get_manager_spec("actor")
    assert spec is not None
    assert spec.manager_alias == "actor"
    assert spec.primitive_class_path == _ACTOR_CLASS_PATH
    assert spec.sandbox_root == "primitives"
    assert spec.domain == "Actor Delegation"


def test_get_manager_spec_invalid():
    """get_manager_spec() returns None for invalid alias."""
    registry = get_registry()
    spec = registry.get_manager_spec("invalid")
    assert spec is None


# ────────────────────────────────────────────────────────────────────────────
# Primitive methods discovery tests
# ────────────────────────────────────────────────────────────────────────────


def test_primitive_methods_for_actor():
    """primitive_methods() honours the class's explicit _PRIMITIVE_METHODS."""
    registry = get_registry()
    methods = registry.primitive_methods(manager_alias="actor")
    assert methods == sorted(_ActorRunner._PRIMITIVE_METHODS)
    assert methods == ["act"]


def test_primitive_methods_unknown_alias_is_empty():
    """primitive_methods() returns an empty list for an unknown alias."""
    registry = get_registry()
    assert registry.primitive_methods(manager_alias="nonexistent") == []


def test_primitive_methods_respects_common_exclusions():
    """primitive_methods() excludes common excluded methods."""
    registry = get_registry()
    for alias in VALID_MANAGER_ALIASES:
        methods = registry.primitive_methods(manager_alias=alias)
        for excluded in _COMMON_EXCLUDED_METHODS:
            assert (
                excluded not in methods
            ), f"{excluded} should be excluded from {alias}"


# ────────────────────────────────────────────────────────────────────────────
# Tool names tests
# ────────────────────────────────────────────────────────────────────────────


def test_tool_names_fully_qualified():
    """tool_names() returns fully-qualified names under the primitives root."""
    registry = get_registry()
    names = registry.tool_names(PrimitiveScope.single("actor"))
    assert names == ["primitives.actor.act"]


# ────────────────────────────────────────────────────────────────────────────
# Primitive row filter tests (for FunctionManager queries)
# ────────────────────────────────────────────────────────────────────────────


def test_primitive_row_filter():
    """primitive_row_filter() builds a membership expression over primitive_class."""
    registry = get_registry()
    filter_expr = registry.primitive_row_filter(PrimitiveScope.single("actor"))
    assert filter_expr == f'primitive_class in ["{_ACTOR_CLASS_PATH}"]'


# ────────────────────────────────────────────────────────────────────────────
# Collect primitives tests
# ────────────────────────────────────────────────────────────────────────────


def test_collect_primitives_returns_expected_fields():
    """collect_primitives() returns rows with required fields."""
    registry = get_registry()
    primitives = registry.collect_primitives(PrimitiveScope.single("actor"))
    assert set(primitives) == {"primitives.actor.act"}
    row = primitives["primitives.actor.act"]
    assert row["name"] == "primitives.actor.act"
    assert isinstance(row["function_id"], int)
    assert row["primitive_class"] == _ACTOR_CLASS_PATH
    assert row["primitive_method"] == "act"
    assert row["argspec"].startswith("(self, request")
    assert "guidelines" in row["argspec"]
    assert row["docstring"].startswith("Spawn an actor to work on a focused sub-task.")
    assert row["is_primitive"] is True
    assert row["implementation"] is None


def test_collect_primitives_without_scope_matches_full_scope():
    """collect_primitives() with no scope collects every registered primitive."""
    registry = get_registry()
    unscoped = registry.collect_primitives()
    scoped = registry.collect_primitives(PrimitiveScope.all_managers())
    assert unscoped == scoped


def test_collect_primitives_matches_get_primitive_sources():
    """Every primitive from get_primitive_sources() appears in the
    collected set."""
    registry = get_registry()
    scope = PrimitiveScope.all_managers()
    primitives = registry.collect_primitives(scope)

    # Build a reverse lookup: (primitive_class_suffix, method) -> name
    method_to_name = {
        (row["primitive_class"].rsplit(".", 1)[-1], row["primitive_method"]): name
        for name, row in primitives.items()
    }
    for cls, method_names in get_primitive_sources():
        class_name = cls.__name__
        for method_name in method_names:
            assert (
                class_name,
                method_name,
            ) in method_to_name, f"Expected auto-discovered primitive for {class_name}.{method_name} not found"


def test_collect_primitives_stable_ids():
    """collect_primitives() generates stable IDs across calls that agree
    with get_function_id()."""
    registry = get_registry()
    scope = PrimitiveScope.all_managers()

    primitives1 = registry.collect_primitives(scope)
    primitives2 = registry.collect_primitives(scope)

    for name in primitives1:
        assert (
            primitives1[name]["function_id"] == primitives2[name]["function_id"]
        ), f"ID for '{name}' should be stable across calls"
        _, alias, method = name.split(".")
        assert primitives1[name]["function_id"] == registry.get_function_id(
            alias,
            method,
        )


# ────────────────────────────────────────────────────────────────────────────
# Hash computation tests
# ────────────────────────────────────────────────────────────────────────────


def test_compute_hash_for_manager():
    """compute_hash_for_manager() returns consistent hash."""
    registry = get_registry()
    hash1 = registry.compute_hash_for_manager("actor")
    hash2 = registry.compute_hash_for_manager("actor")
    assert hash1 == hash2
    assert len(hash1) == 16  # 16-char hex


def test_static_primitive_hash_projection_matches_legacy_payload():
    """Static primitive hashes keep the historical name|argspec|docstring payload."""
    rows = [
        {"name": "b", "argspec": "(b: str)", "docstring": "B"},
        {"name": "a", "argspec": "(a: str)", "docstring": "A"},
    ]

    expected = hashlib.sha256("a|(a: str)|A\nb|(b: str)|B".encode()).hexdigest()[:16]

    assert (
        stable_hash_for_rows(
            rows,
            fields=("name", "argspec", "docstring"),
            digest_chars=16,
            projection="delimited",
        )
        == expected
    )


def test_compute_primitives_hash_stable():
    """compute_primitives_hash() is stable for same scope."""
    registry = get_registry()
    scope = PrimitiveScope.all_managers()
    hash1 = registry.compute_primitives_hash(primitive_scope=scope)
    hash2 = registry.compute_primitives_hash(primitive_scope=scope)
    assert hash1 == hash2


def test_compute_primitives_hash_accepts_precomputed():
    """compute_primitives_hash() can use pre-collected primitives."""
    registry = get_registry()
    scope = PrimitiveScope.single("actor")
    primitives = registry.collect_primitives(scope)

    # Hash with pre-collected should match hash computed internally
    hash_precomputed = registry.compute_primitives_hash(primitives=primitives)
    hash_computed = registry.compute_primitives_hash(primitive_scope=scope)

    assert hash_precomputed == hash_computed


# ────────────────────────────────────────────────────────────────────────────
# Docstring quality tests (prompt context derives from these)
# ────────────────────────────────────────────────────────────────────────────

MIN_SUMMARY_CHARS = 20
MIN_PARAMS_CHARS = 10


def _first_paragraph(docstring: str) -> str:
    """Extract text up to the first blank line."""
    lines = []
    for line in docstring.splitlines():
        if not line.strip():
            if lines:
                break
        else:
            lines.append(line)
    return "\n".join(lines)


def _parameters_block(docstring: str) -> str:
    """Extract the NumPy-style Parameters section."""
    import re

    lines = docstring.splitlines()
    params_lines = []
    in_params = False
    for j, raw in enumerate(lines):
        stripped = raw.strip()
        if stripped == "Parameters":
            in_params = True
            continue
        if in_params and stripped.startswith("---"):
            continue
        if in_params:
            if stripped and not stripped[0].isspace() and not stripped.startswith("-"):
                if j + 1 < len(lines) and lines[j + 1].strip().startswith("---"):
                    break
                if re.match(r"^[A-Z][a-zA-Z\s]+$", stripped) and len(stripped) < 30:
                    break
            params_lines.append(raw)
    return "\n".join(params_lines).rstrip()


def test_all_primitive_methods_have_summary_and_parameters():
    """Every public primitive method must have a non-empty first-paragraph
    summary and a non-empty NumPy-style Parameters block in its docstring.

    The prompt context rendered for the CodeActActor is derived directly
    from these docstrings.  Missing or empty sections mean the LLM gets
    no guidance on what a method does or how to call it.
    """
    registry = get_registry()

    missing_summary = []
    missing_params = []

    for spec in _MANAGER_SPECS:
        cls = registry._load_manager_class(spec.primitive_class_path)
        if cls is None:
            continue
        methods = registry.primitive_methods(manager_alias=spec.manager_alias)

        for method_name in methods:
            fq = f"primitives.{spec.manager_alias}.{method_name}"

            doc = registry._extract_method_docstring(cls, method_name)

            summary = _first_paragraph(doc)
            if len(summary) < MIN_SUMMARY_CHARS:
                missing_summary.append(f"{fq} (got {len(summary)} chars)")

            # Only require a Parameters block if the method actually has parameters
            # beyond `self`.
            try:
                sig = inspect.signature(getattr(cls, method_name))
                has_params = any(
                    p.name != "self"
                    for p in sig.parameters.values()
                    if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
                )
            except (ValueError, TypeError):
                has_params = True  # err on the side of requiring docs

            if has_params:
                params = _parameters_block(doc)
                if len(params) < MIN_PARAMS_CHARS:
                    missing_params.append(f"{fq} (got {len(params)} chars)")

    assert not missing_summary, (
        f"Methods with missing/short first-paragraph summary "
        f"(min {MIN_SUMMARY_CHARS} chars):\n  " + "\n  ".join(missing_summary)
    )
    assert not missing_params, (
        f"Methods with missing/short Parameters block "
        f"(min {MIN_PARAMS_CHARS} chars):\n  " + "\n  ".join(missing_params)
    )


def test_format_method_signature_hides_internal_params():
    """_format_method_signature() strips self and _-prefixed wiring params."""
    sig = get_registry()._format_method_signature(_ActorRunner, "act")
    assert sig.startswith("(request")
    assert "guidelines" in sig
    assert "_clarification_up_q" not in sig
    assert "_clarification_down_q" not in sig
    assert "self" not in sig
