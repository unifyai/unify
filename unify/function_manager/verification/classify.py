"""Deterministic effect-class detection for compositional functions.

The class of a function is a lower bound computed from its AST: the maximum
over the primitives it calls, the classes of the compositional functions it
depends on, and the third-party modules it imports. Every primitive in
``Functions/Primitives`` has an explicit entry in ``PRIMITIVE_EFFECT_CLASSES``;
an unclassified name classifies as ``unsafe_effectful`` and is logged as an
error so the gap is loud.
"""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, List, Mapping, Optional, Set

from ..dependency_analysis import (
    collect_dependencies_from_function_node,
    detect_third_party_imports,
)
from ..execution_env import ENVIRONMENT_MODULES
from ..types.verification import ClassSource, SideEffectClass

logger = logging.getLogger(__name__)

_S = SideEffectClass

# Every static primitive row, keyed by its dotted name. Extending this table
# is a normal code change; the accompanying test enumerates the registry and
# fails when a primitive is missing here.
PRIMITIVE_EFFECT_CLASSES: Dict[str, SideEffectClass] = {
    # actor — a sub-agent may do anything
    "primitives.actor.act": _S.unsafe_effectful,
    # contacts
    "primitives.contacts.ask": _S.read_only,
    "primitives.contacts.update": _S.unsafe_effectful,
    # data
    "primitives.data.claim": _S.unsafe_effectful,
    "primitives.data.create_column": _S.idempotent_effectful,
    "primitives.data.create_derived_column": _S.idempotent_effectful,
    "primitives.data.create_external_column": _S.idempotent_effectful,
    "primitives.data.create_table": _S.idempotent_effectful,
    "primitives.data.delete_column": _S.unsafe_effectful,
    "primitives.data.delete_rows": _S.unsafe_effectful,
    "primitives.data.delete_table": _S.unsafe_effectful,
    "primitives.data.describe_table": _S.read_only,
    "primitives.data.ensure_vector_column": _S.idempotent_effectful,
    "primitives.data.filter": _S.read_only,
    "primitives.data.filter_join": _S.read_only,
    "primitives.data.filter_multi_join": _S.read_only,
    "primitives.data.get_columns": _S.read_only,
    "primitives.data.get_table": _S.read_only,
    "primitives.data.insert_rows": _S.unsafe_effectful,
    "primitives.data.join_tables": _S.read_only,
    "primitives.data.list_tables": _S.read_only,
    "primitives.data.reduce": _S.read_only,
    "primitives.data.reduce_join": _S.read_only,
    "primitives.data.rename_column": _S.idempotent_effectful,
    "primitives.data.rename_table": _S.idempotent_effectful,
    "primitives.data.request_external_write": _S.unsafe_effectful,
    "primitives.data.search": _S.read_only,
    "primitives.data.search_join": _S.read_only,
    "primitives.data.search_multi_join": _S.read_only,
    "primitives.data.update_by_ids": _S.idempotent_effectful,
    "primitives.data.update_rows": _S.idempotent_effectful,
    "primitives.data.vectorize_rows": _S.idempotent_effectful,
    # files — a read-only registry
    "primitives.files.ask_about_file": _S.read_only,
    "primitives.files.describe": _S.read_only,
    "primitives.files.filter_files": _S.read_only,
    "primitives.files.filter_join": _S.read_only,
    "primitives.files.filter_multi_join": _S.read_only,
    "primitives.files.list_columns": _S.read_only,
    "primitives.files.reduce": _S.read_only,
    "primitives.files.render_excel_sheet": _S.read_only,
    "primitives.files.render_pdf": _S.read_only,
    "primitives.files.search_files": _S.read_only,
    "primitives.files.search_join": _S.read_only,
    "primitives.files.search_multi_join": _S.read_only,
    # ingestion
    "primitives.ingestion.cancel": _S.idempotent_effectful,
    "primitives.ingestion.get_logs": _S.read_only,
    "primitives.ingestion.get_status": _S.read_only,
    "primitives.ingestion.list_runs": _S.read_only,
    "primitives.ingestion.pause": _S.idempotent_effectful,
    "primitives.ingestion.resume": _S.idempotent_effectful,
    "primitives.ingestion.retry": _S.idempotent_effectful,
    "primitives.ingestion.submit": _S.unsafe_effectful,
    "primitives.ingestion.wait": _S.read_only,
    # secrets
    "primitives.secrets.ask": _S.read_only,
    "primitives.secrets.update": _S.unsafe_effectful,
    # tasks
    "primitives.tasks.ask": _S.read_only,
    "primitives.tasks.execute": _S.unsafe_effectful,
    "primitives.tasks.get_run_event": _S.read_only,
    "primitives.tasks.get_run_event_children": _S.read_only,
    "primitives.tasks.update": _S.unsafe_effectful,
    # transcripts / web
    "primitives.transcripts.ask": _S.read_only,
    "primitives.web.ask": _S.read_only,
    # a fetch reads the public internet but writes the bytes to a path derived
    # from the URL, so repeating it converges rather than accumulating
    "primitives.web.fetch": _S.idempotent_effectful,
}

# Third-party modules that are pure computation; anything else outside the
# environment-provided modules raises the bound to read_only.
PURE_THIRD_PARTY_MODULES: FrozenSet[str] = frozenset(
    {
        "json",
        "re",
        "math",
        "datetime",
        "pandas",
        "numpy",
        "pydantic",
        "dataclasses",
        "typing",
        "collections",
        "itertools",
        "functools",
        "decimal",
        "statistics",
    },
)

# Modules whose import alone means the function can reach the outside world
# with effects (network clients, mail, process spawning).
UNSAFE_THIRD_PARTY_MODULES: FrozenSet[str] = frozenset(
    {"requests", "httpx", "aiohttp", "smtplib", "subprocess"},
)

# Dotted calls that mutate the machine regardless of which module provides them.
UNSAFE_CALLS: FrozenSet[str] = frozenset({"os.system", "shutil.rmtree"})

_WRITE_MODE_CHARS = frozenset("wax+")


@dataclass
class Classification:
    """Outcome of ``classify_source``."""

    detected: SideEffectClass
    source: ClassSource
    primitives_called: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    third_party_imports: List[str] = field(default_factory=list)
    unclassified_primitives: List[str] = field(default_factory=list)


def _dotted_name(node: ast.AST) -> Optional[str]:
    parts: List[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _open_writes(call: ast.Call) -> bool:
    """Return True for ``open(path, 'w'|'a'|'x'|'+…')`` calls."""
    if not (isinstance(call.func, ast.Name) and call.func.id == "open"):
        return False
    mode: Optional[ast.expr] = None
    if len(call.args) >= 2:
        mode = call.args[1]
    for keyword in call.keywords:
        if keyword.arg == "mode":
            mode = keyword.value
    if isinstance(mode, ast.Constant) and isinstance(mode.value, str):
        return any(ch in _WRITE_MODE_CHARS for ch in mode.value)
    return False


def primitive_effect_class(
    name: str,
    *,
    primitive_rows: Optional[Mapping[str, Mapping]] = None,
) -> Optional[SideEffectClass]:
    """Return the effect class of one primitive, or None when unclassified.

    ``primitive_rows`` (name -> row) covers materialized primitive rows that
    are not in the static table; such a row is unknown ground.
    """
    if name in PRIMITIVE_EFFECT_CLASSES:
        return PRIMITIVE_EFFECT_CLASSES[name]
    if (primitive_rows or {}).get(name) is not None:
        return _S.unsafe_effectful
    return None


def classify_source(
    source: str,
    *,
    known_function_names: Set[str] = frozenset(),
    dependency_class: Optional[Callable[[str], Optional[SideEffectClass]]] = None,
    primitive_rows: Optional[Mapping[str, Mapping]] = None,
    environment_namespaces: FrozenSet[str] = frozenset({"primitives"}),
) -> Classification:
    """Classify one single-function source string.

    ``dependency_class(name)`` returns the (already effective) class of a
    compositional dependency, or None when the dependency does not resolve;
    an unresolved dependency contributes ``unsafe_effectful``.
    """
    tree = ast.parse(source)
    fn_node = tree.body[0]
    if not isinstance(fn_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise ValueError("classify_source expects a single top-level function.")

    namespaces = set(environment_namespaces)
    deps = collect_dependencies_from_function_node(
        fn_node,
        set(known_function_names),
        environment_namespaces=frozenset(namespaces),
    )
    primitives_called = sorted(d for d in deps if "." in d)
    compositional_deps = sorted(d for d in deps if "." not in d)
    third_party = sorted(
        detect_third_party_imports(fn_node, environment_modules=ENVIRONMENT_MODULES),
    )

    bound = _S.safe_noop
    from_primitives = _S.safe_noop
    from_third_party = _S.safe_noop
    unclassified: List[str] = []

    for name in primitives_called:
        klass = primitive_effect_class(name, primitive_rows=primitive_rows)
        if klass is None:
            unclassified.append(name)
            logger.error(
                "Primitive %r has no entry in PRIMITIVE_EFFECT_CLASSES; "
                "classifying it as unsafe_effectful. Add it to classify.py.",
                name,
            )
            klass = _S.unsafe_effectful
        from_primitives = _S.max_of(from_primitives, klass)

    for dep in compositional_deps:
        klass = dependency_class(dep) if dependency_class is not None else None
        if klass is None:
            klass = _S.unsafe_effectful
        from_primitives = _S.max_of(from_primitives, klass)

    for node in ast.walk(fn_node):
        if not isinstance(node, ast.Call):
            continue
        dotted = _dotted_name(node.func)
        if dotted in UNSAFE_CALLS or _open_writes(node):
            from_third_party = _S.max_of(from_third_party, _S.unsafe_effectful)

    for module in third_party:
        if module in UNSAFE_THIRD_PARTY_MODULES:
            from_third_party = _S.max_of(from_third_party, _S.unsafe_effectful)
        elif module not in PURE_THIRD_PARTY_MODULES:
            from_third_party = _S.max_of(from_third_party, _S.read_only)

    bound = _S.max_of(from_primitives, from_third_party)
    if bound is _S.safe_noop:
        source_kind: ClassSource = "pure"
    elif from_primitives.rank >= from_third_party.rank:
        source_kind = "primitives"
    else:
        source_kind = "inferred_third_party"

    return Classification(
        detected=bound,
        source=source_kind,
        primitives_called=primitives_called,
        dependencies=compositional_deps,
        third_party_imports=third_party,
        unclassified_primitives=unclassified,
    )


def effective_class(
    *,
    detected: SideEffectClass,
    source: ClassSource,
    confirmed: Optional[SideEffectClass],
) -> SideEffectClass:
    """Resolve the class policy uses from detection plus an optional confirmation.

    A librarian may raise freely and lower only to the detected bound. Until
    a confirmation exists, a bound that relied on third-party imports is
    treated as ``unsafe_effectful``.
    """
    if confirmed is not None:
        return confirmed if confirmed.rank >= detected.rank else detected
    if source == "inferred_third_party":
        return _S.unsafe_effectful
    return detected
