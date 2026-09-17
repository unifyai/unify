"""
ToolSurfaceRegistry: Single source of truth for manager/primitive configuration.

This module centralizes all manager definitions, primitive method discovery,
prompt context generation, and primitive row collection for FunctionManager indexing.

No other module should define manager lists or primitive configurations.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

from unify.function_manager.hash_utils import stable_hash_for_rows
from unify.function_manager.primitives.scope import PrimitiveScope

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# =============================================================================
# ManagerSpec - Per-manager configuration
# =============================================================================


@dataclass(frozen=True, slots=True)
class PromptText:
    """The overview fields rendered for one manager in the actor prompt."""

    domain: str
    description: str
    use_when: str
    examples: str
    special_note: str | None = None


@dataclass(frozen=True, slots=True)
class ManagerSpec:
    """
    Configuration for a single manager in the tool surface.

    This is the authoritative specification for each manager, containing:
    - Identity (alias, registry key, class path)
    - Method exclusions
    - Prompt metadata (domain, description, use_when, examples, priority)
    - Sandbox namespace root (the top-level key in the sandbox's global_state)
    """

    manager_alias: str
    manager_registry_key: str
    primitive_class_path: str
    # The top-level key under which this manager's methods appear in a
    # CodeActActor sandbox's global_state.  All managers live under
    # ``"primitives"`` (accessed as ``primitives.<alias>.<method>``).
    # Used by construct_sandbox_root() to map dotted depends_on entries
    # back to the class that provides them.
    sandbox_root: str = "primitives"
    excluded_methods: frozenset[str] = field(default_factory=frozenset)
    priority: int = 99
    domain: str = ""
    description: str = ""
    use_when: str = ""
    examples: str = ""
    special_note: str | None = None
    # A manager that replaces this one for new work. The primary prompt fields
    # above may steer authoring toward that replacement, so they only render
    # while the replacement is exposed in the same scope; when it is absent,
    # ``standalone`` renders instead. A prompt must never route to a tool the
    # session cannot call — an actor told "author over there" with no "there"
    # in scope refuses work its visible tools fully support.
    superseded_by: str | None = None
    standalone: PromptText | None = None

    def prompt_text(self, exposed_aliases: frozenset[str]) -> PromptText:
        """The overview text to render given which managers are exposed."""
        if (
            self.superseded_by is not None
            and self.superseded_by not in exposed_aliases
            and self.standalone is not None
        ):
            return self.standalone
        return PromptText(
            domain=self.domain,
            description=self.description,
            use_when=self.use_when,
            examples=self.examples,
            special_note=self.special_note,
        )


# =============================================================================
# Common excluded methods (applies to all managers)
# =============================================================================

_COMMON_EXCLUDED_METHODS: frozenset[str] = frozenset(
    {
        # State management / lifecycle
        "clear",
        # Internal helpers
        "add_tools",
        "get_tools",
    },
)


# =============================================================================
# Canonical Manager Registry (SINGLE SOURCE OF TRUTH)
# =============================================================================

_MANAGER_SPECS: tuple[ManagerSpec, ...] = (
    ManagerSpec(
        manager_alias="contacts",
        manager_registry_key="contacts",
        primitive_class_path="unify.contact_manager.contact_manager.ContactManager",
        excluded_methods=frozenset({"filter_contacts", "update_contact"}),
        priority=3,
        domain="People & Relationships",
        description="People, organizations, contact records (names, emails, phones, roles, locations)",
        use_when="Questions about specific people, contact info, 'who is X?'",
        examples="'Who is our contact at Acme Corp?', 'Find Alice's email', 'Contacts in Berlin?'",
    ),
    ManagerSpec(
        manager_alias="data",
        manager_registry_key="data",
        primitive_class_path="unify.data_manager.data_manager.DataManager",
        # `ingest` is deliberately not exposed here. Storing anything goes
        # through `primitives.ingestion.submit`, which records a run, checkpoints
        # it and can resume it. A direct ingest call has none of that: it blocks
        # the plan for as long as the write takes and leaves nothing to inspect
        # if it dies part-way.
        excluded_methods=frozenset({"ingest"}),
        priority=9,
        domain="Data Operations",
        description=(
            "Read and reshape data already stored in any Unify context "
            "(filter, search, reduce, join, update_rows, vectorize)"
        ),
        use_when=(
            "Querying, aggregating or joining stored tables; updating rows in "
            "place. To *store* new data from anywhere, use primitives.ingestion"
        ),
        examples=(
            "'Filter rows where amount > 1000', 'Join repairs with telematics', "
            "'Sum revenue by region', 'Mark these rows reviewed'"
        ),
        special_note=(
            "DataManager operates on any Unify context path. "
            "Use `describe_table` and `list_tables` to discover schemas. "
            "HARD RULE: push predicates and aggregations into "
            "`filter` / `reduce` / `filter_join` / `reduce_join` / "
            "`update_rows` — never download a large table into Python to "
            "count, aggregate, or decide updates. Prefer one selective "
            "`update_rows(..., filter=...)` over fetch-all then per-row "
            "updates. "
            "STORING new data is not here: use `primitives.ingestion.submit` "
            "for rows from an API or connected app, for files, and for "
            "reshaping one table into another."
        ),
    ),
    ManagerSpec(
        manager_alias="ingestion",
        manager_registry_key="ingestion",
        primitive_class_path=(
            "unify.ingestion_manager.ingestion_manager.IngestionManager"
        ),
        excluded_methods=frozenset(),
        priority=9,
        domain="Storing Data & Files",
        description=(
            "Store data from anywhere into queryable tables or document "
            "collections: rows fetched from an API or connected app, uploaded "
            "and attached files, a whole folder, or a reshape of a stored table"
        ),
        use_when=(
            "Anything that puts NEW data somewhere queryable — API responses, "
            "connected-app pulls, attachments, uploads, folder syncs, table "
            "reshapes"
        ),
        examples=(
            "'Pull my HubSpot deals in and chart them', 'Ingest this PDF', "
            "'Load the whole exports folder', 'Store these API results', "
            "'Make a current-state table out of that event log'"
        ),
        special_note=(
            "ONE VERB: `submit(source, target)` — sources (`RowsSource`, "
            "`FilesSource`, `FolderSource`, `TableSource`) say where data "
            "comes from; targets say where it lands (`TableTarget` for one "
            "queryable table, `CollectionTarget` to keep documents whole, "
            "inner tables extracted alongside); full contract via "
            "help(primitives.ingestion.submit). MANY FILES ARE ONE RUN: "
            "fifteen files each wanting their own table is one "
            "`CollectionTarget(extract_tables=True)`, not fifteen submits — "
            "which throw away the run id that makes the batch answerable. "
            "NOTHING BLOCKS: submit "
            "returns a run handle immediately — poll `get_status(run_id)` "
            "and follow its `next_step`; call `wait(run_id)` only when the "
            "plan genuinely cannot continue. `status.contexts` reports the "
            "exact paths written — query those rather than "
            "guessing. PER-FILE PROGRESS is `status.files`, never "
            "`status.stages`, which counts stages and so says two of fifteen "
            "parsed without saying which two; never report expected rows as "
            "ingested ones. Where it runs is not a caller's choice; every run "
            "checkpoints and is resumable (`retry(run_id)` re-attempts only "
            "what was parked, `pause`/`resume` continue from the last "
            "checkpoint). RE-INGESTING THE SAME SOURCE: declare "
            "`unique_keys` on the `TableTarget` and a second submit updates "
            "those rows in place; without them a re-run appends a second "
            "copy — right for an event log, wrong for current state. IF "
            "INGESTION CANNOT DO IT, SAY SO: on failure read "
            "`get_logs(run_id)`, fix the cause, submit again — never store "
            "the data another way (`primitives.data.create_table` plus "
            "`insert_rows`, or a hand-written loop, writes rows nothing "
            "checkpoints, verifies, or can resume). FOR ROWS FETCHED FROM "
            "AN API the shape is always: fetch → "
            "`submit(RowsSource(rows=...), TableTarget(...))` → `wait` → "
            "read from that table; also the only way to combine two sources, "
            "since API responses cannot be joined directly."
        ),
    ),
    ManagerSpec(
        manager_alias="transcripts",
        manager_registry_key="transcripts",
        primitive_class_path="unify.transcript_manager.transcript_manager.TranscriptManager",
        excluded_methods=frozenset(),
        priority=2,
        domain="Conversation History",
        description="Past messages, conversation history, chat records",
        use_when="Questions about past communications, 'what did X say?'",
        examples="'What did Bob say yesterday?', 'Last message from Alice?', 'Messages mentioning budget?'",
    ),
    ManagerSpec(
        manager_alias="secrets",
        manager_registry_key="secrets",
        primitive_class_path="unify.secret_manager.secret_manager.SecretManager",
        excluded_methods=frozenset(),
        priority=8,
        domain="Credentials & Secrets",
        description="API keys, passwords, tokens, credentials",
        use_when="Managing credentials, API keys, secrets (rarely used in plans)",
        examples="Rarely used directly in plans",
    ),
    ManagerSpec(
        manager_alias="files",
        manager_registry_key="files",
        primitive_class_path="unify.file_manager.managers.file_manager.FileManager",
        excluded_methods=frozenset(
            {
                "exists",
                "list",
                "ingest_files",
                "export_file",
                "export_directory",
                "rename_file",
                "move_file",
                "delete_file",
                "sync",
            },
        ),
        priority=7,
        domain="File Operations & Document Parsing",
        description=(
            "File-specific operations: describing storage layout, listing files, "
            "parsing documents, rendering PDFs/Excel sheets as images"
        ),
        use_when=(
            "Questions about files themselves (metadata, layout, parsing), "
            "document rendering"
        ),
        examples=(
            "'Parse the attached PDF', 'What's in document X?', "
            "'Describe the schema of report.xlsx', 'Render page 3 of the report'"
        ),
    ),
    ManagerSpec(
        manager_alias="actor",
        manager_registry_key="",  # No ManagerRegistry getter - stateless, constructed directly
        primitive_class_path="unify.actor.environments.actor._ActorRunner",
        excluded_methods=frozenset(),
        priority=11,
        domain="Actor Delegation",
        description="Spawn focused sub-actors for isolated multi-step sub-tasks",
        use_when="Task decomposition, isolated reasoning, parallel sub-tasks, context isolation",
        examples="'Delegate research to a sub-actor', 'Spawn an actor to handle data processing'",
    ),
)

# Build lookup dicts for fast access
_MANAGER_BY_ALIAS: Dict[str, ManagerSpec] = {
    spec.manager_alias: spec for spec in _MANAGER_SPECS
}

# Reverse mapping: primitive_class_path -> manager_alias (for deriving alias from stored primitive_class)
_CLASS_PATH_TO_ALIAS: Dict[str, str] = {
    spec.primitive_class_path: spec.manager_alias for spec in _MANAGER_SPECS
}

# sandbox_root -> [specs]: maps each sandbox namespace root to its manager specs.
# Used by construct_sandbox_root() to materialise environment namespaces at
# runtime.  The keys here must stay in sync with the env_namespaces frozenset
# in FunctionManager.add_functions (the storage-time AST detection side).
_SANDBOX_ROOTS: Dict[str, list[ManagerSpec]] = {}
for _spec in _MANAGER_SPECS:
    _SANDBOX_ROOTS.setdefault(_spec.sandbox_root, []).append(_spec)


def construct_sandbox_root(
    root_name: str,
    *,
    primitive_scope: Optional[PrimitiveScope] = None,
) -> Optional[Any]:
    """Construct a fresh root namespace object for a sandbox namespace key.

    This is the factory used by ``FunctionManager._inject_dependencies``
    to satisfy *dotted* entries in a stored function's ``depends_on`` list.
    When a function declares a dependency like ``"primitives.actor.act"`` or
    ``"primitives.contacts.ask"``, ``_inject_dependencies`` extracts the
    root segment (``"primitives"``) and calls this function to obtain a
    live ``Primitives`` instance that provides those methods.

    The returned object is **stateless** — it does not require any ambient
    ContextVars or parent actor state.  This is essential because stored
    functions may be executed outside of a live ``CodeActActor`` sandbox
    (e.g. by the storage-check loop's ``FunctionManager_add_functions``
    tool or by a future caller that only has a FunctionManager).

    Returns ``None`` when *root_name* does not match any known sandbox root.
    """
    specs = _SANDBOX_ROOTS.get(root_name)
    if not specs:
        return None

    from unify.function_manager.primitives.runtime import Primitives

    return Primitives(primitive_scope=primitive_scope)


# =============================================================================
# Routing Guidance for Commonly Confused Manager Pairs
# =============================================================================

_ROUTING_GUIDANCE: List[Dict[str, Any]] = [
    {
        "managers": {"data", "files"},
        "title": "`primitives.data.*` vs `primitives.files.*`",
        "guidance": [
            (
                "data",
                "Use for **ALL analytical and data operations** -- filtering, searching, "
                "aggregating, joining, and data ingestion. Works on "
                "any Unify context (`Data/*`, `Files/*`, `Knowledge/*`). Use when the question "
                "is about DATA INSIDE tables, regardless of where those tables came from. "
                "Also use for ingesting data from APIs/warehouses. Always prefer "
                "server-side `filter=` / `reduce` / join variants over fetching "
                "rows into Python loops.",
            ),
            (
                "files",
                "Use for **file-specific operations** -- describing the storage layout of "
                "an uploaded/received file, listing files in the file registry, parsing "
                "documents, rendering PDFs/Excel sheets as images for visual inspection. "
                "Use when the question is about FILES themselves (metadata, layout, parsing) "
                "or when you need file-path-based context resolution.",
            ),
        ],
        "examples": [
            (
                "What tables exist under Data/examplehousing?",
                "data",
                "primitives.data.list_tables(prefix=...)",
            ),
            (
                "Describe the schema of the repairs table",
                "data",
                "primitives.data.describe_table(...)",
            ),
            (
                "What's in the uploaded PDF?",
                "files",
                "primitives.files.describe(file_path=...)",
            ),
            (
                "Render page 3 of the report as an image",
                "files",
                "primitives.files.render_pdf(...)",
            ),
        ],
    },
]


# =============================================================================
# Stable ID Generation (Matching Old Format)
# =============================================================================


def _get_stable_id(class_name: str, method_name: str) -> int:
    """
    Generate a stable integer ID from class.method name.

    Uses a hash-based approach so IDs are:
    - Deterministic (same name → same ID)
    - Stable across code changes (no positional dependencies)
    - Unique (collision-resistant within practical limits)

    Args:
        class_name: Short class name (e.g., "ContactManager")
        method_name: Method name (e.g., "ask")

    Returns:
        A stable non-negative integer ID within signed 32-bit range.
    """
    key = f"{class_name}.{method_name}"
    # Use first 4 bytes of SHA256, masked to signed int32 range (0x7FFFFFFF)
    # to avoid PostgreSQL integer overflow when sorting by function_id
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


# =============================================================================
# NumPy-style docstring helpers
# =============================================================================


def _get_line_indent(line: str) -> int:
    """Count the number of leading spaces in a line."""
    return len(line) - len(line.lstrip())


def _is_param_declaration_at_level(line: str, base_indent: int) -> bool:
    """Detect whether a line is a parameter declaration at the given indent level.

    A declaration is at ``base_indent`` and matches one of:
    - ``name : type``  (standard single-param)
    - ``name1, name2``  (grouped params sharing a description)
    - ``name1 / name2 : type``  (slash-separated grouped params)
    - ``name``  (bare name, no type annotation)
    """
    stripped = line.strip()
    if not stripped:
        return False
    indent = _get_line_indent(line)
    if indent != base_indent:
        return False
    # Standard "name : type" format
    if " : " in stripped:
        return True
    # Grouped comma-separated names: "a, b, c"
    if "," in stripped:
        parts = [p.strip().rstrip(",") for p in stripped.split(",") if p.strip()]
        if all(re.match(r"^[a-zA-Z_]\w*$", p) for p in parts):
            return True
    # Bare single name
    if re.match(r"^[a-zA-Z_]\w*$", stripped):
        return True
    return False


def _extract_param_names(declaration_line: str) -> list[str]:
    """Extract parameter names from a NumPy-style declaration line.

    Handles: ``name : type``, ``name1, name2``, ``name1 / name2 : type``.
    """
    stripped = declaration_line.strip()
    name_part = stripped.split(" : ")[0] if " : " in stripped else stripped
    # Handle "/" separator (e.g., "_clarification_up_q / _clarification_down_q")
    if " / " in name_part:
        names = [n.strip() for n in name_part.split("/") if n.strip()]
    else:
        names = [n.strip().rstrip(",") for n in name_part.split(",") if n.strip()]
    return names


# =============================================================================
# ToolSurfaceRegistry - Main API
# =============================================================================


class ToolSurfaceRegistry:
    """
    Central registry for tool surface configuration.

    This class provides the API for:
    - Getting manager specs filtered by scope
    - Discovering primitive methods for a manager
    - Generating prompt context and examples
    - Collecting primitive rows for FunctionManager indexing
    - Building row filters for scoped queries
    """

    # Class-level references to canonical data
    MANAGERS = _MANAGER_SPECS
    ROUTING_GUIDANCE = _ROUTING_GUIDANCE

    def __init__(self) -> None:
        """Initialize the registry."""
        # Cache for dynamically loaded classes (avoids repeated imports)
        self._class_cache: Dict[str, Type] = {}

    def manager_specs(self, primitive_scope: PrimitiveScope) -> List[ManagerSpec]:
        """
        Get manager specs for a given scope, sorted by priority.

        Args:
            primitive_scope: The scope defining which managers are exposed.

        Returns:
            List of ManagerSpec for exposed managers, sorted by priority.
        """
        specs = [
            spec
            for spec in _MANAGER_SPECS
            if spec.manager_alias in primitive_scope.scoped_managers
        ]
        return sorted(specs, key=lambda s: s.priority)

    def get_manager_spec(self, manager_alias: str) -> Optional[ManagerSpec]:
        """Get a single manager spec by alias (includes ComputerPrimitives)."""
        return _MANAGER_BY_ALIAS.get(manager_alias)

    def get_function_id(self, manager_alias: str, method_name: str) -> int:
        """Compute the stable function_id for a primitive method.

        This returns the same ID that ``collect_primitives`` produces for rows
        stored in the builtins primitives catalogue, without requiring a DB
        round-trip.

        Args:
            manager_alias: Canonical manager alias (e.g., ``"contacts"``).
            method_name: Method name (e.g., ``"ask"``).

        Returns:
            Stable non-negative integer ID (deterministic, hash-based).

        Raises:
            ValueError: If *manager_alias* is not a known manager.
        """
        spec = _MANAGER_BY_ALIAS.get(manager_alias)
        if spec is None:
            raise ValueError(f"Unknown manager alias: {manager_alias!r}")
        class_name = spec.primitive_class_path.rsplit(".", 1)[-1]
        return _get_stable_id(class_name, method_name)

    def _load_manager_class(self, class_path: str) -> Optional[Type]:
        """Dynamically load a manager class for introspection."""
        if class_path in self._class_cache:
            return self._class_cache[class_path]

        try:
            module_path, class_name = class_path.rsplit(".", 1)
            import importlib

            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            self._class_cache[class_path] = cls
            return cls
        except Exception as e:
            logger.warning(f"Failed to load class {class_path}: {e}")
            return None

    def primitive_methods(self, *, manager_alias: str) -> List[str]:
        """
        Get the list of primitive methods for a manager.

        Discovers methods from @abstractmethod definitions on Base* classes,
        minus common exclusions and per-manager exclusions.

        For ComputerPrimitives, uses the class's _PRIMITIVE_METHODS constant
        since methods are dynamically created via setattr in __init__.

        Args:
            manager_alias: The canonical manager alias.

        Returns:
            Sorted list of method names exposed as primitives.
        """
        spec = _MANAGER_BY_ALIAS.get(manager_alias)
        if not spec:
            logger.warning(f"Unknown manager alias: {manager_alias}")
            return []

        cls = self._load_manager_class(spec.primitive_class_path)
        if cls is None:
            return []

        # Combine common exclusions with per-manager exclusions
        exclude = _COMMON_EXCLUDED_METHODS | spec.excluded_methods

        # Classes that declare _PRIMITIVE_METHODS explicitly (e.g. because
        # methods are dynamically created or not discoverable via @abstractmethod)
        # use that constant as the authoritative method list.
        if hasattr(cls, "_PRIMITIVE_METHODS"):
            return sorted([m for m in cls._PRIMITIVE_METHODS if m not in exclude])

        methods = []

        # Standard case: find @abstractmethod definitions in Base* classes
        for base in cls.__mro__:
            base_name = base.__name__
            # Look for Base* classes (e.g., BaseContactManager, BaseFileManager)
            if not base_name.startswith("Base"):
                continue
            # Skip the root BaseStateManager - we want the specific manager's base
            if base_name == "BaseStateManager":
                continue

            for name, method in vars(base).items():
                if name.startswith("_"):
                    continue
                if name in exclude:
                    continue
                if getattr(method, "__isabstractmethod__", False):
                    if name not in methods:
                        methods.append(name)

        return sorted(methods)

    def tool_names(self, primitive_scope: PrimitiveScope) -> List[str]:
        """
        Get fully-qualified tool names for a scope.

        Args:
            primitive_scope: The scope defining which managers are exposed.

        Returns:
            List of tool names like "primitives.contacts.ask".
        """
        names = []
        for alias in sorted(primitive_scope.scoped_managers):
            for method in self.primitive_methods(manager_alias=alias):
                names.append(f"primitives.{alias}.{method}")
        return names

    def tool_metadata(
        self,
        primitive_scope: PrimitiveScope,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get metadata for all tools in scope.

        Args:
            primitive_scope: The scope defining which managers are exposed.

        Returns:
            Dict mapping tool name to metadata (is_impure, is_steerable).
        """
        metadata = {}
        for name in self.tool_names(primitive_scope):
            # Conservative: all state manager primitives are impure and steerable
            metadata[name] = {
                "is_impure": True,
                "is_steerable": True,
            }
        return metadata

    @staticmethod
    def _resolve_base_method(cls: Optional[Type], method_name: str):
        """Find the canonical method object from the Base* class in the MRO.

        Returns the method object or None.
        """
        if cls is None:
            return None
        for base in cls.__mro__:
            if base.__name__.startswith("Base") and method_name in vars(base):
                return vars(base)[method_name]
        return getattr(cls, method_name, None)

    @staticmethod
    def _format_method_signature(cls: Optional[Type], method_name: str) -> str:
        """Extract a compact signature string for a method.

        Strips ``self`` and any private ``_``-prefixed parameters (internal
        wiring like ``_parent_chat_context`` or ``_clarification_up_q``).

        Falls back to ``...`` when the class is unavailable or introspection
        fails for any reason.
        """
        method = ToolSurfaceRegistry._resolve_base_method(cls, method_name)
        if method is None:
            return "..."
        try:
            sig = inspect.signature(method)
            params = [
                p
                for name, p in sig.parameters.items()
                if name != "self" and not name.startswith("_")
            ]
            return str(sig.replace(parameters=params))
        except (ValueError, TypeError):
            return "..."

    @staticmethod
    def _extract_method_docstring(cls: Optional[Type], method_name: str) -> str:
        """Extract the full docstring for a method from the Base* class.

        Unwraps ``functools.wraps`` / ``__wrapped__`` to reach the original
        docstring.  Returns an empty string when unavailable.
        """
        method = ToolSurfaceRegistry._resolve_base_method(cls, method_name)
        if method is None:
            return ""
        fn = method
        while hasattr(fn, "__wrapped__"):
            fn = fn.__wrapped__
        return inspect.getdoc(fn) or ""

    @staticmethod
    def _extract_section_lines(
        lines: list[str],
        *,
        start_index: int,
        section_name: str,
    ) -> list[str]:
        """Return body lines of a NumPy-style section, or empty if missing."""
        section_lines: list[str] = []
        in_section = False
        for j in range(start_index, len(lines)):
            stripped = lines[j].strip()
            if stripped == section_name:
                in_section = True
                continue
            if in_section and stripped.startswith("---"):
                continue
            if in_section:
                if (
                    stripped
                    and not stripped[0].isspace()
                    and not stripped.startswith("-")
                ):
                    if j + 1 < len(lines) and lines[j + 1].strip().startswith(
                        "---",
                    ):
                        break
                    if re.match(r"^[A-Z][a-zA-Z\s]+$", stripped) and len(stripped) < 30:
                        break
                section_lines.append(lines[j])
        return section_lines

    @staticmethod
    def _extract_summary_and_params(docstring: str) -> str:
        """Extract summary, Parameters, and Anti-patterns from a docstring.

        Returns a compact version containing the summary (up to the first
        blank line), the NumPy-style ``Parameters`` section, and
        ``Anti-patterns`` when present (critical for DataManager
        efficiency). Omits Returns, Raises, Examples, Notes, and internal
        ``_``-prefixed parameters.
        """
        if not docstring:
            return ""

        lines = docstring.splitlines()

        # First paragraph (up to first blank line after content)
        summary_lines: list[str] = []
        i = 0
        for i, line in enumerate(lines):
            if not line.strip():
                if summary_lines:
                    break
            else:
                summary_lines.append(line)

        params_lines = ToolSurfaceRegistry._extract_section_lines(
            lines,
            start_index=i,
            section_name="Parameters",
        )
        anti_lines = ToolSurfaceRegistry._extract_section_lines(
            lines,
            start_index=i,
            section_name="Anti-patterns",
        )

        # Filter out internal _-prefixed parameter entries.
        # Detect the base indentation level from the first non-empty line.
        base_indent = 0
        for line in params_lines:
            if line.strip():
                base_indent = _get_line_indent(line)
                break

        filtered_params: list[str] = []
        skip_entry = False
        for line in params_lines:
            if _is_param_declaration_at_level(line, base_indent):
                names = _extract_param_names(line)
                skip_entry = bool(names) and all(n.startswith("_") for n in names)
            if not skip_entry:
                filtered_params.append(line)

        result = "\n".join(summary_lines)
        params = "\n".join(filtered_params).rstrip()
        if params:
            result += "\n\nParameters\n----------\n" + params
        anti = "\n".join(anti_lines).rstrip()
        if anti:
            result += "\n\nAnti-patterns\n-------------\n" + anti
        return result

    @staticmethod
    def _filter_internal_params_from_docstring(docstring: str) -> str:
        """Remove ``_``-prefixed parameter entries from a full docstring.

        Scans the Parameters section and drops any entry whose parameter
        name starts with ``_`` (including its indented description lines).
        All other docstring content is preserved unchanged.
        """
        if not docstring or "Parameters" not in docstring:
            return docstring

        lines = docstring.splitlines()
        result_lines: list[str] = []
        in_params = False
        skip_entry = False
        param_base_indent: int = 0

        i = 0
        while i < len(lines):
            stripped = lines[i].strip()

            # Detect start of Parameters section
            if not in_params and stripped == "Parameters":
                # Check for the dashes line that follows
                if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                    in_params = True
                    result_lines.append(lines[i])
                    result_lines.append(lines[i + 1])
                    # Detect base indent from the first param declaration
                    for k in range(i + 2, len(lines)):
                        if lines[k].strip():
                            param_base_indent = _get_line_indent(lines[k])
                            break
                    i += 2
                    continue
                else:
                    result_lines.append(lines[i])
                    i += 1
                    continue

            if in_params:
                # Detect next section header (end of Parameters)
                if (
                    stripped
                    and _get_line_indent(lines[i]) <= param_base_indent
                    and not stripped.startswith("-")
                ):
                    is_section = False
                    if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                        is_section = True
                    if re.match(r"^[A-Z][a-zA-Z\s]+$", stripped) and len(stripped) < 30:
                        is_section = True

                    if is_section:
                        in_params = False
                        skip_entry = False
                        result_lines.append(lines[i])
                        i += 1
                        continue

                if _is_param_declaration_at_level(lines[i], param_base_indent):
                    names = _extract_param_names(lines[i])
                    skip_entry = bool(names) and all(n.startswith("_") for n in names)

                if not skip_entry:
                    result_lines.append(lines[i])
            else:
                result_lines.append(lines[i])

            i += 1

        return "\n".join(result_lines)

    def prompt_context(self, primitive_scope: PrimitiveScope) -> str:
        """
        Generate prompt context for exposed managers.

        Structure: routing guidance only (which manager to pick). Method
        docs are not inlined — they are discovered at run time through
        ``FunctionManager_search_functions`` and read via ``help()`` /
        ``inspect.signature`` inside the sandbox.

        Args:
            primitive_scope: The scope defining which managers are exposed.

        Returns:
            Formatted prompt context string.
        """
        specs = self.manager_specs(primitive_scope)
        if not specs:
            return ""

        lines = ["### State manager primitives (`primitives.*`)\n"]
        lines.append(
            "Always available — call by exact name via `execute_function` or "
            "inside `execute_code`. The **Methods** line under each manager "
            "is its complete callable surface (also indexed by FunctionManager "
            "search; read a signature with `help()` / `inspect.signature`): a "
            "name that is not listed does not exist on that manager. Each "
            "manager owns one domain of the assistant's durable state:\n",
        )

        exposed_aliases = primitive_scope.scoped_managers

        # ── Section 1: Brief manager overview (routing-focused) ──
        # Compact by design: one description bullet (+ use-when, + note where
        # a contract demands it) plus the bare method names, so the model
        # never has to guess a manager's verbs from another manager's.
        # Deeper routing between overlapping managers lives in builtin
        # guidance ("choosing between overlapping state managers"); method
        # docs live behind search + help().
        for spec in specs:
            text = spec.prompt_text(exposed_aliases)
            lines.append(f"\n**{text.domain}** → `primitives.{spec.manager_alias}`")
            description = text.description
            if text.use_when:
                description = f"{description}. **Use when**: {text.use_when}"
            lines.append(f"- {description}")
            methods = self.primitive_methods(manager_alias=spec.manager_alias)
            lines.append(
                "- **Methods**: " + ", ".join(f"`{name}`" for name in methods),
            )
            if text.special_note:
                lines.append(f"- **Note**: {text.special_note}")

        # ── Section 2: General rules ──
        # Routing-only by design: handle, mutation and notification doctrine
        # lives once, in the StateManagerEnvironment rules block.
        if len(specs) > 1:
            lines.append("\n**General Rules**:")
            lines.append(
                "- When in doubt between managers, prefer the most specific "
                "domain match; for overlapping pairs (data vs files vs "
                "ingestion) search guidance for "
                '"choosing between overlapping state managers"',
            )
            lines.append(
                "- Typed claim / procedure catalogues (`KnowledgeManager_*`, "
                "`GuidanceManager_*`) are top-level Actor JSON tools, not "
                "`primitives.*`",
            )

        # No per-method docs and no discovery pointer are inlined here:
        # the base prompt's "Sandbox Environment" section carries the
        # FunctionManager-search → help()/inspect introspection bridge.

        return "\n".join(lines)

    def _get_method_metadata(
        self,
        cls: Type,
        method_name: str,
        class_name: str,
        manager_alias: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Extract metadata (signature, docstring) from a class method.

        Handles functools.wraps by looking for __wrapped__ attribute.
        For ComputerPrimitives dynamic methods, looks up docstrings from backend classes.

        Args:
            cls: The class containing the method.
            method_name: Name of the method to introspect.
            class_name: Short class name (used for stable ID generation).
            manager_alias: The canonical alias (e.g. "contacts") for
                building the qualified name ``primitives.{alias}.{method}``.

        Returns:
            Primitive metadata dict, or None if method not found.
        """
        method = getattr(cls, method_name, None)

        if method is None:
            return None

        # Unwrap functools.wraps to get original function metadata
        fn = method
        while hasattr(fn, "__wrapped__"):
            fn = fn.__wrapped__

        qualified_name = f"primitives.{manager_alias}.{method_name}"

        try:
            signature = str(inspect.signature(fn))
        except (ValueError, TypeError):
            signature = "(...)"

        docstring = inspect.getdoc(fn) or ""

        return {
            "name": qualified_name,
            "function_id": _get_stable_id(class_name, method_name),
            "argspec": signature,
            "docstring": docstring,
            "embedding_text": (
                f"Function Name: {qualified_name}\n"
                f"Signature: {signature}\n"
                f"Docstring: {docstring}"
            ),
            "implementation": None,
            "is_primitive": True,
            "depends_on": [],
            "precondition": None,
            "verify": False,
            "guidance_ids": [],
            "primitive_class": cls.__module__ + "." + cls.__name__,
            "primitive_method": method_name,
        }

    def collect_primitives(
        self,
        primitive_scope: Optional[PrimitiveScope] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Introspect primitive classes and return their metadata.

        Auto-discovers primitive methods from each class by inspecting @abstractmethod
        definitions on base classes (minus per-class exclusions), then extracts
        signature and docstring information.

        Each primitive receives a stable `function_id` derived from a hash of its
        fully-qualified name (e.g., "ContactManager.ask"). This ensures IDs are:
        - Deterministic across runs
        - Stable when methods are added/removed (no positional dependencies)
        - Consistent across all deployments

        Args:
            primitive_scope: Optional scope to filter managers. If None, collects all
                           primitives (state managers and ComputerPrimitives).

        Returns:
            Dict mapping qualified_name (e.g. "primitives.contacts.ask") to primitive
            metadata suitable for insertion into the Functions/Primitives context.
        """
        primitives: Dict[str, Dict[str, Any]] = {}

        # Determine which specs to process
        if primitive_scope is None:
            specs_to_process = _MANAGER_SPECS
        else:
            specs_to_process = [
                spec
                for spec in _MANAGER_SPECS
                if spec.manager_alias in primitive_scope.scoped_managers
            ]

        for spec in specs_to_process:
            cls = self._load_manager_class(spec.primitive_class_path)
            if cls is None:
                continue

            class_name = cls.__name__
            method_names = self.primitive_methods(manager_alias=spec.manager_alias)

            for method_name in method_names:
                metadata = self._get_method_metadata(
                    cls,
                    method_name,
                    class_name,
                    spec.manager_alias,
                )
                if metadata is not None:
                    primitives[metadata["name"]] = metadata

        logger.debug(f"Collected {len(primitives)} primitives")
        return primitives

    def compute_primitives_hash(
        self,
        primitives: Optional[Dict[str, Dict[str, Any]]] = None,
        primitive_scope: Optional[PrimitiveScope] = None,
    ) -> str:
        """
        Compute a stable hash of all primitive signatures.

        Used to detect when primitives have changed (docstrings updated, methods
        added/removed) and a sync is needed.

        Args:
            primitives: Optional pre-collected primitives dict. If provided, uses these
                       directly without re-collecting.
            primitive_scope: Optional scope to filter managers. Only used if primitives
                           is not provided.

        Returns:
            16-character hex hash string.
        """
        if primitives is None:
            primitives = self.collect_primitives(primitive_scope)

        return stable_hash_for_rows(
            primitives.values(),
            fields=("name", "argspec", "docstring"),
            digest_chars=16,
            projection="delimited",
        )

    def compute_hash_for_manager(self, manager_alias: str) -> str:
        """
        Compute hash for a single manager's primitives.

        Args:
            manager_alias: The manager to hash.

        Returns:
            16-character hex hash string.
        """
        scope = PrimitiveScope(scoped_managers=frozenset({manager_alias}))
        return self.compute_primitives_hash(primitive_scope=scope)

    def primitive_row_filter(self, primitive_scope: PrimitiveScope) -> str:
        """
        Build a Unify filter expression for scoped primitive queries.

        Uses primitive_class (which is already stored in Function model) to filter,
        avoiding the need for a separate primitive_manager field.

        Args:
            primitive_scope: The scope defining which managers to include.

        Returns:
            Filter expression using membership over primitive class paths.
        """
        # Collect class paths for scoped managers
        class_paths = []
        for alias in primitive_scope.scoped_managers:
            spec = _MANAGER_BY_ALIAS.get(alias)
            if spec:
                class_paths.append(spec.primitive_class_path)

        if not class_paths:
            return "False"
        class_list = ", ".join(json.dumps(cp) for cp in sorted(class_paths))
        return f"primitive_class in [{class_list}]"


# =============================================================================
# Module-level convenience functions (matching old API)
# =============================================================================


def collect_primitives() -> Dict[str, Dict[str, Any]]:
    """
    Introspect all registered primitive classes and return their metadata.

    This is a convenience function matching the old module-level API.
    Delegates to ToolSurfaceRegistry.collect_primitives().

    Returns:
        Dict mapping qualified_name (e.g. "primitives.contacts.ask") to primitive
        metadata suitable for insertion into the Functions/Primitives context.
    """
    return get_registry().collect_primitives()


def compute_primitives_hash(primitives: Dict[str, Dict[str, Any]]) -> str:
    """
    Compute a stable hash of all primitive signatures.

    This is a convenience function matching the old module-level API.
    Delegates to ToolSurfaceRegistry.compute_primitives_hash().

    Args:
        primitives: Dict from collect_primitives().

    Returns:
        16-character hex hash string.
    """
    return get_registry().compute_primitives_hash(primitives=primitives)


def get_primitive_sources() -> List[tuple[Type, List[str]]]:
    """
    Get all primitive classes with their discovered method names.

    Returns a list of (class, method_names) tuples. This provides access to
    the auto-discovered primitives for code that needs to iterate over them.

    Returns:
        List of (class_object, [method_names]) tuples.
    """
    registry = get_registry()
    result = []
    for spec in _MANAGER_SPECS:
        cls = registry._load_manager_class(spec.primitive_class_path)
        if cls is not None:
            methods = registry.primitive_methods(manager_alias=spec.manager_alias)
            result.append((cls, methods))
    return result


# Module-level singleton for convenience
_registry_instance: Optional[ToolSurfaceRegistry] = None


def get_registry() -> ToolSurfaceRegistry:
    """Get the singleton registry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = ToolSurfaceRegistry()
    return _registry_instance
