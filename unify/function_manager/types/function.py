from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any

from unify.common.stale_reason import StaleReason, coerce_stale_reasons


class Function(BaseModel):
    """
    Represents a Python function stored in the FunctionManager.

    A function is either user-defined (with implementation source code) or a
    primitive (a primitive namespace method with no stored implementation).
    """

    function_id: Optional[int] = Field(
        None,
        description=(
            "Unique identifier for the function. "
            "Auto-assigned for user functions, explicit stable IDs for primitives."
        ),
    )
    name: str = Field(
        ...,
        description="The name of the function.",
        json_schema_extra={"ui_editable": True},
    )
    argspec: str = Field(
        ...,
        description="The function's signature, e.g. '(x: int, y: int) -> int'.",
        json_schema_extra={"ui_editable": True},
    )
    docstring: str = Field(
        "",
        description="The docstring of the function.",
        json_schema_extra={"ui_editable": True},
    )
    implementation: Optional[str] = Field(
        None,
        description=(
            "The full source code of the function. "
            "None for primitives (implementation lives in Python class)."
        ),
        json_schema_extra={"ui_editable": True},
    )
    depends_on: List[str] = Field(
        default_factory=list,
        description=(
            "Functions this function depends on, auto-detected from the AST "
            "at storage time. Bare names (e.g. 'helper') are compositional "
            "functions. Dotted names (e.g. 'primitives.actor.act') are "
            "environment namespaces; root segments resolve to fresh instances."
        ),
    )
    stale_reasons: List[StaleReason] = Field(
        default_factory=list,
        description="Structured records for declared dependencies that no longer resolve.",
    )
    precondition: Optional[Dict[str, Any]] = Field(
        None,
        description="A dictionary representing the state required before the function can be run, e.g., {'url': '...'}.",
        json_schema_extra={"ui_editable": True},
    )

    guidance_ids: List[int] = Field(
        default_factory=list,
        description=(
            "List of Guidance.guidance_id values that reference this function; "
            "represents the inverse many-to-many relationship."
        ),
        json_schema_extra={"ui_editable": True},
    )

    # Primitive-specific fields
    is_primitive: bool = Field(
        False,
        description=(
            "Whether this is an action primitive (primitive namespace method) rather than "
            "a user-defined function. Primitives have no stored implementation."
        ),
    )

    primitive_class: Optional[str] = Field(
        None,
        description="Fully-qualified class path for primitive execution routing.",
    )

    primitive_method: Optional[str] = Field(
        None,
        description="Method name on the primitive class.",
    )

    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Source-specific metadata for function subtypes. Provider-backed "
            "integration rows store catalogue-level tool metadata here; "
            "per-user connection state is resolved live and never stored on "
            "Function rows."
        ),
    )

    dependencies: List[str] = Field(
        default_factory=list,
        description=(
            "PEP 508 requirement strings for the third-party packages the "
            "implementation imports, e.g. ['pandas>=2.0', 'tabulate']. "
            "Installed into the workspace environment before the function runs."
        ),
        json_schema_extra={"ui_editable": True},
    )

    # ── Usage trace: the memory strength behind activation-weighted
    # retrieval (see function_manager/activation.py). Written fire-and-
    # forget at the execution choke points; read at query time by
    # search_functions to compute standing. Never consulted by execution
    # itself — a dormant function still runs when addressed directly.
    created_at: Optional[str] = Field(
        None,
        description=(
            "ISO timestamp of the row's creation. Creation counts as the "
            "function's first activation event (the newborn grace), so a "
            "fresh function surfaces long enough to earn its first call."
        ),
    )
    usage_calls: int = Field(
        0,
        description="Total recorded invocations across all execution paths.",
    )
    usage_last_called_at: Optional[str] = Field(
        None,
        description="ISO timestamp of the most recent recorded invocation.",
    )
    usage_recent_calls: List[str] = Field(
        default_factory=list,
        description=(
            "Bounded window of recent invocation timestamps (ISO), kept for "
            "rhythm estimation — decay runs against the function's own "
            "median inter-use interval, not a global half-life."
        ),
    )
    usage_search_hits: int = Field(
        0,
        description=(
            "How often search surfaced this function. Retrieved-but-never-"
            "called is a near-miss relevance signal, distinct from unused."
        ),
    )

    @field_validator("stale_reasons", mode="before")
    @classmethod
    def _validate_stale_reasons(cls, v):
        return coerce_stale_reasons(v)
