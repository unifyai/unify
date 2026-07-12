"""Structured link-debt records shared by Knowledge, Guidance, and Functions."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

DepKind = Literal[
    "function",
    "file",
    "contact",
    "data",
    "depends_on",
    "knowledge",
    "guidance",
    "transcript",
]


class StaleReason(BaseModel):
    """One broken or missing dependency link on an authored artifact.

    Persist as plain dicts via ``model_dump(mode="json")``. Non-empty
    ``stale_reasons`` on a row means the artifact has link debt: still
    discoverable, but second-class until repaired via explicit update /
    re-link (or, for compositional ``depends_on`` names, when the same
    bare name is re-added). Never invent new associations from these
    records alone.
    """

    kind: Literal["missing_dependency"] = Field(
        "missing_dependency",
        description=(
            "Link-debt class. Today only ``missing_dependency`` — the "
            "referenced dependee was deleted, renamed, or otherwise no "
            "longer resolves."
        ),
    )
    dep_kind: DepKind = Field(
        ...,
        description=(
            "What kind of dependency broke: ``function`` (Guidance "
            "function_ids), ``file`` / ``contact`` / ``data`` / "
            "``transcript`` / ``knowledge`` (Knowledge source_refs), "
            "``depends_on`` (compositional Function name edge), or "
            "``guidance`` (rare inverse cleanup)."
        ),
    )
    id: Optional[int] = Field(
        None,
        description=(
            "Numeric id of the missing dependee when known "
            "(function_id, file_id, contact_id, knowledge_id, …). "
            "Snapshot before FK CASCADE pops the live pointer."
        ),
    )
    name: Optional[str] = Field(
        None,
        description=(
            "Human / bare name of the missing dependee when known "
            "(e.g. function name, depends_on bare name). Used for "
            "display and for name-keyed depends_on re-validation."
        ),
    )
    path: Optional[str] = Field(
        None,
        description="Workspace filepath when the missing dependee was a file.",
    )
    context: Optional[str] = Field(
        None,
        description=(
            "Unify context path when the missing dependee was a data "
            "context (Knowledge source_refs kind=data)."
        ),
    )
    message: str = Field(
        "",
        description=(
            "Short human-readable explanation for Actor/Console chips, "
            'e.g. "missing function_id=42 name=export_slides".'
        ),
    )

    @field_validator("dep_kind", mode="before")
    @classmethod
    def _coerce_dep_kind(cls, v):
        return str(v)


def coerce_stale_reasons(value) -> List[StaleReason]:
    """Normalize list/dict/None payloads into ``list[StaleReason]``."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError("stale_reasons must be a list")
    out: List[StaleReason] = []
    for item in value:
        if isinstance(item, StaleReason):
            out.append(item)
        elif isinstance(item, str):
            # Legacy / accidental string entries → wrap as message-only debt.
            out.append(
                StaleReason(dep_kind="depends_on", message=item),
            )
        else:
            out.append(StaleReason.model_validate(item))
    return out


def stale_reason_key(reason: StaleReason) -> tuple:
    """Dedup key for merging stale reason lists."""
    return (
        reason.kind,
        reason.dep_kind,
        reason.id,
        reason.name,
        reason.path,
        reason.context,
    )


def merge_stale_reasons(
    existing: List[StaleReason] | None,
    *additions: StaleReason,
) -> List[StaleReason]:
    """Append new reasons without duplicating identical debt entries."""
    out = list(coerce_stale_reasons(existing))
    seen = {stale_reason_key(r) for r in out}
    for reason in additions:
        key = stale_reason_key(reason)
        if key not in seen:
            out.append(reason)
            seen.add(key)
    return out
