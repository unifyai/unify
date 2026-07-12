from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from unify.common.authorship import AuthoredRow
from unify.common.stale_reason import StaleReason, coerce_stale_reasons

from .source_ref import SourceRef, coerce_source_refs

UNASSIGNED = -1


class KnowledgeKind(StrEnum):
    fact = "fact"
    policy = "policy"
    definition = "definition"
    decision = "decision"
    constraint = "constraint"
    insight = "insight"
    preference = "preference"


class KnowledgeStatus(StrEnum):
    active = "active"
    superseded = "superseded"
    invalidated = "invalidated"


class Knowledge(AuthoredRow):
    """One typed claim in the Knowledge ledger."""

    knowledge_id: int = Field(
        default=UNASSIGNED,
        description="Auto-incrementing unique identifier for the knowledge claim",
        ge=UNASSIGNED,
    )
    title: str = Field(
        description="Short-form title of the claim (a few words)",
        min_length=1,
        max_length=200,
    )
    content: str = Field(
        description="Full claim text / body",
        min_length=1,
    )
    kind: KnowledgeKind = Field(
        default=KnowledgeKind.fact,
        description="Claim kind: fact, policy, definition, decision, constraint, insight, preference.",
    )
    topics: List[str] = Field(
        default_factory=list,
        description="Freeform topic tags for filtering and discovery.",
    )
    source_refs: List[SourceRef] = Field(
        default_factory=list,
        description=(
            "Provenance references supporting this claim. Discriminated by "
            "``kind`` (user_statement, manual, actor_trajectory, file, data, "
            "transcript, web, derived_from_knowledge, contact)."
        ),
    )
    confidence: Optional[float] = Field(
        None,
        description="Optional confidence in [0, 1].",
        ge=0.0,
        le=1.0,
    )
    observed_at: Optional[datetime] = Field(
        None,
        description="When the underlying observation was made.",
    )
    valid_from: Optional[datetime] = Field(
        None,
        description="Start of the claim's validity window, if known.",
    )
    valid_until: Optional[datetime] = Field(
        None,
        description="End of the claim's validity window, if known.",
    )
    status: KnowledgeStatus = Field(
        default=KnowledgeStatus.active,
        description="Lifecycle status: active, superseded, or invalidated.",
    )
    supersedes_ids: List[int] = Field(
        default_factory=list,
        description="knowledge_id values this claim replaces.",
    )
    superseded_by_id: Optional[int] = Field(
        None,
        description="knowledge_id of the claim that replaced this one, if any.",
    )
    stale_reasons: List[StaleReason] = Field(
        default_factory=list,
        description=(
            "Structured link debt when provenance no longer resolves "
            "(missing file/contact/data/derived knowledge, etc.)."
        ),
    )
    is_builtin: bool = Field(
        default=False,
        description=(
            "True for read-only platform builtin knowledge; "
            "False for tenant-authored entries."
        ),
    )
    custom_key: Optional[str] = Field(
        None,
        description=(
            "Stable source-defined key for sync identity. "
            "None for user-added entries."
        ),
    )
    custom_hash: Optional[str] = Field(
        None,
        description=(
            "Hash of source-defined custom knowledge for sync detection. "
            "None for user-added entries or builtins."
        ),
    )

    @field_validator("is_builtin", mode="before")
    @classmethod
    def _validate_is_builtin(cls, v):
        if v is None:
            return False
        return v

    @field_validator("kind", mode="before")
    @classmethod
    def _coerce_kind(cls, v):
        if v is None:
            return KnowledgeKind.fact
        if isinstance(v, KnowledgeKind):
            return v
        return KnowledgeKind(str(v))

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v):
        if v is None:
            return KnowledgeStatus.active
        if isinstance(v, KnowledgeStatus):
            return v
        # Dropped status: treat legacy "orphaned" rows as active + debt elsewhere.
        if str(v) == "orphaned":
            return KnowledgeStatus.active
        return KnowledgeStatus(str(v))

    @field_validator("topics", mode="before")
    @classmethod
    def _validate_topics(cls, v):
        if v is None:
            return []
        if not isinstance(v, list):
            raise TypeError("topics must be a list[str]")
        return [str(item) for item in v]

    @field_validator("source_refs", mode="before")
    @classmethod
    def _validate_source_refs(cls, v):
        return coerce_source_refs(v)

    @field_validator("stale_reasons", mode="before")
    @classmethod
    def _validate_stale_reasons(cls, v):
        return coerce_stale_reasons(v)

    @field_validator("supersedes_ids", mode="before")
    @classmethod
    def _validate_int_lists(cls, v):
        if v is None:
            return []
        if not isinstance(v, list):
            raise TypeError("expected a list[int]")
        out: list[int] = []
        for item in v:
            try:
                out.append(int(item))
            except Exception as exc:
                raise ValueError("list must contain integers") from exc
        return out

    @model_validator(mode="before")
    @classmethod
    def _inject_sentinel(cls, data: dict) -> dict:
        if isinstance(data, dict):
            data.setdefault("knowledge_id", UNASSIGNED)
            # Strip removed skill-link fields from legacy payloads.
            data.pop("related_function_ids", None)
            data.pop("related_guidance_ids", None)
        return data

    def to_post_json(self) -> dict:
        exclude = {"knowledge_id"} if self.knowledge_id == UNASSIGNED else set()
        return self.model_dump(mode="json", exclude=exclude)
