from __future__ import annotations

from typing import List, Optional

from pydantic import Field, field_validator, model_validator

from unify.common.authorship import AuthoredRow
from unify.common.stale_reason import StaleReason, coerce_stale_reasons

from ...image_manager.types import AnnotatedImageRefs

UNASSIGNED = -1


class Guidance(AuthoredRow):
    guidance_id: int = Field(
        default=UNASSIGNED,
        description="Auto-incrementing unique identifier for the guidance entry",
        ge=UNASSIGNED,
    )
    title: str = Field(
        description="Short-form title of the guidance (a few words)",
        min_length=1,
        max_length=200,
    )
    content: str = Field(
        description="Full description of the guidance; may align with images",
        min_length=1,
    )
    images: AnnotatedImageRefs = Field(
        default_factory=lambda: AnnotatedImageRefs.model_validate([]),
        description=(
            "List of annotated image references aligned to the text. Each entry must be an AnnotatedImageRef."
        ),
    )

    function_ids: List[int] = Field(
        default_factory=list,
        description=(
            "List of Function.function_id values that this guidance is relevant for. "
            "Represents a many-to-many relationship between Guidance and Functions."
        ),
    )
    stale_reasons: List[StaleReason] = Field(
        default_factory=list,
        description="Structured records for related functions that no longer resolve.",
    )

    is_builtin: bool = Field(
        default=False,
        description=(
            "True for read-only platform builtin guidance from the global "
            "catalogue; False for tenant-authored entries."
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
            "Hash of source-defined custom guidance for sync detection. "
            "None for user-added entries or builtins."
        ),
    )

    # Images are a list-based AnnotatedImageRefs container (persisted as a plain list in the backend).

    @field_validator("is_builtin", mode="before")
    @classmethod
    def _validate_is_builtin(cls, v):
        if v is None:
            return False
        return v

    @field_validator("function_ids", mode="before")
    @classmethod
    def _validate_function_ids(cls, v):
        """Ensure function_ids is a list[int]. None → []. Coerce values to int."""
        if v is None:
            return []
        if not isinstance(v, list):
            raise TypeError("function_ids must be a list[int]")
        out: list[int] = []
        for item in v:
            try:
                out.append(int(item))
            except Exception as exc:
                raise ValueError("function_ids must contain integers") from exc
        return out

    @field_validator("stale_reasons", mode="before")
    @classmethod
    def _validate_stale_reasons(cls, v):
        return coerce_stale_reasons(v)

    @model_validator(mode="before")
    @classmethod
    def _inject_sentinel(cls, data: dict) -> dict:
        data.setdefault("guidance_id", UNASSIGNED)
        return data

    def to_post_json(self) -> dict:
        exclude = {"guidance_id"} if self.guidance_id == UNASSIGNED else set()
        return self.model_dump(mode="json", exclude=exclude)
