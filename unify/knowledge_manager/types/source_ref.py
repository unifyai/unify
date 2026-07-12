"""Provenance references attached to knowledge claims (discriminated by kind)."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field, TypeAdapter


class SourceKind(StrEnum):
    user_statement = "user_statement"
    transcript = "transcript"
    file = "file"
    data = "data"
    web = "web"
    actor_trajectory = "actor_trajectory"
    derived_from_knowledge = "derived_from_knowledge"
    manual = "manual"
    contact = "contact"


class _SourceRefBase(BaseModel):
    note: Optional[str] = Field(
        None,
        description="Freeform note describing how this source supports the claim.",
    )


class UserStatementSourceRef(_SourceRefBase):
    kind: Literal["user_statement"] = "user_statement"


class ManualSourceRef(_SourceRefBase):
    kind: Literal["manual"] = "manual"


class ActorTrajectorySourceRef(_SourceRefBase):
    kind: Literal["actor_trajectory"] = "actor_trajectory"


class FileSourceRef(_SourceRefBase):
    kind: Literal["file"] = "file"
    file_id: Optional[int] = Field(
        None,
        description="FileRecords.file_id when the file row is known.",
    )
    filepath: Optional[str] = Field(
        None,
        description="Workspace-relative filepath when kind is file.",
    )


class DataSourceRef(_SourceRefBase):
    kind: Literal["data"] = "data"
    context: str = Field(
        ...,
        description=(
            "Unify context path for the data table/corpus this claim "
            "was distilled from (no global data_id — path is the identity)."
        ),
        min_length=1,
    )


class TranscriptSourceRef(_SourceRefBase):
    kind: Literal["transcript"] = "transcript"
    exchange_id: int = Field(
        ...,
        description="Transcript exchange id when kind is transcript.",
    )


class WebSourceRef(_SourceRefBase):
    kind: Literal["web"] = "web"
    url: str = Field(
        ...,
        description="URL when kind is web.",
        min_length=1,
    )


class DerivedFromKnowledgeSourceRef(_SourceRefBase):
    kind: Literal["derived_from_knowledge"] = "derived_from_knowledge"
    knowledge_id: int = Field(
        ...,
        description="Parent knowledge_id when kind is derived_from_knowledge.",
    )


class ContactSourceRef(_SourceRefBase):
    kind: Literal["contact"] = "contact"
    contact_id: int = Field(
        ...,
        description="Contacts.contact_id when kind is contact.",
    )


SourceRef = Annotated[
    Union[
        UserStatementSourceRef,
        ManualSourceRef,
        ActorTrajectorySourceRef,
        FileSourceRef,
        DataSourceRef,
        TranscriptSourceRef,
        WebSourceRef,
        DerivedFromKnowledgeSourceRef,
        ContactSourceRef,
    ],
    Field(discriminator="kind"),
]

_SOURCE_REF_ADAPTER: TypeAdapter = TypeAdapter(SourceRef)


def coerce_source_refs(value) -> List[SourceRef]:
    """Normalize list/dict/None payloads into discriminated ``SourceRef`` list."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError("source_refs must be a list")
    out: List[SourceRef] = []
    for item in value:
        if isinstance(
            item,
            (
                UserStatementSourceRef,
                ManualSourceRef,
                ActorTrajectorySourceRef,
                FileSourceRef,
                DataSourceRef,
                TranscriptSourceRef,
                WebSourceRef,
                DerivedFromKnowledgeSourceRef,
                ContactSourceRef,
            ),
        ):
            out.append(item)
        else:
            out.append(_SOURCE_REF_ADAPTER.validate_python(item))
    return out


def source_ref_to_dict(ref: SourceRef) -> dict:
    """Serialize one SourceRef for Orchestra log payloads."""
    return ref.model_dump(mode="json")
