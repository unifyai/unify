"""KnowledgeManager typed models."""

from unify.common.stale_reason import StaleReason

from .knowledge import Knowledge, KnowledgeKind, KnowledgeStatus, UNASSIGNED
from .meta import KnowledgeMeta
from .source_ref import (
    ContactSourceRef,
    DataSourceRef,
    DerivedFromKnowledgeSourceRef,
    FileSourceRef,
    SourceKind,
    SourceRef,
    coerce_source_refs,
)

__all__ = [
    "ContactSourceRef",
    "DataSourceRef",
    "DerivedFromKnowledgeSourceRef",
    "FileSourceRef",
    "Knowledge",
    "KnowledgeKind",
    "KnowledgeMeta",
    "KnowledgeStatus",
    "SourceKind",
    "SourceRef",
    "StaleReason",
    "UNASSIGNED",
    "coerce_source_refs",
]
