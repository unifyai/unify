from __future__ import annotations

import functools
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .base import BaseKnowledgeManager
from .types.knowledge import Knowledge, KnowledgeKind, KnowledgeStatus
from .types.source_ref import SourceKind, SourceRef, coerce_source_refs
from ..common.stale_reason import (
    StaleReason,
    merge_stale_reasons,
    stale_reason_key,
)
from ..common.simulated import (
    maybe_tool_log_scheduled,
    maybe_tool_log_completed,
)


class SimulatedKnowledgeManager(BaseKnowledgeManager):
    """Drop-in replacement for KnowledgeManager with an in-memory store."""

    def __init__(
        self,
        description: str = "nothing fixed, make up some imaginary scenario",
        *,
        log_events: bool = False,
        rolling_summary_in_prompts: bool = True,
        simulation_guidance: Optional[str] = None,
        hold_completion: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self._description = description
        self._log_events = log_events
        self._rolling_summary_in_prompts = rolling_summary_in_prompts
        self._simulation_guidance = simulation_guidance
        self._hold_completion = hold_completion
        self._entries: Dict[int, Knowledge] = {}
        self._next_id: int = 1

    @functools.wraps(BaseKnowledgeManager.search, updated=())
    def search(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> List[Knowledge]:
        return [
            c for c in self._entries.values() if c.status == KnowledgeStatus.active
        ][:k]

    @functools.wraps(BaseKnowledgeManager.filter, updated=())
    def filter(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Knowledge]:
        rows = list(self._entries.values())
        if filter is not None:
            matched = []
            for claim in rows:
                try:
                    if eval(filter, {"__builtins__": {}}, claim.model_dump()):
                        matched.append(claim)
                except Exception:
                    pass
            rows = matched
            if "status" not in filter:
                rows = [c for c in rows if c.status == KnowledgeStatus.active]
        else:
            rows = [c for c in rows if c.status == KnowledgeStatus.active]
        return rows[offset : offset + limit]

    @functools.wraps(BaseKnowledgeManager.get_knowledge, updated=())
    def get_knowledge(
        self,
        *,
        knowledge_id: int,
    ) -> Knowledge:
        entry = self._entries.get(knowledge_id)
        if entry is None:
            raise ValueError(f"No knowledge found with knowledge_id {knowledge_id}.")
        return entry

    @functools.wraps(BaseKnowledgeManager.add_knowledge, updated=())
    def add_knowledge(
        self,
        *,
        title: str,
        content: str,
        kind: KnowledgeKind | str = KnowledgeKind.fact,
        topics: Optional[List[str]] = None,
        source_refs: Optional[List[SourceRef | dict]] = None,
        confidence: Optional[float] = None,
        observed_at: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        destination: str | None = None,
    ) -> "ToolOutcome":
        kid = self._next_id
        self._next_id += 1
        self._entries[kid] = Knowledge(
            knowledge_id=kid,
            title=title,
            content=content,
            kind=kind,
            topics=topics or [],
            source_refs=coerce_source_refs(source_refs),
            confidence=confidence,
            observed_at=observed_at,
            valid_from=valid_from,
            valid_until=valid_until,
        )
        self.reconcile_sources(knowledge_ids=[kid], destination=destination)
        return {
            "outcome": "knowledge created successfully",
            "details": {"knowledge_id": kid},
        }

    @functools.wraps(BaseKnowledgeManager.update_knowledge, updated=())
    def update_knowledge(
        self,
        *,
        knowledge_id: int,
        title: Optional[str] = None,
        content: Optional[str] = None,
        kind: Optional[KnowledgeKind | str] = None,
        topics: Optional[List[str]] = None,
        source_refs: Optional[List[SourceRef | dict]] = None,
        confidence: Optional[float] = None,
        observed_at: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        destination: str | None = None,
    ) -> "ToolOutcome":
        existing = self._entries.get(knowledge_id)
        if existing is None:
            raise ValueError(
                f"No knowledge found with knowledge_id {knowledge_id} to update.",
            )
        updates: Dict[str, Any] = {}
        if title is not None:
            updates["title"] = title
        if content is not None:
            updates["content"] = content
        if kind is not None:
            updates["kind"] = kind
        if topics is not None:
            updates["topics"] = topics
        if source_refs is not None:
            updates["source_refs"] = coerce_source_refs(source_refs)
            updates["stale_reasons"] = []
        if confidence is not None:
            updates["confidence"] = confidence
        if observed_at is not None:
            updates["observed_at"] = observed_at
        if valid_from is not None:
            updates["valid_from"] = valid_from
        if valid_until is not None:
            updates["valid_until"] = valid_until
        if not updates:
            raise ValueError("At least one field must be provided for an update.")
        self._entries[knowledge_id] = existing.model_copy(update=updates)
        self.reconcile_sources(
            knowledge_ids=[knowledge_id],
            destination=destination,
        )
        return {
            "outcome": "knowledge updated",
            "details": {"knowledge_id": knowledge_id},
        }

    @functools.wraps(BaseKnowledgeManager.delete_knowledge, updated=())
    def delete_knowledge(
        self,
        *,
        knowledge_id: int,
        destination: str | None = None,
    ) -> "ToolOutcome":
        if knowledge_id not in self._entries:
            raise ValueError(
                f"No knowledge found with knowledge_id {knowledge_id} to delete.",
            )
        reason = StaleReason(
            dep_kind="knowledge",
            id=int(knowledge_id),
            message=f"missing knowledge_id={int(knowledge_id)}",
        )
        for claim in list(self._entries.values()):
            if any(
                ref.kind == SourceKind.derived_from_knowledge
                and int(ref.knowledge_id) == int(knowledge_id)
                for ref in claim.source_refs or []
            ):
                self._append_stale_reasons(
                    knowledge_ids=[claim.knowledge_id],
                    reasons=[reason],
                )
        del self._entries[knowledge_id]
        return {
            "outcome": "knowledge deleted",
            "details": {"knowledge_id": knowledge_id},
        }

    @functools.wraps(BaseKnowledgeManager.invalidate_knowledge, updated=())
    def invalidate_knowledge(
        self,
        *,
        knowledge_id: int,
        destination: str | None = None,
    ) -> "ToolOutcome":
        existing = self._entries.get(knowledge_id)
        if existing is None:
            raise ValueError(
                f"No knowledge found with knowledge_id {knowledge_id} to invalidate.",
            )
        self._entries[knowledge_id] = existing.model_copy(
            update={"status": KnowledgeStatus.invalidated},
        )
        return {
            "outcome": "knowledge invalidated",
            "details": {"knowledge_id": knowledge_id},
        }

    @functools.wraps(BaseKnowledgeManager.supersede_knowledge, updated=())
    def supersede_knowledge(
        self,
        *,
        old_knowledge_id: int,
        title: Optional[str] = None,
        content: Optional[str] = None,
        kind: Optional[KnowledgeKind | str] = None,
        topics: Optional[List[str]] = None,
        source_refs: Optional[List[SourceRef | dict]] = None,
        confidence: Optional[float] = None,
        observed_at: Optional[datetime] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        new_knowledge_id: Optional[int] = None,
        destination: str | None = None,
    ) -> "ToolOutcome":
        old = self._entries.get(old_knowledge_id)
        if old is None:
            raise ValueError(
                f"No knowledge found with knowledge_id {old_knowledge_id} to supersede.",
            )
        if new_knowledge_id is None:
            if not title or not content:
                raise ValueError(
                    "title and content are required when creating a replacement claim.",
                )
            result = self.add_knowledge(
                title=title,
                content=content,
                kind=kind or KnowledgeKind.fact,
                topics=topics,
                source_refs=source_refs,
                confidence=confidence,
                observed_at=observed_at,
                valid_from=valid_from,
                valid_until=valid_until,
            )
            new_knowledge_id = int(result["details"]["knowledge_id"])
            new = self._entries[new_knowledge_id]
            self._entries[new_knowledge_id] = new.model_copy(
                update={"supersedes_ids": [old_knowledge_id]},
            )
        else:
            new = self._entries.get(new_knowledge_id)
            if new is None:
                raise ValueError(
                    f"No knowledge found with knowledge_id {new_knowledge_id}.",
                )
            supersedes = list(new.supersedes_ids or [])
            if old_knowledge_id not in supersedes:
                supersedes.append(old_knowledge_id)
            self._entries[new_knowledge_id] = new.model_copy(
                update={"supersedes_ids": supersedes},
            )
        self._entries[old_knowledge_id] = old.model_copy(
            update={
                "status": KnowledgeStatus.superseded,
                "superseded_by_id": new_knowledge_id,
            },
        )
        return {
            "outcome": "knowledge superseded",
            "details": {
                "old_knowledge_id": old_knowledge_id,
                "new_knowledge_id": new_knowledge_id,
            },
        }

    def _append_stale_reasons(
        self,
        *,
        knowledge_ids: List[int],
        reasons: List[StaleReason],
        destination: str | None = None,
    ) -> None:
        """Append deduplicated link debt to claims before dependency deletion."""
        for knowledge_id in knowledge_ids:
            claim = self._entries.get(knowledge_id)
            if claim is None:
                continue
            merged = merge_stale_reasons(claim.stale_reasons, *reasons)
            self._entries[knowledge_id] = claim.model_copy(
                update={"stale_reasons": merged},
            )

    def mark_stale_for_missing_source(
        self,
        *,
        knowledge_ids: List[int],
        reason: StaleReason | dict,
        destination: str | None = None,
    ) -> None:
        """Snapshot source-link debt before an external dependency is deleted."""
        stale_reason = (
            reason
            if isinstance(reason, StaleReason)
            else StaleReason.model_validate(reason)
        )
        self._append_stale_reasons(
            knowledge_ids=knowledge_ids,
            reasons=[stale_reason],
            destination=destination,
        )

    @staticmethod
    def _reason_identity(reason: StaleReason) -> tuple[str, object] | None:
        if (
            reason.dep_kind in {"file", "contact", "knowledge"}
            and reason.id is not None
        ):
            return reason.dep_kind, int(reason.id)
        if reason.dep_kind == "data" and reason.context:
            return "data", reason.context.strip("/")
        return None

    @staticmethod
    def _ref_identity(ref: SourceRef) -> tuple[str, object] | None:
        if ref.kind == SourceKind.file and ref.file_id is not None:
            return "file", int(ref.file_id)
        if ref.kind == SourceKind.contact:
            return "contact", int(ref.contact_id)
        if ref.kind == SourceKind.derived_from_knowledge:
            return "knowledge", int(ref.knowledge_id)
        if ref.kind == SourceKind.data:
            return "data", ref.context.strip("/")
        return None

    @functools.wraps(BaseKnowledgeManager.reconcile_sources, updated=())
    def reconcile_sources(
        self,
        *,
        knowledge_ids: Optional[List[int]] = None,
        destination: str | None = None,
    ) -> "ToolOutcome":
        targets = (
            [self._entries[i] for i in knowledge_ids if i in self._entries]
            if knowledge_ids
            else [
                c for c in self._entries.values() if c.status == KnowledgeStatus.active
            ]
        )
        stale_knowledge_ids: list[int] = []
        for claim in targets:
            declared_identities = {
                identity
                for ref in claim.source_refs
                if (identity := self._ref_identity(ref)) is not None
            }
            preserved = [
                reason
                for reason in claim.stale_reasons
                if self._reason_identity(reason) not in declared_identities
            ]
            missing: list[StaleReason] = []
            for ref in claim.source_refs or []:
                if (
                    ref.kind == SourceKind.derived_from_knowledge
                    and ref.knowledge_id not in self._entries
                ):
                    missing.append(
                        StaleReason(
                            dep_kind="knowledge",
                            id=ref.knowledge_id,
                            message=f"missing knowledge_id={ref.knowledge_id}",
                        ),
                    )
            refreshed = merge_stale_reasons(preserved, *missing)
            if refreshed:
                stale_knowledge_ids.append(claim.knowledge_id)
            if [stale_reason_key(r) for r in refreshed] != [
                stale_reason_key(r) for r in claim.stale_reasons
            ]:
                self._entries[claim.knowledge_id] = claim.model_copy(
                    update={"stale_reasons": refreshed},
                )
        return {
            "outcome": "sources reconciled",
            "details": {
                "checked": len(targets),
                "stale_knowledge_ids": stale_knowledge_ids,
                "stale_count": len(stale_knowledge_ids),
            },
        }

    @functools.wraps(BaseKnowledgeManager.clear, updated=())
    def clear(self) -> None:
        sched = maybe_tool_log_scheduled(
            "SimulatedKnowledgeManager.clear",
            "clear",
            {},
        )
        type(self).__init__(
            self,
            description=getattr(
                self,
                "_description",
                "nothing fixed, make up some imaginary scenario",
            ),
            log_events=getattr(self, "_log_events", False),
            rolling_summary_in_prompts=getattr(
                self,
                "_rolling_summary_in_prompts",
                True,
            ),
            simulation_guidance=getattr(self, "_simulation_guidance", None),
            hold_completion=getattr(self, "_hold_completion", False),
        )
        if sched:
            label, cid, t0 = sched
            maybe_tool_log_completed(label, cid, "clear", {"outcome": "reset"}, t0)


if TYPE_CHECKING:
    from ..common.tool_outcome import ToolOutcome  # noqa: F401
