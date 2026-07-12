from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from ..manager_registry import SingletonABCMeta
from ..common.global_docstrings import CLEAR_METHOD_DOCSTRING
from ..common.state_managers import BaseStateManager
from ..common.tool_outcome import ToolOutcome
from .types.knowledge import Knowledge, KnowledgeKind
from .types.source_ref import SourceRef


class BaseKnowledgeManager(BaseStateManager, metaclass=SingletonABCMeta):
    """
    Public contract that every concrete knowledge-manager must satisfy.

    Stores durable domain claims in a single typed Knowledge ledger: facts,
    policies, definitions, decisions, constraints, insights, and preferences.
    Each claim carries provenance (``source_refs``), optional validity windows,
    confidence, topic tags, structured ``stale_reasons`` for broken source
    links, and lifecycle status (active / superseded / invalidated).

    Exposes CRUD and lifecycle operations (search, filter, get_knowledge,
    add_knowledge, update_knowledge, delete_knowledge, invalidate_knowledge,
    supersede_knowledge, reconcile_sources, clear) as first-class JSON tool
    calls on the CodeActActor.

    Prefer reading and writing claims here when the durable unit of knowledge
    is a typed statement with provenance — not a person attribute, procedure,
    executable function, raw file corpus, or credential.
    """

    _as_caller_description: str = (
        "the KnowledgeManager, managing typed domain claims on behalf of the end user"
    )

    # ------------------------------------------------------------------ #
    # Public interface                                                   #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def search(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> List["Knowledge"]:
        """Search for knowledge claims by semantic similarity to reference content.

        Knowledge claims are typed ledger entries (facts, policies, definitions,
        decisions, constraints, insights, preferences) with provenance and
        lifecycle status.

        By default only ``status == 'active'`` claims are returned. Pass an
        explicit filter via ``filter`` (on the filter method) or include
        ``status`` in a follow-up filter when you need superseded or invalidated
        claims. Long claims are returned with a truncated content
        preview; fetch the complete text with ``get_knowledge``.

        Parameters
        ----------
        references : Dict[str, str] | None, default None
            Mapping of source expressions to reference text for semantic
            search. Keys are column names or descriptive labels; values are
            the reference text to compare against.
        k : int, default 10
            Maximum number of results to return. Must be <= 1000.

        Returns
        -------
        List[Knowledge]
            Up to *k* rows ranked by similarity, backfilled to *k* when
            similarity yields fewer rows.

        Examples
        --------
        Good::

            KnowledgeManager_search(references={"content": "battery warranty"})

        Anti-pattern: do not use search to invent new facts — call
        ``add_knowledge`` after confirming the claim is not already stored.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List["Knowledge"]:
        """Filter knowledge claims using a Python filter expression.

        By default only ``status == 'active'`` claims are returned unless the
        caller filter already mentions ``status``. Long claims are returned
        with a truncated content preview; use ``get_knowledge`` for the full
        body.

        Parameters
        ----------
        filter : str | None, default None
            A Python boolean expression evaluated with column names in scope
            (e.g. ``"kind == 'policy' and 'warranty' in topics"``). When
            ``None``, returns active claims subject to pagination.
        offset : int, default 0
            Zero-based index of the first result to include.
        limit : int, default 100
            Maximum number of records to return. Must be <= 1000.

        Returns
        -------
        List[Knowledge]
            Matching knowledge records as Knowledge models.

        Examples
        --------
        Good::

            KnowledgeManager_filter(filter="kind == 'decision'")
            KnowledgeManager_filter(filter="status == 'superseded'")

        Anti-pattern: do not use filter to invent missing claims — if nothing
        matches, leave the result empty and add a claim only when the user
        (or a trusted source) actually provided one.
        """
        raise NotImplementedError

    @abstractmethod
    def get_knowledge(
        self,
        *,
        knowledge_id: int,
    ) -> "Knowledge":
        """Fetch one knowledge claim by id with its complete, untruncated content.

        ``search`` and ``filter`` return truncated content previews for long
        claims; call this before relying on the full text.

        Parameters
        ----------
        knowledge_id : int
            Identifier of the claim to fetch.

        Returns
        -------
        Knowledge
            The complete knowledge claim.
        """
        raise NotImplementedError

    @abstractmethod
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
        """Create a new typed knowledge claim in the ledger.

        Use for durable domain statements the assistant should remember:
        facts, policies, definitions, decisions, constraints, insights, or
        preferences. Always attach ``source_refs`` when provenance is known.

        Parameters
        ----------
        title : str
            Short human-readable title.
        content : str
            Full claim body.
        kind : KnowledgeKind | str, default fact
            Claim kind.
        topics : list[str] | None
            Optional topic tags.
        source_refs : list[SourceRef | dict] | None
            Provenance pointers (user_statement, transcript, file, data, web,
            actor_trajectory, derived_from_knowledge, manual).
        confidence : float | None
            Optional confidence in [0, 1].
        observed_at / valid_from / valid_until : datetime | None
            Optional observation and validity window.
        destination : str | None, default None
            Where this claim lives. Pass ``"personal"`` (the default) for
            private working knowledge. Pass ``"team:<id>"`` for team-shared
            claims. See the *Accessible shared teams* block in your system
            prompt for available teams.

        Returns
        -------
        ToolOutcome
            Outcome string and details containing the newly assigned
            ``knowledge_id``.

        Examples
        --------
        Good::

            KnowledgeManager_add_knowledge(
                title="Battery warranty",
                content="Tesla battery warranty is eight years.",
                kind="fact",
                topics=["warranty", "tesla"],
                source_refs=[{"kind": "user_statement", "note": "said in chat"}],
            )

        Anti-pattern: do not store person attributes, step-by-step procedures,
        executable code, raw file corpora, or credentials here. Do not invent
        provenance; omit ``source_refs`` rather than fabricating them.
        """
        raise NotImplementedError

    @abstractmethod
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
        """Update fields of an existing knowledge claim by id.

        Use for in-place corrections that do not replace the claim's identity.
        When a new claim should replace an old one (contradiction, revision
        with new provenance), prefer ``supersede_knowledge`` so lineage is
        preserved. Built-in claims (``is_builtin=True``) are read-only.

        Parameters
        ----------
        knowledge_id : int
            Identifier of the row to update.
        title / content / kind / topics / source_refs / confidence /
        observed_at / valid_from / valid_until
            Fields to replace; omit to keep existing values.
        destination : str | None, default None
            Destination scope for the write (personal or ``team:<id>``).

        Returns
        -------
        ToolOutcome
            Outcome string and details with the ``knowledge_id``.

        Anti-pattern: do not use update to soft-delete — call
        ``invalidate_knowledge`` or ``delete_knowledge``. Do not use update
        to mark a claim as replaced — call ``supersede_knowledge``.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_knowledge(
        self,
        *,
        knowledge_id: int,
        destination: str | None = None,
    ) -> "ToolOutcome":
        """Hard-delete a knowledge claim by id.

        Prefer ``invalidate_knowledge`` when you want to retain an audit trail.
        Built-in claims (``is_builtin=True``) cannot be deleted.

        Parameters
        ----------
        knowledge_id : int
            Identifier of the row to delete.
        destination : str | None, default None
            Destination scope for the write.

        Returns
        -------
        ToolOutcome
            Outcome string and details with the removed ``knowledge_id``.
        """
        raise NotImplementedError

    @abstractmethod
    def invalidate_knowledge(
        self,
        *,
        knowledge_id: int,
        destination: str | None = None,
    ) -> "ToolOutcome":
        """Mark a knowledge claim as invalidated without deleting it.

        Use when a claim is known to be wrong or withdrawn but should remain
        in the ledger for audit. Invalidated claims are excluded from default
        search/filter results.

        Parameters
        ----------
        knowledge_id : int
            Identifier of the claim to invalidate.
        destination : str | None, default None
            Destination scope for the write.

        Returns
        -------
        ToolOutcome
            Outcome string and details with the ``knowledge_id``.
        """
        raise NotImplementedError

    @abstractmethod
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
        """Replace an existing claim with a newer one, preserving lineage.

        Creates a new claim (or links an existing ``new_knowledge_id``), sets
        the old claim's ``status`` to ``superseded`` and ``superseded_by_id``
        to the replacement, and records the old id in the new claim's
        ``supersedes_ids``.

        Parameters
        ----------
        old_knowledge_id : int
            Claim being replaced.
        title / content / ...
            Fields for the replacement claim when creating a new row. Required
            (at least title and content) unless ``new_knowledge_id`` is given.
        new_knowledge_id : int | None
            Optional existing claim that should become the replacement instead
            of creating a new row.
        destination : str | None, default None
            Destination scope for the write.

        Returns
        -------
        ToolOutcome
            Outcome with ``old_knowledge_id`` and ``new_knowledge_id``.

        Anti-pattern: do not manually edit ``status`` / ``superseded_by_id``
        via ``update_knowledge`` — use this method so both sides stay wired.
        """
        raise NotImplementedError

    @abstractmethod
    def reconcile_sources(
        self,
        *,
        knowledge_ids: Optional[List[int]] = None,
        destination: str | None = None,
    ) -> "ToolOutcome":
        """Best-effort check that claim provenance still resolves.

        Scans active claims (or the given ``knowledge_ids``) and verifies
        identity-bearing file, contact, data-context, and derived-knowledge
        source refs. Each claim's structured ``stale_reasons`` is refreshed
        without changing its lifecycle status. Declared source refs remain
        attached even when their targets no longer resolve.

        Parameters
        ----------
        knowledge_ids : list[int] | None
            Optional subset to check; when omitted, scans active claims.
        destination : str | None, default None
            Destination scope for reads and stale-reason writes.

        Returns
        -------
        ToolOutcome
            Outcome with ``checked``, ``stale_knowledge_ids``, and
            ``stale_count`` details.
        """
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> None:
        raise NotImplementedError


# Attach centralised docstring
BaseKnowledgeManager.clear.__doc__ = CLEAR_METHOD_DOCSTRING
