"""The IngestionManager contract.

Storing data is one verb with two questions: where does it come from, and where
should it land. Those vary independently, so they are two arguments rather than one
config, and every combination of them means something:

===============  ====================  ==========================================
source           target                result
===============  ====================  ==========================================
``RowsSource``   ``TableTarget``        rows become one queryable table
``TableSource``  ``TableTarget``        stored rows reshaped into another table
``FilesSource``  ``CollectionTarget``  documents kept whole, tables inside them
                                        extracted alongside
``FilesSource``  ``TableTarget``        spreadsheets or CSVs merged into one table
``FolderSource`` either                 the same, over an open-ended set of files
===============  ====================  ==========================================

Choosing a target has one rule: **a table is for querying columns, a collection is
for keeping documents whole.** If the intent is to filter, aggregate or chart it,
that is a table. If the intent is to read, search or cite it, that is a collection.

## Nothing blocks

``submit`` records the request and returns a handle immediately. Work happens in the
background whichever tier runs it, so a plan is never held open by a large
ingestion. Poll ``get_status`` when there is other work to do, or call ``wait`` when
there genuinely is not.

## Two tiers, one interface

Small work runs in process; large or open-ended work is dispatched to run remotely.
The tier is chosen from measurable shape — how many rows, how many files, how many
bytes, whether the source is a folder — and never from how a request was phrased.
``mode`` overrides that when the caller knows better.

The distinction is deliberately invisible to everything downstream: a run has the
same id, the same statuses and the same recovery verbs either way. Code that
submits work does not need to know where it ran in order to find out how it ended.

## Runs are recoverable, not fire-and-forget

Every run is recorded before any work starts, so a failure is always something with
an id that can be inspected and retried rather than an error that scrolled past.
Items that exhaust their retries are parked rather than dropped, and
``retry(only="dlq")`` re-attempts exactly those, leaving successful work untouched.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from unify.ingestion_manager.types.request import (
    EmbedSpec,
    IngestionMode,
    IngestionSource,
    IngestionTarget,
)
from unify.ingestion_manager.types.run import (
    IngestionRun,
    IngestionSummary,
    LogEntry,
    RetryResult,
    RetryScope,
    RunState,
    RunStatus,
)

# Imported for the signature of `post_ingest`, which is the same shape a direct
# table write accepts, so derived columns are requested identically either way.
from unify.data_manager.types.ingest import PostIngestConfig


class BaseIngestionManager(ABC):
    """Store data and files from any source, observably and recoverably."""

    # ── submitting ────────────────────────────────────────────────────────

    @abstractmethod
    def submit(
        self,
        source: IngestionSource,
        target: IngestionTarget,
        *,
        embed: Optional[EmbedSpec] = None,
        post_ingest: Optional[PostIngestConfig] = None,
        destination: Optional[str] = None,
        mode: IngestionMode = "auto",
    ) -> IngestionRun:
        """Start storing data, and return a handle without waiting for it.

        Returns as soon as the run is recorded. The returned ``run_id`` is how
        everything afterwards refers to this work, and the run's ``contexts``
        report the exact paths it wrote — which is what lets a view be built over
        the result without anyone predicting the storage layout in advance.

        Parameters
        ----------
        source : RowsSource | FilesSource | FolderSource | TableSource
            Where the data comes from. ``RowsSource`` for anything already in hand,
            including a response just fetched from an external service.
            ``FilesSource`` for specific files, ``FolderSource`` for an open-ended
            set of them, ``TableSource`` to reshape what is already stored.
        target : TableTarget | CollectionTarget
            Where it lands. ``TableTarget`` names one queryable context and is the
            right choice when the data will be filtered, aggregated or charted.
            ``CollectionTarget`` keeps documents whole and extracts any tables
            found inside them; leave its ``name`` unset to give each file its own
            namespace, or set it to group a related set under a stable name that
            can be added to later.
        embed : EmbedSpec | None
            Text columns to make semantically searchable. Requested the same way
            for rows and for file content.
        post_ingest : PostIngestConfig | None
            Derived columns computed once the data is stored.
        destination : str | None
            Ownership root: ``"personal"`` (the default) or ``"team:<id>"``. The
            privacy floor is personal — when it is unclear whether something
            belongs to a team, ask rather than widening the audience.
        mode : "auto" | "inline" | "dispatched"
            Leave as ``"auto"`` unless there is a reason not to. ``"inline"``
            forces in-process execution, which is worth doing when a later step in
            the same plan needs the data immediately and it is known to be small.
            ``"dispatched"`` forces remote execution for work that would otherwise
            be judged small but is known to be slow.

        Returns
        -------
        IngestionRun
            Handle carrying ``run_id`` and the current state. ``contexts`` fills in
            as the run progresses.

        Raises
        ------
        ValueError
            If the source and target cannot mean anything together — rows have no
            documents to keep whole, so they cannot go to a collection. The message
            names the alternative.
        """

    # ── observing ─────────────────────────────────────────────────────────

    @abstractmethod
    def get_status(self, run_id: str) -> RunStatus:
        """Report where a run has got to, and what to do about it.

        Carries per-stage progress, how many rows and files have been handled, how
        many items are parked, and any error. ``next_step`` states the single action
        that makes sense now in plain words — including that nothing is needed when
        the run simply finished — so recovery does not depend on reconstructing the
        rule from the other fields.

        Safe to poll. Check ``is_terminal`` to know when polling can stop.
        """

    @abstractmethod
    def get_logs(
        self,
        run_id: str,
        *,
        stage: Optional[str] = None,
        limit: int = 200,
    ) -> List[LogEntry]:
        """Read what a run recorded, newest last.

        For the question ``get_status`` cannot answer: *why* a stage failed. Narrow
        to one stage when a status has already identified where the failure was.
        """

    @abstractmethod
    def wait(
        self,
        run_id: str,
        *,
        timeout_s: Optional[float] = None,
    ) -> RunStatus:
        """Block until a run finishes, or until the timeout passes.

        Use only when the plan genuinely cannot proceed without the data — for
        example when the next step builds a view over it. Otherwise prefer
        ``get_status``, which leaves the plan free to do other work.

        Returns the final status on completion, or the current one if the timeout
        passes first. A timeout is not a failure: the run continues, and the same
        ``run_id`` still tracks it.
        """

    @abstractmethod
    def list_runs(
        self,
        *,
        state: Optional[RunState] = None,
        context: Optional[str] = None,
        limit: int = 50,
    ) -> List[IngestionSummary]:
        """List recent runs, newest first.

        Filter by ``state`` to find what needs attention, or by ``context`` to see
        what has been written to a particular place — which is how to tell whether
        a table is stale, and what last wrote to it.
        """

    # ── recovering ────────────────────────────────────────────────────────

    @abstractmethod
    def retry(self, run_id: str, *, only: RetryScope = "dlq") -> RetryResult:
        """Re-attempt part of a run.

        ``only="dlq"`` re-attempts just the items that exhausted their retries and
        were parked. This is the default because it is the safe one: work that
        already succeeded is not touched, so retrying twice cannot duplicate data.

        ``only="stale"`` re-attempts items claimed by a worker that then stopped
        reporting, which recovers a crashed worker's share without disturbing
        healthy work.

        ``only="all"`` re-attempts everything, including items that succeeded. Only
        appropriate when the target was emptied or the way the data is parsed
        changed, since it will re-write rows that were already correct.

        A result of zero requeued items is an answer, not a failure: there was
        nothing in that scope to retry.
        """

    @abstractmethod
    def cancel(self, run_id: str) -> bool:
        """Stop a run and abandon its remaining work.

        Work already completed stays; nothing is rolled back. A cancelled run
        cannot be resumed — use ``pause`` to stop something that should continue
        later. Returns whether the run was in a state that could be cancelled.
        """

    @abstractmethod
    def pause(self, run_id: str) -> bool:
        """Stop a run but keep its remaining work.

        For relieving pressure — a rate limit being approached, a noisy neighbour —
        without losing progress. Resume when the reason has passed.
        """

    @abstractmethod
    def resume(self, run_id: str) -> bool:
        """Continue a paused run from where it stopped.

        Only a paused run can be resumed. To re-attempt a failed one, use
        ``retry``.
        """
