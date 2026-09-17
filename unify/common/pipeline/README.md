## `unify/common/pipeline/` — shared pipeline infrastructure

This package owns the reusable infrastructure for ingestion pipelines: typed transport models, artifact storage, observability ledgers, resilience policies, work queues, and run orchestration.

It is a cross-cutting shared module with zero imports from `FileManager`, `DataManager`, or any state manager. Both the FM executor and the DM ingestion path consume these protocols and types identically.

## Design principles

- **Ports and Adapters**: every infrastructure concern (artifact storage, work queues, run ledgers) is expressed as a `Protocol` with a filesystem or in-memory implementation. Each is a *full* implementation, not a stub — `LocalArtifactStore` fences and checkpoints — because an executor cannot know which adapter it was handed and must not offer a guarantee the binding cannot keep.
- **Control plane is JSON**: manifests, queue messages, ledger records, and progress events are all JSON-serializable Pydantic models. Large tabular data is never serialized through the control plane.
- **No pickle**: binary serialization on durable or cross-process boundaries is explicitly forbidden. Row data stays in its original source file or is materialized as JSONL/Parquet artifacts.
- **Typed settings**: all configuration flows through Pydantic `BaseSettings` models defined in `config.py`. No ad-hoc `os.getenv()` calls.

## Module inventory

### `types.py` — typed transport models

Defines the sealed `TableInputHandle` union for deferred row loading:

```
TableInputHandle (discriminated union)
  ├── InlineRowsHandle      — small tables with rows materialized in memory
  ├── CsvFileHandle          — pointer to a CSV file + dialect metadata
  ├── XlsxSheetHandle        — pointer to an XLSX sheet + header metadata
  └── ObjectStoreArtifactHandle — pointer to a materialized JSONL/Parquet artifact
```

Also defines `ParsedFileBundle`, the pipeline-owned wrapper that pairs a semantic `FileParseResult` with a dict of `TableInputHandle`s keyed by table ID.

The key invariant: `ExtractedTable` stays a pure semantic DTO. Transport and persistence concerns live exclusively in `TableInputHandle` and `ParsedFileBundle`.

### `artifact_store.py` — durable artifact materialization

`ArtifactStore` protocol and `LocalArtifactStore` (filesystem-backed). Given any `TableInputHandle`, the store materializes rows into a durable artifact and returns an `ObjectStoreArtifactHandle`.

The local implementation writes JSONL files under a content-addressed path and returns `file://` URIs.

### `run_ledger.py` — run lifecycle manifests

Typed models for tracking pipeline run lifecycle:

- `PipelineRunManifest`: top-level run record with `run_id`, status, timing
- `PipelineFileManifest`: per-file record within a run
- `PipelineStageManifest`: per-stage timing and error tracking

`RunLedger` protocol with `JsonlRunLedger` implementation that appends manifests as JSONL records for post-run inspection.

### `checkpointed_ingest.py` — resumable table ingestion

The one place rows are written. Every ingestion path calls `CheckpointedIngest`,
so the guarantees below hold whichever caller started the run.

Three invariants, each answering a specific way an interrupted ingestion loses or
duplicates rows:

- **Checkpoint after every committed chunk** — resume re-does at most one chunk.
- **A fenced lease per table** — at-most-one-writer, and a superseded writer
  *notices* rather than rolling progress backwards over its successor's.
- **Verify committed rows against the declared count** — a run that landed fewer
  rows than its source declared fails loudly instead of passing as success.

`TableWork` describes one table to ingest; `IngestOutcome` reports what happened,
with `shortfalls` naming any table whose durable progress is behind its declared
count. `incomplete_tables` answers the same question from checkpoints alone, so an
audit and the run's own finalisation gate cannot disagree.

### `retry_policy.py` — network resilience

`ResilientRequestPolicy` encapsulates retry logic for network boundaries:

- configurable max retries, backoff multiplier, jitter ratio, deadline budget
- typed `FailureKind` classification (retryable, permanent, rate-limited, timeout)
- `RetryDecision` with computed delay

Applied consistently to artifact store operations and queue interactions.

### `work_queue.py` — typed work queue

`WorkQueue` protocol for queue-backed parse/ingest orchestration:

- `publish(topic, payload)` → message ID
- `receive(max_messages, topics)` → leased `ReceivedWorkItem`s
- `ack(receipt_id)` / `retry(receipt_id, error, delay)` / `dead_letter(receipt_id, error)`

`InMemoryWorkQueue` is the local implementation backed by `asyncio.Queue`, following the same async patterns used by `EventBus` and `ConversationManager`. `LocalQueueWorker` drains items with automatic ack/retry/dead-letter routing based on handler exceptions.

Dead-lettered items are retained in memory for operator inspection via the `dead_letters` property.

### `_utils.py` — shared internal helpers

Private module with `utc_now()`, `utc_now_iso()`, and `JsonlWriter` used across the ledger modules. Not part of the public API.

## End-to-end data flow

```
FileParser.parse() → FileParseResult (semantic, public)
       ↓
ParsedFileBundle (pipeline-owned: result + TableInputHandles)
       ↓
FileManager executor (ingest via DataManager, emit ledgers)
       ↓
ArtifactStore (optional materialization for large tables)
       ↓
RunLedger (JSONL persistence for inspection)
```
