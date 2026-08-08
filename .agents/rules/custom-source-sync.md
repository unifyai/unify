# Custom Source Sync: one engine, one identity contract

All git-tracked source definitions (tasks, functions, venvs, guidance,
knowledge, contacts, secrets, blacklist, data seeds, dashboards,
integration registry) reconcile through the shared engine in
`unify/common/custom_sync.py`. Full contract:
[`docs/writeups/custom-source-sync.md`](../../docs/writeups/custom-source-sync.md).

## Invariants

- A managed row has `custom_key`, `custom_hash` **and** `managed_by`
  (which source reconciles it: the deployment or a workflow slug); rows
  authored by users/actors have none of them. `custom_key` is the
  identity; auto-counted ids (`task_id`, `function_id`, …) are
  environment-local handles that source code must never reference.
- Every reconcile pass is scoped to ONE `managed_by`. `live_rows` and
  `find_collision` filter with `managed_rows_filter(managed_by)`, never
  a bare `custom_hash != None` — an unscoped query hands one source its
  siblings' rows and prune then deletes them. Rows with a null
  `managed_by` belong to the deployment and are stamped on their next
  content change.
- `managed_by` is reconcile provenance only — who may overwrite/prune
  the row. Which workflows *reference* a row is membership, a separate
  multi-valued field; never encode membership into `managed_by`.
- Each manager declares its key policy in ONE place (its `custom_*.py`
  collector). Changing a key policy is an identity migration for every
  deployment — plan it, never drive-by edit it.
- Inserts write `custom_key`/`custom_hash` atomically with the row.
  No create-then-stamp second write.
- Two live managed rows with the same `custom_key` is an error the
  engine raises (`CustomSyncDuplicateKeyError`) — never silently pick a
  survivor.
- Per-entry failures are isolated and re-raised as
  `CustomSyncPartialFailure` after the pass; the aggregate hash is not
  stored on partial failure so the next reconcile retries.
- Every reconcile holds `exclusive_sync_lease` on the meta context.
- Writers either persist the collected field dict wholesale, or consume
  every field and raise on leftovers. A collector hashing a field the
  writer drops caused live rows to pin themselves "up to date" with the
  field unwritten (the task `tags` incident) — the leftover check exists
  to make that class impossible.

## Hard refuse

- A new bespoke `sync_custom_*` diff loop, or "just this one" fork of
  the engine's semantics inside a manager. Extend the engine instead.
- Adding a field to a collector's hash without routing the same field
  through the writer (and vice versa).
- Stamping identity after insert, or hand-writing rows with a
  `custom_key` outside the engine.
- Changing a manager's key derivation (or making an optional source
  `key` mandatory) without a migration plan for live rows in every
  deployment.
- Registering a surface for workflow installs while its adapter still
  queries unscoped. The `SurfaceRegistry` refuses these
  (`UnscopedSurfaceError`); routing around it reintroduces mutual prune.

## Deviations are declared knobs, not forks

`prune=False` (secrets), `collision="yield"` (secrets),
`find_adoptable` (data seeds, integration registry, functions/venvs
legacy rows), `find_released` (tasks: a planted task the user has
edited), `should_update` (tasks: skip while running), `max_workers`
(tasks). New deviations need a named knob on the adapter and a line in
the writeup's table.

## Handing a row to the user

A surface may end the loan on one row: clear `managed_by`, keep
`custom_key`, and set `custom_released=True`. From then on no source
reconciles it, prune never reaches it, and `find_released` stops the
next pass planting a duplicate. Releasing is a **positive flag**, never
inferred from a null `managed_by` — rows written before `managed_by`
existed also have none, and those the deployment still owns.
