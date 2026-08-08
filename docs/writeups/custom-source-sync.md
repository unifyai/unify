# Custom Source Sync: the One Contract

Every state manager that supports git-tracked source definitions —
tasks, functions, venvs, guidance, knowledge, contacts, secrets,
blacklist, data seeds, dashboards, the integration registry — follows a
single reconcile contract, implemented once in
`unify/common/custom_sync.py` and adapted per manager. This document is
the contract. The repo rule
(`.agents/rules/custom-source-sync.md`) is the enforcement summary.

## Why one contract

The ten managers historically carried near copy-paste reconcile loops
that evolved independently. The divergence was drift, not design, and it
produced real production defects:

- **Hash/writer drift.** Each manager hand-maintained a hash collector
  *and* row writers as separate code paths. When `tags` was added to
  task sync, the hash learned the field before the writers did — live
  rows were stamped with the tagged hash while `tags` was written as
  `None`, then skipped forever as "up to date".
- **Non-atomic identity.** Task inserts created the row first and
  stamped `custom_key`/`custom_hash` in a second write. A crash between
  the writes leaves an orphan the next reconcile cannot match, so it
  plants a duplicate.
- **Silent duplicate keys.** Live rows were collapsed into a dict keyed
  by `custom_key`; two rows with the same key meant the loop silently
  updated one while the other rotted.
- **Inconsistent hardening.** Only guidance and functions took the
  `exclusive_sync_lease`; only functions isolated per-entry failures;
  only data and the integration registry could adopt pre-existing rows.
  Every one of those behaviors is desirable everywhere; each existed in
  exactly one or two places.

## The identity contract

1. **Three columns, one meaning, everywhere.** A managed row carries
   `custom_key` (stable identity of the authored source entry),
   `custom_hash` (content fingerprint of that entry), and `managed_by`
   (which source reconciles it). Invariant: `custom_key` is set **iff**
   `custom_hash` is set. Rows authored by users or actors carry none of
   them.
2. **`custom_key` is the identity; storage ids are handles.**
   Auto-counted ids (`task_id`, `function_id`, `guidance_id`, …) are
   environment-local surrogates allocated by Orchestra. Source code
   never chooses them and nothing in git may reference them.
3. **Keys are declared once per manager.** Each manager states its key
   policy in exactly one place — a pure function on the source entry in
   its `custom_*.py` collector:
   - *Explicit-in-source* (default): tasks (`key` field, mandatory),
     guidance, knowledge, blacklist.
   - *Derived-from-content* (allowed only where the content **is** the
     identity): functions and venvs (`name` — the call-site contract),
     data seeds (`context|seed_value`), dashboards (`tile_id` /
     `layout_id`), integration registry (`slug`), contacts
     (`first_name|surname` fallback), secrets (`name` fallback).
   Changing a key policy is an identity migration for every deployment
   and needs an explicit plan, not a drive-by edit.
4. **Rows are born with their identity.** Insert writes `custom_key`
   and `custom_hash` in the same write as the rest of the row. No
   create-then-stamp. (Managers whose insert path cannot carry the
   fields directly must stamp them before the row becomes visible to a
   concurrent reconcile — in practice: fix the insert path.)
5. **Duplicates fail loudly.** If two live managed rows share a
   `custom_key`, the engine raises `CustomSyncDuplicateKeyError` instead
   of silently picking a survivor. Until Orchestra grows null-exempt
   secondary unique keys, this check is the constraint.

## The reconcile contract

`unify.common.custom_sync.reconcile_custom_rows(source, adapter)`
implements the only diff loop:

```
live   = {row.custom_key: row for managed rows}     # dup keys raise
for key, fields in source:                          # per-key isolation
    fields = adapter.transform(key, fields)          # resolve names→ids etc.
    if key in live:
        if live hash != source hash and adapter.should_update(...):
            adapter.update(key, live_row, fields)
    elif released := adapter.find_released(key, fields):
        pass                                         # the user owns it now
    elif adoptable := adapter.find_adoptable(key, fields):
        adapter.adopt(key, adoptable, fields)        # stamp identity in place
    elif collision := adapter.find_collision(key, fields):
        yield or replace per adapter.collision
    else:
        adapter.insert(key, fields)
if adapter.prune:
    delete managed rows whose key left the source
if failures: raise CustomSyncPartialFailure(failures)
```

Uniform semantics, inherited by every manager:

- **Source supremacy.** A hash mismatch overwrites live edits to a
  managed row. Operator changes to managed rows are loans — unless the
  surface hands the row over (see `find_released` below), which ends the
  loan permanently.
- **Per-entry failure isolation.** One broken entry cannot abort the
  rest of the catalog. Failures are collected and re-raised as
  `CustomSyncPartialFailure` *after* the full pass; the aggregate hash
  is then **not** stored, so the next reconcile retries the failed
  entries and only them (the unchanged ones short-circuit on their row
  hash).
- **Aggregate-hash short-circuit.** `run_custom_sync(...)` wraps the
  loop with the stored-hash comparison so an unchanged bundle is a
  single meta read, not N row reads.
- **The sync lease.** Every reconcile runs under
  `exclusive_sync_lease(f"{meta_context}:custom_sync")` — not just
  guidance and functions.
- **Strict field consumption.** Writers either write the collected
  field dict wholesale, or (when transforming, e.g. tasks resolving
  `entrypoint_function` → `entrypoint`) must consume every field and
  raise on leftovers. A collector learning a field the writer ignores
  is a loud error, not silent drift.

Adapter knobs (each a deliberate, named policy — not a fork of the
loop):

| Knob | Default | Deviations |
|---|---|---|
| `prune` | `True` | secrets: `False` (never auto-delete credentials) | <!-- pragma: allowlist secret -->
| `collision` | `"replace"` (user-added row with the same key/natural id is deleted, source row inserted) | secrets: `"yield"` (a user-owned credential wins over the deploy value) | <!-- pragma: allowlist secret -->
| `find_adoptable` | none | data seeds (by seed value), integration registry (by slug), functions/venvs (by name — legacy rows without `custom_key`) |
| `find_released` | none | tasks: a planted task the user has edited. Clearing `managed_by` removes the row from `live_rows`, so without this probe the key reads as *missing* and the pass plants a second copy beside the edited one. Released rows are marked `custom_released=True` rather than inferred from a null `managed_by`, because a row written before `managed_by` existed also has none — and that one the deployment still owns |
| `should_update` | always | tasks: skip while an execution is running |
| `max_workers` | 1 | tasks: parallel updates, serialized inserts |

## Scoping: one source per pass

`managed_by` names the source whose reconcile owns the row — the
deployment (`MANAGED_BY_DEPLOYMENT`) or an installed workflow's slug.
Every pass is scoped to exactly one value of it:

- **`live_rows` and `find_collision` must filter with
  `managed_rows_filter(managed_by)`**, never a bare
  `custom_hash != None`. The loop prunes every managed row whose key
  left the source, so an unscoped query hands one source its siblings'
  rows and prune deletes them — the first workflow to sync a context
  would silently uninstall the deployment's content there, and vice
  versa.
- **The engine stamps `managed_by` after `transform`**, so collected
  content hashes stay identical regardless of who installs a bundle. A
  writer that persists its field dict wholesale gets the stamp for
  free; a transforming writer must consume it like any other field.
- **Rows predating the column are the deployment's.** A null
  `managed_by` is admitted only by the deployment's filter and stamped
  on the row's next content change; no migration, no second writer.
- **Aggregate hashes get one meta slot per source**
  (`stored_hash_field`): the deployment keeps the original unsuffixed
  field so existing installations do not re-sync on upgrade.
- **Duplicate keys are per-source.** Two rows of *different* sources
  may share a `custom_key` freely; they are distinct rows.

`managed_by` is provenance-for-reconcile only: who may overwrite and
prune the row. It says nothing about who *references* the row — that
is workflow membership, a separate multi-valued concern tracked on the
row, and deliberately not folded into this column.

## Functions: name-as-key, formalized

Functions and venvs keep `name` as their identity — a function's name
is its call-site contract (`entrypoint_function`, `depends_on`, actor
code), so renaming one is a breaking change regardless of sync
machinery. Under the contract this is a *declared derivation*
(`custom_key == name`), not a structural exception: rows carry
`custom_key` like everyone else, the engine matches on it, and legacy
rows written before the column existed are adopted by name and stamped
on their next sync.

## What this deliberately excludes

**FileManager's required-file overlay** (`sync_custom_files`) is not row
reconcile: it materializes files onto the Local drive and ingests them,
keyed by destination path with a content hash, with no identity columns
and no prune semantics. It keeps its own aggregate-hash short-circuit
but does not use the row engine — forcing it in would misrepresent what
it does.

## What this deliberately does not change

- **Public manager APIs.** `sync_custom(...)` signatures and the
  destination-grouping entry points stay; only the internals route
  through the engine.
- **Key values.** No manager's key policy changed in the unification.
  Live rows keep their identities; no churn.
- **Secrets' conservatism.** Never pruning and yielding to user-owned
  credentials are correct for credentials and are kept as declared
  policy.

## Adding a new source-synced surface

1. Write the collector (`custom_<thing>.py`): a pydantic source-entry
   model, a single documented key function, and a `collect_*` that emits
   `{key: {custom_key, custom_hash, ...fields}}` with the hash computed
   over the emitted fields.
2. Write an adapter subclassing `CustomSyncAdapter` with `live_rows`,
   `insert`, `update`, `delete` and any policy knobs.
3. Call `run_custom_sync(...)` from the manager's `sync_custom`, inside
   the destination loop and lease.

Do **not** write a bespoke diff loop. If the engine is missing a
capability, extend the engine.
