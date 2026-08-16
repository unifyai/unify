# Workflows

A **workflow** is a hand-curated, versioned package that sets an assistant up
for a recurring job in one install. `daily_briefing` is two guidance
procedures, one knowledge claim, one shared function and a weekday-08:30
recurring task, with a Gmail connection as its requirement.

The word is load-bearing and was expensive to reclaim: see
[`.agents/rules/product-vocabulary.md`](../../.agents/rules/product-vocabulary.md).
A workflow is the noun you install. What it plants are **procedures**,
**claims**, **functions** and **tasks**.

## The one mechanism

Installing fans out each surface's existing `sync_custom` — the shared engine in
[`unify/common/custom_sync.py`](../../unify/common/custom_sync.py), whose contract
is [`custom-source-sync.md`](custom-source-sync.md). There is **no second content
store, no second reconcile loop and no second lookup path**: a procedure a
workflow planted is found by the same search as any other. What installation adds
is provenance, which is the only reason update and uninstall are possible at all.

Uninstall is the same fan-out with empty sources, so the engine's prune removes
exactly that slug's rows. This is why the per-destination `sync_custom_*` methods
are registered and never the destination-grouping wrappers — those derive
destinations from the entries, so an empty source reaches nothing and an
uninstall silently prunes nothing while reporting success. That defect shipped
once; a signature test now pins it.

## Identity: two fields, not one

| Field | Meaning | Where |
|---|---|---|
| `managed_by` | which source reconciles this row — may be cleared when the user takes ownership | every managed row |
| `workflows` | which installs reference this row | shared-identity surfaces only |

Provenance is not membership. Splitting them is what makes N:M possible and what
makes uninstall safe.

Per-slug surfaces (guidance, knowledge, tasks) are stamped `managed_by == slug`
and carry no list. Shared-identity surfaces (functions, venvs — keyed by name,
because a name is the call-site contract) sync as the **union of every installed
bundle** under one `workflow_library` source, with membership on the row. Per-slug
sources on a name-keyed surface would make one name two rows, and the
legacy-adoption probe would steal the row back and forth between them.
Same-name-different-content across two bundles refuses the install loudly, before
anything plants.

## The workflow/task boundary

This is the invariant most at risk of erosion, so it is written out.

| | TaskScheduler | WorkflowManager |
|---|---|---|
| Grammatical role | verb — *do this* | noun — *this assistant is set up to do X* |
| Owns | durable work: schedule, trigger, executions, retries | install state and settings |
| Runtime | `Tasks/Executions`, a steerable handle | **none** |
| Status | stored, per execution | **derived** — installed + constituent task state |
| "Run now" | `POST /v0/tasks/{task_id}/trigger` | does not exist |

Tasks are independent by design — there is no queue chaining or ordering between
them — so a workflow-level run would need orchestration the system deliberately
lacks. "Run the workflow now" resolves to triggering its task.

**Erosion tests.** Any of these means the boundary has broken:

- a `Workflows/Executions` context, or any per-run row under `Workflows`
- `install_workflow` returning anything steerable that is not a `TaskScheduler`
  handle
- a `status` value on the installation row that describes *work* rather than
  *setup*
- Console rendering run history on the Workflows surface

`Workflows/Requests` is **not** a violation: a request records one intent to
change install state, with no schedule, no retry policy, no occurrence rows and
nothing steerable. Its status describes a reconcile pass, not a job.

## Requirements gate arming, never planting

An install with an unmet requirement still plants everything and returns the
`connect_required` envelope; the planted tasks — born disarmed, which is the
engine's default — stay held until the connection lands. A repeat install is the
arm-on-connect path.

Refusing the install would hide what the user is about to get; installing armed
would fire a job against nothing.

Whether an app is connected is never the bundle's business. A requirement names a
**provider app slug** — the id space Console's gallery, `app_slug` and native
manifests share — and `RequirementResolver` consults each authority in turn: a
live connection row, then the app's own native package manifest, then a
bundle-declared secret for BYOD OAuth. It reports `via` so a UI can say whether
the fix is a connect flow or a pasted key. **Never gate on the secret keyset
alone** — that silently held every gallery-connected app.

`needs_connection` is derived at read time, never stored: connections change
without the installation row being touched. `partial` outranks it, because
something genuinely failed to plant.

## Surface verdicts

| Surface | In a bundle? | How |
|---|---|---|
| guidance, knowledge | yes | content, per-slug |
| functions, venvs | yes | content; `custom_key == name`, so shared atoms converge |
| tasks | yes | content; planted disarmed |
| data | schema only | declare the table, never seed rows |
| canvas | yes | pre-built view + bindings + actions |
| integrations, secrets | requirement | declared and checked; never carried |
| contacts, transcripts, blacklist | **no** | runtime-populated by the workflow's own functions |

Contacts are observed entities — a curated bundle cannot know them, so a bundle
ships the *function that populates* contacts, never contact rows.

## Canvas is gated in CI, not at install

The authoring gates (lint → typecheck → bundle → headless render → critique) are
expensive and LLM-involving, so per-install builds would be slow and
non-deterministic. We curate, so we gate at curation time and ship the built
bundle on the row.

Prefer query bindings over refresh tasks: a canvas bound to a stored table is
live server-side per view. The scheduled task keeps the **table** fresh; it does
not refresh the view.

## Where things live

The catalogue listing and each artifact's published copy are **platform data** in
the public-read `Builtins` project (`Workflows/Catalog`, `Workflows/Content`),
admin-seeded and hash-guarded, exactly like the integrations app catalogue. A
hosted assistant is an on-demand job that is usually asleep when someone opens
Console, so a read must never wake one.

Everything per-assistant stays in the assistant's own contexts: installations,
params, planted rows, and requested changes.

Authoring is **git-only**. Bundles live in unify-deploy; unify points at them via
`UNIFY_WORKFLOWS_DIR` and resolves the installed `unify_deploy` package as a
fallback — a guarded, optional import, the same pattern
`integration_status/discovery.py` uses, so a checkout without the private sibling
simply has no shelf.

## Mutations from a reading surface

Console cannot install a workflow: planting needs the reconcile engine, which is
the assistant's. So a click records a durable `Workflows/Requests` row and the
assistant carries it out — on the wake Orchestra dispatches, or on its next boot.

Persist, then dispatch. Dispatching first could wake an assistant for work no row
records. The wake is an optimisation for latency; the row is the mechanism, and a
boot sweep drains the same queue. Each request is claimed with a server-side
compare-and-set, so at-least-once delivery and the boot sweep cannot both apply
one change, and a claim past the stale window is recoverable with the dead holder
fenced out by its own claim key.
