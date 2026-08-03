## Task Scheduler – Architecture and Guide

This package manages the creation, scheduling, execution, and lifecycle of tasks. It provides:

- A public manager (`TaskScheduler`) that exposes `ask` / `update` / `execute` methods
- A simulated manager for demos and tests
- Strong types for tasks, schedules, and statuses
- An execution layer that delegates a single task run to the actor substrate
- A storage layer that centralizes Unify I/O


### High-level picture

- `TaskScheduler` is the orchestrator. It composes read/write "tools" and runs LLM loops for `ask` and `update`. The `execute` path does not use an async tool loop; it returns a `SteerableToolHandle` directly.
- Tasks are independent: there is no queue chaining or ordering between tasks. Each task holds only its own schedule and trigger.
- All reads/writes go through `TasksStore` (Unify I/O).

**A definition is authored intent; a run is a row.** The `Tasks` row holds what
a person asked for — name, description, schedule, repeat rule, entrypoint — and
one switch, `enabled`. It carries no run state at all: no next-run timestamp, no
"currently firing" flag, no lease. Everything about a particular firing lives in
`Tasks/Executions`, and the open row there *is* the thing that will fire.

That split is the reason most of this subsystem looks the way it does. When the
next-run time lives on the definition, advancing a series means mutating the
row every tick, and two dispatchers racing to mutate it need a lock or a
time-based lease to stay correct. With occurrences as their own rows, the
question "what runs next" is a query rather than a field, and safety comes from
the key instead of a lease:

- **`run_key` is derived, not allocated.** Orchestra's
  `_build_open_execution_run_key` and unify's `build_task_run_key` compose it
  from `delivery`, `wake`, `assistant_id`, `destination`, `task_id`, a digest of
  the authored revision, and a tail. Two components that agree on the facts
  produce the same string, so create-or-adopt converges on one row without
  coordinating. A projected head can only carry what the row itself knows — the
  due time on a scheduled wake, the medium on a triggered one — so the contact
  and message that fire a live trigger are absent until an event actually
  arrives, and a firing carrying them is a distinct occurrence with its own key.
- **The two implementations must stay byte-identical.** `team:11` and
  `2026-07-29T16:50:00+00:00` once normalized differently on each side, so the
  dispatcher failed to adopt the projected row and minted a second execution for
  the same occurrence. Two rows read as concurrency, the overlap guard then
  skipped every tick, and the series stopped without an error. Both normalizers
  exist because of that.
- **`revision` is a digest of authored fields only**, so adding or removing a
  projected column is a non-event, while a real edit (a new schedule, a
  different entrypoint) retires the open head and mints a fresh occurrence.


### Key files and responsibilities

- `task_scheduler.py`
  - The core manager. Exposes public `ask`, `update`, `execute` and a comprehensive set of private tools (create/delete/cancel tasks; list/get tasks; bulk schedule edits).
  - Uses `TasksStore` for I/O. Exposes `ContactManager.ask` among its tools for cross-domain flows.

- `active_task.py`
  - `ActiveTask`: a per-task, steerable handle that wraps the actor's live plan. Mirrors status into the Tasks row on stop/result. Classifies interjections (cancel) using the scheduler.

- `storage.py`
  - `TasksStore`: centralized Unify I/O (reads/writes, normalization).

- `prompt_builders.py`
  - Builds dynamic system prompts for LLM loops (ask/update) from the tools the scheduler actually exposes, with examples and safety guidance.

- `simulated.py`
  - `SimulatedTaskScheduler`: drop-in replacement for demos/tests. Mirrors the real surface but does not touch storage.

- `types/` (Pydantic models & enums)
  - `task.py`: canonical `Task` model (ids, name/description, status, schedule, trigger, deadline, repeat, priority, response_policy, activated_by). Enforces schedule/trigger exclusivity.
  - `schedule.py`: `Schedule` model.
  - `status.py`: lifecycle enum (`scheduled`, `triggerable`, `active`, `completed`, `cancelled`, `failed`).
  - `activated_by.py`: activation reasons (`schedule`, `trigger`, `explicit`).
  - `repetition.py`: recurrence patterns. `priority.py`: priority enum. `trigger.py`: inbound trigger definition.


### Core flows

1) Ask (read-only)
   - Builds a live toolset (filters, semantic search, contact lookup), injects a dynamic system prompt, and runs a tool-use loop. Must not mutate data.

2) Update (mutations)
   - Exposes creation, deletion, cancellation, and schedule manipulation tools. Enforces schedule/trigger invariants via a single validated write funnel.

3) Execute (run now)
   - Validates the task is runnable, records activation provenance, and delegates execution to the actor substrate. Returns a `SteerableToolHandle` for the caller to await or interject.

4) Scheduled activation
   - User-authored scheduled task rows are projected by Orchestra into machine-facing execution rows.
   - Recurrence is owned by Orchestra, not by whoever dispatches. Marking a run
     `running` projects that series' next occurrence, so a repeating task always
     has its successor on the ledger before the current run does any work. The
     runtime computes no slots; there is no handoff for a dispatcher to drop.
   - The task supervisor sweep is the floor under that: it re-projects a head for
     any enabled, armed definition that has none. A series can only reach that
     state if the run-start transition never happened at all.
   - Communication materializes scheduled live executions as Cloud Tasks targeting the adapters `/scheduled/tasks/due` endpoint.
   - The live wake reason is delivered to ConversationManager, which asks the slow brain to start with `primitives.tasks.execute(task_id=...)`.
   - Cloud Scheduler is used for platform maintenance jobs; per-task cadence is delivered by dynamic Cloud Tasks.

5) Trigger activation
   - Trigger definitions are projected into execution rows and mechanically matched by medium/contact filters when inbound communication events arrive.
   - Live trigger candidates are surfaced to the slow brain, which performs semantic acceptance and calls `primitives.tasks.execute(task_id=..., trigger_attempt_token=...)` so the run adopts the exact inbound provenance.
   - `triggerable` is an execution state, not a definition state. A recurring trigger's next arm is projected onto the ledger the same way a schedule's successor is; the definition carries only authored intent and its `enabled` switch.

6) Offline activation
   - Offline means the hidden headless lane: the live ConversationManager and main actor are not woken.
   - Offline scheduled activations use Cloud Tasks targeting Communication's offline-dispatch endpoint, which creates a short-lived Unity Kubernetes job.
   - The job runs `offline_runner.py`, which starts the same actor substrate headlessly and delegates through `TaskScheduler.execute(...)`.
   - Offline delivery is independent from execution style. Agentic offline tasks keep `entrypoint=None`; symbolic offline tasks use a stored FunctionManager entrypoint.

7) Resource opt-ins
   - `requires_filesystem` and `requires_computer` are authored independently of `offline` and `entrypoint`.
   - When either is true, dispatch waits for a ready assistant desktop (Local sync and/or computer-use) before the run starts.

8) Concurrency
   - Multiple executions of the same `task_id` may be `active` at once (for example a 90-minute job on a 60-minute schedule).
   - `execute` only refuses to start when activation provenance targets the exact `source_task_log_id` that is already `active`.
   - Symbolic entrypoints receive opt-in kwargs (`task_id`, `run_key`, `task_execution_context`) so they can gate or skip themselves when desired.


### Schedule invariants (enforced centrally)

- Tasks with `schedule.start_at` must be `scheduled`.
- Trigger-based tasks (`trigger`) cannot also carry a schedule.
- Writes go through `_validated_write(...)`, which checks invariants, prevents direct `active` writes, and enforces schedule/trigger exclusivity.


### Considered and declined: polling instead of Cloud Tasks

Once Orchestra owned recurrence, replacing the Cloud Tasks timer with a
poller over due execution rows became possible: the ledger already knows
every open occurrence and its slot, so a loop selecting `state == scheduled
AND scheduled_for <= now` would need no per-task timer at all.

It is not worth doing. Cloud Tasks delivers per-task cadence at second
granularity with no idle cost and no work of ours to keep alive; a poller
trades that for a fixed interval that bounds every task's latency, a new
always-on component to run and monitor, and lease/visibility handling to
stop two pollers claiming one occurrence. That is a materially larger
surface than the timer it replaces, in exchange for removing a dependency
that has not been a source of incidents. The dropped-baton failures came
from the recurrence handoff, which is now gone; they were never the timer's
fault.

Revisit only if Cloud Tasks itself starts costing us — quota ceilings,
regional constraints, or materialization failures that the supervisor sweep
has to keep covering for.


### Storage

- All I/O runs through `TasksStore`.
- Environment toggle `UNITY_TASK_LOCAL_VIEW_OFF` is a no-op in the current design (kept for compatibility with env configs).


### Execution handle

- Durable work uses **two** Orchestra surfaces only:
  - `Tasks` — definition (series), keyed by `task_id`
  - `Tasks/Executions` — one wake/attempt, unique `run_key` (idempotency key)
- There is **no** `Tasks/Activations` and no separate Runs ledger. Occurrence and
  attempt are the same Execution row (`state`: scheduled → triggerable →
  running → completed/failed/cancelled).
- Vocabulary: `wake` ∈ {scheduled, triggered, explicit, provider_event};
  `delivery` ∈ {live, offline}. Recurrence creates the *next* Execution when
  the current one **starts**.
- EventBus stamps `task_id` + `run_key` (no `instance_id`). Diagnose a run with
  **depth-1** primitives (never dump the full forest in one call):

```python
kids = await primitives.tasks.get_run_event_children(run_key=rk)
failed = [c for c in kids["children"] if c.get("error")]
if failed:
    detail = await primitives.tasks.get_run_event(
        run_key=rk,
        node_id=failed[0]["node_id"],
    )
# Prefer a short last expression over printing large payloads.
f"children={len(kids['children'])} failed={len(failed)}"
```

  Executions may live under `Teams/{id}/Tasks/Executions` while Events stay under the
  executing assistant’s `…/Events/*` — join is by `run_key` value. Low-level helper:
  `unify.task_scheduler.task_run_events.fetch_task_run_events` / `project_immediate_children`.


### Entrypoints and description-driven execution

- `entrypoint` is optional for all tasks. When it is null, execution is actor-driven: a contained child actor run interprets the task name, description, schedule/trigger metadata, repeat pattern, and response policy.
- `offline` controls delivery only. The headless lane still runs through the actor substrate; `entrypoint` controls whether that actor run is symbolic.
- Direct `TaskScheduler.execute(...)` needs either a run-scoped actor delegate or an explicitly configured actor. A production live wake normally reaches execution through `Actor.act` and `primitives.tasks.execute(...)`; tests can still inject a simulated actor explicitly.
- After a successful recurring or triggerable description-driven run, the actor always runs a storage review that considers whether the observed trajectory is stable enough to store as a function. The write is conditional: if future runs still need broad planning or tool discovery, the task remains description-driven.


### Clarification and contacts

- The ask/update loops can expose a `request_clarification` tool (when queues are provided) to ask the human questions without mixing clarifications into normal replies.
- The scheduler also exposes `ContactManager.ask` for cross-domain context when tasks mention people/contact ids.


### Safety

- After any mutation (including execution start), the caller must refresh task state before further edits.


### Common environment variables

- `UNITY_TASK_SIM_ACTOR_DURATION`: default simulated actor duration (seconds).


### Quick orientation for new contributors

- Start in `task_scheduler.py` to see public surface and tool wiring.
- Read `storage.py` to understand I/O behaviors.
- `active_task.py` defines the live execution handle returned by `execute`.
- `machine_state.py` handles execution validation and stale delivery rejection.
