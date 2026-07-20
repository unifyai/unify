## Task Scheduler – Architecture and Guide

This package manages the creation, scheduling, execution, and lifecycle of tasks. It provides:

- A public manager (`TaskScheduler`) that exposes `ask` / `update` / `execute` methods
- A simulated manager for demos and tests
- Strong types for tasks, schedules, and statuses
- An execution layer that delegates a single task run to the actor substrate
- A storage layer that centralizes Unify I/O


### High-level picture

- `TaskScheduler` is the orchestrator. It composes read/write "tools" and runs LLM loops for `ask` and `update`. The `execute` path does not use an async tool loop; it returns a `SteerableToolHandle` directly.
- Tasks are independent: there is no queue chaining or ordering between tasks. Each task holds only its own schedule, trigger, and status.
- All reads/writes go through `TasksStore` (Unify I/O).


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
   - User-authored scheduled task rows are projected by Orchestra into machine-facing activation rows.
   - Communication materializes scheduled live activations as Cloud Tasks targeting the adapters `/scheduled/tasks/due` endpoint.
   - The live wake reason is delivered to ConversationManager, which asks the slow brain to start with `primitives.tasks.execute(task_id=...)`.
   - Cloud Scheduler is used for platform maintenance jobs; per-task cadence is delivered by dynamic Cloud Tasks.

5) Trigger activation
   - Trigger definitions are projected into activation rows and mechanically matched by medium/contact filters when inbound communication events arrive.
   - Live trigger candidates are surfaced to the slow brain, which performs semantic acceptance and calls `primitives.tasks.execute(task_id=..., trigger_attempt_token=...)` so the run adopts the exact inbound provenance.
   - Recurring triggerable tasks clone a future triggerable instance before the current instance is marked active.

6) Offline activation
   - Offline means the hidden headless lane: the live ConversationManager and main actor are not woken.
   - Offline scheduled activations use Cloud Tasks targeting Communication's offline-dispatch endpoint, which creates a short-lived Unity Kubernetes job.
   - The job runs `offline_runner.py`, which starts the same actor substrate headlessly and delegates through `TaskScheduler.execute(...)`.
   - Offline delivery is independent from execution style. Agentic offline tasks keep `entrypoint=None`; symbolic offline tasks use a stored FunctionManager entrypoint.

7) Resource opt-ins
   - `requires_filesystem` and `requires_computer` are authored independently of `offline` and `entrypoint`.
   - When either is true, dispatch waits for a ready assistant desktop (Local sync and/or computer-use) before the run starts.

8) Concurrency
   - Multiple instances of the same `task_id` may be `active` at once (for example a 90-minute job on a 60-minute schedule).
   - `execute` only refuses to start when activation provenance targets the exact instance that is already `active`.
   - Symbolic entrypoints receive opt-in kwargs (`task_id`, `instance_id`, `task_execution_context`) so they can gate or skip themselves when desired.


### Schedule invariants (enforced centrally)

- Tasks with `schedule.start_at` must be `scheduled`.
- Trigger-based tasks (`trigger`) cannot also carry a schedule.
- Writes go through `_validated_write(...)`, which checks invariants, prevents direct `active` writes, and enforces schedule/trigger exclusivity.


### Storage

- All I/O runs through `TasksStore`.
- Environment toggle `UNITY_TASK_LOCAL_VIEW_OFF` is a no-op in the current design (kept for compatibility with env configs).


### Execution handle

- `ActiveTask`: the `SteerableToolHandle` returned by `execute`. Mirrors task status to the Tasks row as execution proceeds and handles cancellation interjections.
- Durable executions materialize a `Tasks/Runs` row (unique `run_key`) and stamp EventBus payloads with `task_id` / `instance_id` / `run_key` plus a `Task.run(...)` hierarchy segment.
- In `EVENTBUS_ORCHESTRA_PERSIST_MODE=allowlist`, Orchestra keeps a **dense** `Events/ManagerMethod` + `Events/ToolLoop` tree only while that lineage is set; interactive traffic stays on the tool allowlist. Join without duplicating payloads:

```python
from unify.task_scheduler.task_run_events import fetch_task_run_events

tree = fetch_task_run_events(run_key, events_base_context="{user}/{assistant}/Events")
```

  Runs may live under `Teams/{id}/Tasks/Runs` while Events stay under the executing assistant’s `…/Events/*` — join is by `run_key` value. Not yet exposed on `primitives.tasks.*`.


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
- `machine_state.py` handles activation validation and stale delivery rejection.
