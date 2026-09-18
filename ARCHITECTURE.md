# Architecture

This document describes Unify's internal architecture for developers who want to understand how the system works, contribute, or evaluate the design decisions.

## Mental model

Unify implements an AI assistant's brain as a persistent **ConversationManager** that talks to the user, above a central **Actor** that writes Python programs to do the work, backed by two **skill libraries** the Actor consults before it writes code: `FunctionManager` (stored functions) and `GuidanceManager` (procedures). What the assistant learns from a job that went well flows back into those libraries through a storage review.

Most public operations in the system, from a brain turn to a multi-step plan, run inside an **async LLM tool loop** and return a **steerable handle**. These handles are the universal interface for steerable work: you can pause, resume, interject into, ask questions about, or stop any operation — at any nesting depth — while it's running. The skill libraries are the exception: they expose direct CRUD methods as Actor JSON tools, not NL tool loops.

```
User (terminal chat)
 │
 ▼
ConversationManager ── event-driven, one tool decision per turn
 │
 │  starts actions, steers in-flight work
 ▼
CodeActActor ── one Python program per turn in a persistent sandbox ──►
 │
 │  execute_function runs a stored function; primitives.actor nests actors;
 │  FunctionManager_* / GuidanceManager_* are typed tools
 ▼
┌───────────────────────────────────────────────────────┐
│  Skill libraries                                      │
│                                                       │
│  FunctionManager ─── stored functions, venvs          │
│  GuidanceManager ─── procedures, builtins catalogue   │
│                                                       │
│  EventBus ─── typed pub/sub backbone                  │
└───────────────────────────────────────────────────────┘
 │
 ▼
unify.db ── one SQLite file: projects, contexts, rows, derived columns
```

Steering propagates through the full tree: stopping the Actor stops its nested loops; interjecting into the ConversationManager can reach a deeply nested in-flight tool loop.

The assistant is **reactive**: every piece of work starts from a message the user sent or from work those messages started. There is no scheduler, no timer wheel and no inbound channel other than the in-app chat, so the runtime is one process with no listeners.

---

## The async tool loop

**Files:** `unify/common/async_tool_loop.py`, `unify/common/_async_tool/loop.py`

The async tool loop is the primary runtime for steerable manager methods. Those methods typically: create an LLM client, register domain-specific tools, start a loop, and return a handle. Typed catalogues instead expose direct CRUD/lifecycle methods consumed as Actor JSON tools.

### How it works

```
┌──────────────────────────────────────────────┐
│             async_tool_loop_inner            │
│                                              │
│  1. Send messages to LLM                     │
│  2. LLM returns tool calls                   │
│  3. Execute tools in parallel (with limits)  │
│  4. Collect results, append to transcript    │
│  5. Check for interjections, pauses, stops   │
│  6. Repeat until LLM produces final answer   │
│                                              │
│  At any point between steps:                 │
│   - Interjections are drained from the queue │
│   - Pause events block until resumed         │
│   - Stop events trigger graceful exit        │
│   - Context compression fires if too long    │
└──────────────────────────────────────────────┘
```

The loop handles:

- **Parallel tool execution** with configurable concurrency limits
- **Interjection queue** — new instructions injected mid-flight without restarting
- **Pause/resume** via asyncio events
- **Context compression** — when the conversation exceeds the model's context window, the loop compresses and restarts transparently
- **Dynamic tools** — tools that are generated at runtime based on the current state (e.g., per-action steering tools)
- **Tool policies** — gating which tools are available at which step (used for discovery-first patterns)
- **Time awareness** — optional wall-clock context injected after each tool completion
- **Prompt caching** — cooperative cache-control headers for providers that support it

### The interjection mechanism

When `handle.interject("also check trains")` is called, the message is placed in an asyncio queue. Between LLM turns, the loop drains this queue and injects the messages as new user turns. If `interrupt_llm_with_interjections` is enabled, an in-flight LLM call is cancelled and restarted with the interjection included.

This is how the user can redirect an agent mid-task without the overhead of stopping and restarting from scratch.

### Steering code that is already running

**Files:** `unify/function_manager/steering.py`, `steering_patcher.py`

Between LLM turns is not enough for `execute_code` and `execute_function`,
whose work happens *inside* one tool call. A correction arriving four sends
into a five-send loop must reach the loop, not merely kill it.

This is an execution-engine concern rather than an actor one — it applies
wherever Python runs — and it exists only while a call is in flight. Four
parts:

- **Probes.** An AST pass injects `_cp` (yield, honour pause, take
  corrections) and `_int` (raise if a patch targets this function) at function
  entry and at the top of every loop iteration, plus loop and branch context
  probes, and `_around_cp` around awaited dispatches — including awaits nested
  inside expressions, so a comprehension or a `gather` is covered too.
- **An idempotency cache.** Every dispatch through a tool namespace is
  memoised under `(tool, args, occurrence)`. The key deliberately excludes
  execution position: the commonest correction narrows a loop with a filter,
  which puts every surviving call inside a new branch, and a positional key
  would change their identity and re-send everything already sent.
- **Retry in place.** `ControlledInterruption` unwinds to a retry loop that
  splices the patched function into the *source* — not the namespace, which
  re-running the block would overwrite — and runs again. Completed dispatches
  replay, so execution reaches the first real change having redone nothing.
- **A bounded lifetime.** Cache, patches and probes are created when the call
  begins and discarded when it returns. That bound is what makes the rest
  sound: no cache key can outlive the execution it describes.

The patch itself is written by an LLM owned by the execution engine
(`steering_patcher`), given the running source and the calls already
completed, and restricted to rewriting functions the block defines.

Two limits. Replay records that a side effect happened; it cannot undo one, so
invalidation is explicit rather than inferred. And a probe only runs when the
block yields — synchronous blocking work holds the event loop, and during that
window the correction cannot even arrive.

---

## Steerable handles

**File:** `unify/common/async_tool_loop.py`

`SteerableToolHandle` is the abstract protocol. `AsyncToolLoopHandle` is the concrete implementation backed by an asyncio task. Every public manager method returns one.

```python
class SteerableToolHandle(ABC):
    async def ask(self, question) -> SteerableToolHandle  # inspect the running work
    async def interject(self, message)                     # inject new instructions
    async def stop(self, reason=None)                      # cancel
    async def pause(self)                                  # freeze without cancelling
    async def resume(self)                                 # unfreeze
    def done(self) -> bool                                 # completion check
    async def result(self) -> str                          # await final answer
    async def next_clarification(self) -> dict             # bottom-up question from tool
    async def next_notification(self) -> dict              # bottom-up status update
    async def answer_clarification(self, call_id, answer)  # respond to tool's question
```

### Nested steering

When the Actor calls `primitives.actor.act(...)`, the nested actor starts its own tool loop and returns its own `SteerableToolHandle`. This inner handle is tracked by the Actor's loop. When the user calls `handle.pause()` on the Actor's handle, the pause propagates to all active inner handles via a **mirror queue** mechanism:

1. The outer handle receives `pause()`
2. It sets its own pause event and enqueues a `_mirror` sentinel
3. The inner loop drains the mirror, synthesizes a helper tool call in the transcript (so the LLM sees it happened), and dispatches `pause()` to each child handle
4. Child handles repeat the process recursively

This gives full-depth steering with transcript visibility at every level.

### `ask()` — inspecting a loop, running or completed

`ask()` is not a simple state peek, and it represents the inspected loop
differently depending on whether it has finished:

- **Still running** — a live snapshot: the transcript so far, compact-
  serialized, with roles transformed to `inner_user`/`inner_assistant` (so
  the inspection LLM distinguishes the inspected conversation from its own).
- **Completed** — a compact **digest** instead: built once, mechanically (no
  LLM call), from data the loop already retains — the original request, each
  tool call's name/`thought`/result preview+size, source URLs seen, and the
  final result. Cached on the handle, so a second question about the same
  completed handle reuses byte-identical text and hits the provider's prefix
  cache. A `read_child_message(idx)` tool is offered alongside it so the
  inspection loop can fetch any one message from the completed transcript
  verbatim (capped at 32KB) when the digest's previews aren't enough.

Both paths otherwise follow the same shape:

1. Creates a fresh LLM client with the snapshot/digest as system context
2. Starts a **new, read-only tool loop** to answer the question
3. If the inspected loop has its own inner handles, their `ask_*` tools are forwarded to the inspection loop, enabling recursive drill-down

This means you can ask "what's the flight search doing?" and the inspection loop can, if needed, call `ask_flight_search()` to query the inner handle's own transcript.

### `forward_handle_call` — signature adaptation

Different handle implementations extend the base signature with domain-specific kwargs (e.g., `ConversationManagerHandle.interject` adds `pinned`). `forward_handle_call` introspects the target method's actual signature, filters out unsupported kwargs, and applies positional fallbacks — so delegation boundaries work without hand-written adapter code.

---

## The CodeAct Actor

**File:** `unify/actor/code_act_actor.py`

The Actor doesn't pick from a JSON tool menu. It generates Python programs in a persistent sandbox:

```python
rows = load_orders("~/exports/orders.csv")        # a stored function, discovered first
summary = {s: sum(r["amount"] for r in rows if r["status"] == s) for s in ("paid", "refunded")}
report = await primitives.actor.act(f"Write a two-paragraph note on {summary}")
```

This runs in a `PythonExecutionSession` — a persistent sandbox where stored functions are callable by name, `primitives.actor` spawns nested actors, and state survives between turns.

### Why CodeAct over JSON tools

JSON tool calling forces every composition to be a separate round-trip. To load a file, reshape it and hand the result to a nested actor, the LLM would need several turns of tool selection; in Python it is one program with real variables, loops and control flow.

The Actor still uses the async tool loop internally (the LLM generates code as a "tool call" that gets executed), so it inherits all the steering, compression, and observability infrastructure.

### Discovery-first tool policy

The Actor implements a **gating policy**: until the LLM has queried both `FunctionManager` (what stored functions exist?) and `GuidanceManager` (what procedures/SOPs apply?), the full tool surface is hidden. This forces an explore-then-act pattern that prevents the LLM from jumping to action before understanding what's available.

### Primitives registry

**File:** `unify/function_manager/primitives/registry.py`

`ToolSurfaceRegistry` is the single source of truth for what the sandbox exposes under `primitives`: the method surface of each namespace (today `primitives.actor`, the nested-actor entry point), its tool schemas, the prompt context that describes it, and the sandbox's global state — all from one declaration.

### Storage review

**Files:** `unify/actor/code_act_actor.py` (`_start_storage_check_loop`)

After a run completes — and after each completed turn of a persistent session — a **storage review** loop reads the trajectory and decides whether anything is worth persisting: a stored function (code that worked) or a procedure (how to compose things). Often nothing is.

---

## The skill libraries

Each library follows the same pattern:

1. A **base class** (`base.py`) defines the public API as abstract methods with rich docstrings. These docstrings are the LLM-facing contract — they're attached to concrete implementations via `functools.wraps`, so the Actor reads one description whichever implementation is behind it.

2. A **concrete implementation** whose CRUD methods are exposed to the Actor as `FunctionManager_*` / `GuidanceManager_*` JSON tools.

3. A **simulated implementation** with the same signatures, used by tests that exercise the actor's routing without paying for the real library.

**FunctionManager** — Stored Python functions with metadata, per-function venvs, and execution in-process or out-of-process. Also the read-only builtins catalogue of every primitive the Actor can call.

**GuidanceManager** — Procedures: step-by-step instructions, walkthroughs, and strategies for composing functions. Linked to functions by id, so a rule change finds every implementation that embeds it. Reads federate over a global builtins catalogue of imported Agent Skills.

---

## The ConversationManager

**File:** `unify/conversation_manager/conversation_manager.py`

The ConversationManager is the top-level orchestrator for the live conversation. It has a fundamentally different design from the other managers because it handles real-time interaction: it sees the full picture — the chat thread, notifications, in-flight actions, system state — and makes deliberate decisions about what to do. It uses a single-shot tool decision pattern (one LLM call → one action) rather than a multi-turn loop, because the user might send another message at any moment.

### Events in, events out

Inbound messages arrive as `UnifyMessageReceived` events on an in-memory event broker; replies leave as `UnifyMessageSent` events. The terminal chat in `unify/cli.py` is one client of that broker, and any other front end drives the same loop the same way.

### In-flight action tracking

The ConversationManager maintains `in_flight_actions` — a dict of currently running steerable handles with metadata. For each action, it dynamically generates steering tools (`ask_<action>`, `interject_<action>`, `stop_<action>`, `pause_<action>`, `resume_<action>`) that the brain's LLM can call. This is how "how's the flight search going?" routes to the right handle.

### Event-driven scheduling

The ConversationManager uses a `Debouncer` that coalesces rapid-fire events (new messages, action completions, notifications) into batched brain invocations. This prevents thrashing when multiple things happen simultaneously.

---

## The local store

**Files:** `unify/db/engine.py`, `unify/db/expressions.py`

`unify.db` is an in-process SQLite engine with the shape of a document store:

- **Projects** hold **contexts** (tables), and contexts hold **rows** of JSON with typed **fields**.
- A context can declare **unique keys**, **auto-counted ids**, **foreign keys** (with `CASCADE` / `SET NULL` propagation through scalar, list and nested-list references) and **derived columns** whose equations are evaluated on every write.
- **Filters and sort keys** are Python expressions evaluated per row by an AST walker (never `eval`), with SQL-style `None` propagation and builtins such as `exists` and `now`.
- **Commits** snapshot a context or a whole project and can be rolled back, which is what the test fixtures use to reset scenarios.

The public API (`db.get_logs`, `db.create_logs`, `db.update_logs`, `db.create_context`, …) is what every manager reads and writes through, and the whole assistant is one file under `UNIFY_HOME`.

---

## The event bus

**File:** `unify/events/event_bus.py`

The EventBus is an in-process, asyncio-friendly pub/sub system with:

- **Typed payloads** — all events are Pydantic models declared in `events/types/`. Invalid payloads are rejected at publish time.
- **Searchable history** — events are stored in a windowed deque per type, queryable with filters.
- **Callback registration** — subscribe to event types with async callbacks.
- **Callback cascade tracking** — `_CURRENT_ROOT_SEQ` (a context variable) tracks which callback triggered which, so `join_callbacks()` can await an entire cascade deterministically.
- **Optional persistence** — the bus can persist events to `Events/*` contexts in the store and prefill itself from them on the next start.

Managers and tool loops publish structured events (tool calls, steering actions, method boundaries) via `to_event_bus()`. This feeds runtime coordination (MemoryManager reacts to message events) and observability.

### Lineage and hierarchy

Every tool loop has a **lineage** — a list of string segments tracking its position in the nesting tree, propagated via `TOOL_LOOP_LINEAGE` (a `ContextVar`). Each segment includes a random suffix for per-invocation identity:

```
["ConversationManager.act(a1b2)", "Actor.act(c3d4)", "Actor.act(e5f6)"]
```

This lineage is attached to every event the loop publishes, enabling full parent-child correlation in logs.

---

## Context propagation

**File:** `unify/common/_async_tool/propagation_mode.py`

When a tool loop calls a nested tool that starts its own loop, the parent conversation may need to be visible to the child (e.g., so a nested actor knows what the user originally asked). Unify handles this with explicit role transformation:

- **`outer_user` / `outer_assistant`** — parent conversation roles, injected into child loops as system context
- **`inner_user` / `inner_assistant`** — child conversation roles, visible when the parent inspects via `ask()`
- **`user` / `assistant`** — current conversation roles (always the active loop)

This three-layer separation prevents prompt injection between nesting levels and lets each LLM clearly distinguish "what the user said" from "what the calling manager said" from "what I'm doing."

`ChatContextPropagation` controls the policy:
- `ALWAYS` — always pass parent context to child tools
- `NEVER` — never pass it
- `LLM_DECIDES` — expose a boolean parameter so the LLM can opt out per tool call

---

## Multi-request coordination

**File:** `unify/common/_async_tool/multi_handle.py`

A single tool loop can serve **multiple concurrent requests** through the `MultiHandleCoordinator`. Each request gets:

- A unique `request_id`
- Its own clarification and notification queues
- Independent completion/cancellation
- Tagged interjections so the LLM knows which request a message belongs to

The LLM calls `final_answer(request_id, answer)` to complete specific requests. The loop continues until all requests are done (or persists indefinitely if `persist=True`).

This is used by the ConversationManager to handle multiple user messages that arrive while the brain is already processing — rather than queuing them sequentially, they're multiplexed through a shared loop with shared context.

---

## Testing

**Directory:** `tests/`

Tests use real LLM calls, never mocked. Responses are cached by UniLLM so that:

- First run: real inference, responses stored
- Subsequent runs: cached responses replayed in milliseconds
- Cache key = exact LLM input (change a prompt → automatic cache miss → fresh inference)

This means the test suite is both **deterministic** (cached runs are byte-for-byte reproducible) and **honest** (every cached response was produced by a real model given that exact input).

Tests run in parallel via `tests/parallel_run.sh`, which spawns each test in an isolated tmux session against its own SQLite store, with per-terminal tmux server isolation. Results stream inline as tests complete.

Tests fall on a spectrum between **symbolic** (infrastructure-focused: does steering work? does nesting propagate correctly?) and **eval** (capability-focused: did the assistant answer correctly?). The caching system makes both types fast after initial population.

**Synchronization** uses trigger-based helpers (`tests/async_helpers.py`) rather than sleeps, making tests robust to the 1000x timing difference between cached (milliseconds) and live (minutes) LLM calls.

---

## System dependencies

Unify persists state in its own SQLite file and makes LLM calls through **UniLLM** (a caching/tracing/normalization layer in a sibling repo).

```
Unify ──► unify.db ──► <UNIFY_HOME>/store.sqlite
  │
  └─────► UniLLM ──► OpenRouter / Anthropic / DeepSeek
```

The core architecture (handles, loops, CodeAct, manager composition) is independent of the specific persistence layer.

---

## Directory layout

```
unify/
├── unify/
│   ├── cli.py                          # Terminal chat, `python -m unify`
│   ├── workspace.py                    # The assistant's working directory
│   ├── db/
│   │   ├── engine.py                   # The store: contexts, rows, derived columns, commits
│   │   └── expressions.py              # The row expression language
│   ├── common/
│   │   ├── async_tool_loop.py          # SteerableToolHandle, start_async_tool_loop
│   │   └── _async_tool/
│   │       ├── loop.py                 # async_tool_loop_inner (the engine)
│   │       ├── loop_config.py          # LoopConfig, TOOL_LOOP_LINEAGE
│   │       ├── multi_handle.py         # MultiHandleCoordinator
│   │       ├── propagation_mode.py     # ChatContextPropagation enum
│   │       ├── context_compression.py  # Transparent context compression
│   │       ├── dynamic_tools_factory.py # Runtime tool generation
│   │       └── messages.py             # forward_handle_call, mirror dispatch
│   ├── actor/
│   │   ├── base.py                     # BaseActor, BaseCodeActActor
│   │   ├── code_act_actor.py           # CodeActActor implementation
│   │   ├── execution/                  # PythonExecutionSession, sandbox
│   │   └── environments/               # Pluggable execution environments
│   ├── conversation_manager/
│   │   ├── conversation_manager.py     # ConversationManager (the interaction loop)
│   │   ├── events.py                   # Chat and actor events on the broker
│   │   └── domains/
│   │       ├── brain.py                # Brain spec construction
│   │       ├── brain_action_tools.py   # act, wait, and per-action steering tools
│   │       └── event_handlers.py       # One handler per event type
│   ├── guidance_manager/
│   ├── function_manager/
│   │   └── primitives/
│   │       ├── registry.py             # ToolSurfaceRegistry (single source of truth)
│   │       └── scope.py                # PrimitiveScope
│   ├── events/
│   │   ├── event_bus.py                # EventBus
│   │   └── types/                      # Pydantic event payloads
│   └── manager_registry.py             # Singleton factory for manager instances
├── tests/
│   ├── parallel_run.sh                 # Isolated parallel test runner
│   ├── async_helpers.py                # Trigger-based synchronization
│   └── <module>/                       # Tests mirror production structure
├── scripts/                            # Skill import, builtins seeding, git hooks
└── docs/writeups/                      # Design writeups
```

---

## Design principles

**English as an API.** Components communicate through natural-language interfaces: the brain speaks to the actor in a request, the actor to nested actors and to the skill libraries in words. This makes the system inspectable without reading implementation code — you can read the LLM transcripts and understand what happened.

**No heuristics, no regex routing.** If the system needs to respond correctly to a type of user input, the fix is always a prompt or tool docstring improvement that nudges the LLM, never a hardcoded rule that pattern-matches on the input.

**Fail loud.** No defensive try/except around things that shouldn't fail. No null checks for things that shouldn't be null. When assumptions break, the system crashes visibly rather than silently degrading.

**Aggressive refactoring.** Zero backward compatibility. When requirements change, code is rewritten to optimally support the new requirements, not patched with compatibility shims.

**Real LLMs in tests.** Never mocked. Cached for speed, but every cached response was produced by a real model. This catches prompt regressions that mocks would hide.
