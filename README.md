<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/unify/main/assets/brand/unify-readme-banner-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/unify/main/assets/brand/unify-readme-banner-light.png" alt="unify" width="100%">
  </picture>
</p>

<p align="center">
  <a href="https://unify.ai">unify.ai</a> · <a href="https://discord.com/invite/sXyFF8tDtm">Discord</a> · <a href="LICENSE">MIT license</a>
</p>

# unify

**unify is the brain of an AI teammate that runs entirely on your machine: a persistent reasoning loop above a code-writing actor, typed memory in a local SQLite store, a skill library that grows from work that went well, and a steering protocol that reaches into any running operation. No backend, no accounts, no infra. One LLM key and `python -m unify`.**

The shape is deliberately human-in-the-loop: an assistant that keeps moving while you steer it, not one that replaces the person steering.

Every conversation is distilled into **typed, queryable memory** (contacts, knowledge, transcripts, files, each in its own table, not transcript soup or markdown files you maintain by hand), so the assistant knows what your weekend rewrite is for, which libraries you care about, and the regression you asked it to watch out for last Wednesday.

After a successful run it **promotes what worked into a personal skill library** (executable Python *plus* the procedural how-to prose to use it) that every future session consults before reaching for raw tools. A stored function is not *trusted* on day one: its side-effect class is read off its code, its contract is checked around every call, and any change to the code, its dependencies, its environment or its linked guidance puts it back at the start of that ramp.

**At a glance, vs the closest open-source alternatives:**

|  | unify | OpenClaw | Hermes Agent |
|---|---|---|---|
| Persistent reasoning loop *above* the tool-caller | ✓ | no | no |
| Mid-flight steering (pause / redirect / interject) | ✓ | abort + redeliver | text injection |
| Typed memory tables (contacts, knowledge, transcripts) | ✓ | markdown / JSONL | markdown + SQLite |
| Auto-grown skill library (executable code + prose) | ✓ | skills | skills |
| Runs in one process on your machine | ✓ | gateway + agent runs | single loop |

---

## Install

**Prerequisites:** Python 3.12+, [uv](https://docs.astral.sh/uv/), and one LLM provider key (OpenRouter, Anthropic, or DeepSeek; OpenAI models are reached through OpenRouter). macOS, Linux, or WSL2.

`unify` and its LLM client `unillm` are sibling checkouts linked by an editable install:

```bash
git clone https://github.com/unifyai/unify.git
git clone https://github.com/unifyai/unillm.git
cd unify
uv sync --all-groups
cp .env.example .env      # add your provider key
```

Then start chatting:

```bash
.venv/bin/python -m unify
```

```text
> What did I leave half-finished on the indexer rewrite last week?
> Here's the benchmark spreadsheet, which configs regressed?
> Draft a note to Sarah with the numbers that changed.
```

Everything the assistant remembers lives under `~/.unify/` (`UNIFY_HOME`): the SQLite store, the embeddings cache, the `workspace/` directory the actor reads and writes files in, and the runtime logs. Delete the directory and you have a fresh assistant. `/help` inside the chat lists the few slash commands (attach a file, quit); `unify --debug` streams the runtime logs to the terminal.

<details>
<summary>Configuration</summary>

`.env` holds the whole configuration; `.env.example` documents every key. The ones that matter:

| Variable | Purpose |
|---|---|
| `OPENROUTER_API_KEY` / `ANTHROPIC_API_KEY` / `DEEPSEEK_API_KEY` | At least one provider key |
| `UNIFY_MODEL`, `UNIFY_REASONING_EFFORT` | The default model (a unillm `model@provider` endpoint) and effort |
| `UNIFY_HOME` | Where the store, embeddings cache and workspace live (default `~/.unify`) |
| `UNIFY_STORE_PATH` | An explicit path for the SQLite store |
| `UNIFY_EMBED_MODEL` | Local `fastembed` model for vector columns, or `<model>@openrouter` |
| `UNILLM_CACHE` | Cache LLM responses locally; later runs replay identical calls |
| `ASSISTANT_FIRST_NAME`, `USER_FIRST_NAME`, … | Optional identity for the assistant and its user |

</details>

---

## What works

- **Chat** with one assistant in the terminal. Every message you send is a normal inbound event; every reply is a normal outbound one, so the same loop drives any front end you put on it.
- **Work in the background.** "Look into X" dispatches a code-writing actor; the conversation keeps going while it runs, and you can ask it how it is doing, redirect it, pause it, or stop it.
- **Skills.** Functions and guidance the assistant stored after a job that went well, discovered before it writes new code.
- **Files.** Name a file path in the chat and the actor reads it where it is; anything it produces lands in the workspace.

---

## Steering while work is in-flight

When the assistant is mid-task, steer it the way you would steer a colleague: **send another message**.

```text
> Actually, narrow it to ones with Rust bindings.
> What step are you on?
> Pause that, something urgent.
```

Each message wakes the slow brain, which can answer you directly or redirect in-flight work through its action-steering tools (`interject_*`, `ask_*`, `pause_*`, `resume_*`, `stop_*`). Nothing restarts; the correction propagates down the live call stack into whatever manager loop is currently running.

```text
You          ▸  "Find me high-throughput vector DBs under Apache 2."
Assistant    ▸  (start searching)
You          ▸  "Actually, narrow it to ones with Rust bindings."
Assistant    ▸  (adjust the in-flight search, don't restart)
You          ▸  "Pause that, something urgent."
Assistant    ▸  (freeze exactly where they are)
... five minutes later ...
You          ▸  "OK, resume. How's it going?"
Assistant    ▸  (pick up where they left off, give you a status update)
```

```text
Assistant    ▸  Three tasks running at once.
                  [0] watch_pr_reviews    ██████████░░░  in progress
                  [1] digest_releases     ████████████░  in progress
                  [2] retry_failed_build  ██░░░░░░░░░░  starting
                Each one independently inspectable, steerable, and pausable.
```

---

## Highlights

<table>
<tr><td><b>Interruptible mid-task</b></td><td>Every operation can be paused, resumed, redirected, or queried while it's running, including operations <i>nested inside other operations</i>, all the way down.</td></tr>
<tr><td><b>Plans in code, not tool-by-tool</b></td><td>Multi-step work is one sandboxed Python program with real variables, loops, and control flow, not a chain of one-tool-at-a-time JSON decisions.</td></tr>
<tr><td><b>Structured memory, not transcript soup</b></td><td>Contacts, knowledge, transcripts, and files live in typed, queryable tables, distilled from conversations every fifty messages, not piled into markdown.</td></tr>
<tr><td><b>Learns reusable skills, and earns trust in them</b></td><td>After a successful trajectory, the assistant saves both the underlying Python (with metadata + venv) and the procedural prose for using it. The next session composes them into a plan instead of re-deriving. Stored functions carry a verification ledger: their side-effect class is read off the code, their contract is checked around every call, and any change to the code, its dependencies, its environment or its linked guidance puts them back on the ramp.</td></tr>
<tr><td><b>Concurrent work, independently steerable</b></td><td>Multiple actions run at once: pause one, redirect another, ask a third for status, without affecting the rest.</td></tr>
<tr><td><b>Local-first, fully open</b></td><td>Runtime, persistence and LLM client are MIT-licensed and run in one process on your laptop. The store is a SQLite file you can open with any tool.</td></tr>
</table>

---

## How it works

A persistent **interaction loop** (`ConversationManager`) stays present across the conversation and keeps thinking while work is in flight. When something needs deeper reasoning, it dispatches a **background reasoner** (`Actor`) that writes Python plans over a back office of typed state managers. Every operation returns a live, steerable handle, and those handles nest: a correction you make in chat propagates *down* through the dispatched action into whatever manager call is currently running.

This is the same **interaction loop / background reasoner** split [articulated by Thinking Machines](https://thinkingmachines.ai/blog/interaction-models/): they put it *inside the model* (one model trained to interact natively); unify arrives at the same shape at the harness level.

```text
You ──► ConversationManager (slow brain: event-driven, single-shot tool decisions)
            │
            │  act(...) / interject / ask / pause / resume / stop
            ▼
        CodeActActor (writes one Python program per turn in a persistent sandbox)
            │
            ▼
        Skill libraries (stored functions + procedures, discovered before writing code)
            │
            ▼
        unify.db (in-process SQLite store: contexts, rows, derived vector columns)
```

**Dispatch flows down; steering flows back up the same path.** Every level returns the same `SteerableToolHandle`, so a mid-flight redirect doesn't abort the run, doesn't append a second prompt, and doesn't wait for the next tool boundary. It propagates through the live nested call stack as a typed signal any inner manager loop can act on.

---

## Under the hood

### Steerable handles: the universal protocol

Every public manager method returns one: the same `ask`, `interject`, `pause`, `resume`, `stop` surface at every level of the call stack.

```python
handle = await actor.act("Survey high-throughput vector DBs and draft a comparison")
await handle.interject("Only ones with Rust bindings")   # mid-flight redirect
await handle.pause(); ...; await handle.resume()         # freeze and resume
```

When the Actor calls `primitives.actor.act(...)`, the nested actor returns its own handle, nested inside the Actor's, which is nested inside the `ConversationManager`'s. Steering at any level propagates down through the live call stack as a typed signal any inner loop can act on, not as an abort or a queued prompt.

### CodeAct: the Actor writes Python programs

Most agents emit one JSON tool call at a time and let the LLM stitch results across turns. unify's Actor writes a single Python program per turn in a persistent sandbox, calling stored functions and nested actors from code:

```python
rows = load_orders("~/exports/orders.csv")          # a stored function, discovered first
by_status = {}
for row in rows:
    by_status[row["status"]] = by_status.get(row["status"], 0) + row["amount"]
note = await primitives.actor.act(f"Write a short note explaining {by_status}")
```

A load → reshape → delegate sequence becomes one coherent plan with real variables, loops, and control flow, rather than separate tool-selection turns round-tripping through tool messages.

### The local store

`unify.db` is an in-process SQLite engine with the shape of a document store: **projects** hold **contexts** (tables), contexts hold **rows** of JSON with typed **fields**, and a context can declare unique keys, auto-counted ids and **derived columns** whose equations are evaluated on write. Filters and sort keys are ordinary Python expressions evaluated per row (`age > 30 and 'berlin' in city.lower()`), with `embed()` and `cosine()` available so a derived vector column and a nearest-neighbour sort need no external service. Embeddings come from a local `fastembed` model by default and are cached on disk.

Everything the assistant keeps — chat history, functions, procedures — goes through this one API, so the whole assistant is one file you can back up, inspect, or delete.

### Functions and Guidance: a dual library

Two persistent libraries the Actor consults before reaching for raw tools:

- **`FunctionManager`**: executable Python (with metadata and a venv) the Actor composes into plans.
- **`GuidanceManager`**: procedural how-to prose (SOPs, software walkthroughs, multi-step strategies).

After a successful trajectory, a reviewer loop (`store_skills`) can extract *both*: code worth keeping plus the narrative for using it.

### Stored functions carry a verification ledger

A stored function is not trusted because it was stored. Its **effect class** is read deterministically off its code (`safe_noop` < `read_only` < `idempotent_effectful` < `unsafe_effectful`), a **contract** is derived from its type hints plus whatever the reviewer wrote down, and tier-0 checks validate the arguments before every call and the result after it. Any change to the source, its dependencies, its environment or its linked guidance invalidates that trust.

### Concurrent steerable actions

```text
┌─ In-Flight Actions ────────────────────────────────┐
│                                                     │
│  [0] watch_pr_reviews    ██████████░░░  In progress │
│      → ask, interject, stop, pause                  │
│                                                     │
│  [1] digest_releases     ████████████░  In progress │
│      → ask, interject, stop, pause                  │
│                                                     │
│  [2] retry_failed_build  ██░░░░░░░░░░  Starting     │
│      → ask, interject, stop, pause                  │
│                                                     │
└─────────────────────────────────────────────────────┘
```

Each action gets its own dynamically-generated steering tools on the slow brain's tool surface: inspect, interject, pause, resume, or stop any one without touching the rest.

### Putting it together

For the full breakdown (async tool loop internals, event bus, primitive registry, the store) see [`ARCHITECTURE.md`](ARCHITECTURE.md). The manager map at a glance:

```text
ConversationManager (interaction loop, event-driven scheduling)
    │
    ▼
CodeActActor (generates Python plans, calls primitives.* APIs)
    │
    ▼
Skill libraries (discovered before the Actor writes code)
    │
    ├── FunctionManager      : stored functions, venvs, verification ledger
    └── GuidanceManager      : procedures, how-to knowledge
    │
    └── EventBus             : typed pub/sub backbone (Pydantic events)
```

---

## Bring your skills with you

OpenClaw and Hermes Agent both represent skills as `SKILL.md` files (the [agentskills.io](https://agentskills.io) standard: YAML frontmatter + a markdown body, with optional bundled `scripts/`). That maps almost one-to-one onto a `GuidanceManager` entry, so either skill library can be imported off-the-shelf as guidance:

```bash
# Dry run (the default): print what would be imported, write nothing
.venv/bin/python -m scripts.skill_migration.openclaw_to_guidance
.venv/bin/python -m scripts.skill_migration.hermes_to_guidance

# Import for real (titles are namespaced "[openclaw] …" / "[hermes] …")
.venv/bin/python -m scripts.skill_migration.openclaw_to_guidance --execute
.venv/bin/python -m scripts.skill_migration.hermes_to_guidance  --execute
```

Each script looks for a sibling checkout (`../openclaw`, `../hermes-agent`) by default; pass `--repo-root` to point elsewhere. A skill's `description` and markdown body become the guidance `content`, and any bundled `scripts/` are inlined verbatim as a textual reference. Promoting that inlined code into a runnable `FunctionManager` function (and linking it back via `function_ids`) is a separate, deliberate step. Re-runs skip titles that already exist; pass `--conflict overwrite` to update them in place instead.

---

## Steering in practice: six things a single agent loop can't do

Because *every* operation, at every level of the call stack, returns the same live `SteerableToolHandle`, a handful of interactions become natural that a single blocking agent loop (which can ultimately only *abort* or *wait*) can't express.

<details>
<summary><b>1. Course-correct a task that's running three loops deep, live</b></summary>

Kick off work that nests `ConversationManager → Actor → nested Actor`. Halfway through, say *"use the March export, not February."* The correction travels **down the live call stack** into the innermost loop and changes its behaviour, no restart, no second prompt appended, no waiting for the next tool boundary. A monolithic loop can only hard-interrupt the child and start it over from scratch.

</details>

<details>
<summary><b>2. Ask a busy task what it's doing, without disturbing it</b></summary>

`handle.ask("what step are you on and why?")` spins up a **read-only inspection loop** over the task's in-flight transcript and returns an answer while the task keeps running, recursing into deeper nested handles if you want detail. You're interrogating live reasoning mid-flight, not polling a status string the agent remembered to update.

</details>

<details>
<summary><b>3. Freeze a nested operation, look inside, resume exactly where it left off</b></summary>

`pause()` halts new reasoning at the current point, propagating across the whole nested stack, while you inspect intermediate state or interject a constraint. `resume()` picks up from exactly where it stopped. An interrupt-only model can *stop*, but it can't freeze-and-continue.

</details>

<details>
<summary><b>4. Run three tasks at once and steer each one differently</b></summary>

Hold a live handle to each of several concurrent actions. **Pause** one, **interject** a new constraint into another, **stop** a third, all while the orchestrator keeps reasoning and the rest run untouched. Each gets its own dynamically-generated steering tools on the orchestrator's surface. Delegation that blocks the parent until a child returns offers no per-task live control.

</details>

<details>
<summary><b>5. Surface a clarification from the innermost loop, and route the answer back down</b></summary>

When an inner manager hits genuine ambiguity, its clarification **bubbles up through every intervening layer** to you; your answer flows back **down** to the loop that asked, and the original deep operation completes, without unwinding the stack. A single-level clarification primitive can't surface a question from three orchestration layers down.

</details>

<details>
<summary><b>6. Stop one branch of a fan-out without touching its siblings</b></summary>

`stop()` a single nested branch, with a reason that's recorded as a synthetic tool call in the transcript, while its sibling branches carry on. A thread-scoped abort flag is all-or-nothing across a subtree; here the cut is surgical.

</details>

---

## The runtime stack

Two MIT-licensed repos make up the local runtime.

| Repo | Role |
|------|------|
| **unify** (this) | Agent runtime: managers, tool loops, CodeAct, the local store, the chat loop |
| **[unillm](https://github.com/unifyai/unillm)** | LLM access layer: OpenRouter, Anthropic, DeepSeek, or any compatible endpoint, with response caching |

---

## Running the tests

Tests exercise the real system (steerable handles, CodeAct, manager composition, nested tool loops) against a per-process SQLite store with cached LLM responses:

```bash
uv sync --all-groups

tests/parallel_run.sh tests/                    # everything
tests/parallel_run.sh tests/actor/              # one module
tests/parallel_run.sh tests/function_manager/   # another
```

See [tests/README.md](tests/README.md) for the full philosophy: responses are cached, not mocked. Delete the cache and you're re-evaluating against live models.

---

## Where to start reading

| File | What's there |
|------|-------------|
| `unify/common/async_tool_loop.py` | `SteerableToolHandle`: the protocol everything returns |
| `unify/common/_async_tool/loop.py` | The async tool loop engine: nesting, steering, context propagation |
| `unify/actor/code_act_actor.py` | CodeAct: plan generation, sandbox, primitives |
| `unify/conversation_manager/conversation_manager.py` | The slow brain: debouncing, in-flight actions, event loop |
| `unify/conversation_manager/domains/brain_action_tools.py` | How the brain starts, steers, and tracks concurrent work |
| `unify/db/engine.py` | The local store: contexts, rows, derived columns, commits |
| `unify/db/expressions.py` | The row expression language behind every filter and sort |
| `unify/function_manager/primitives/registry.py` | How primitives are assembled into the typed API surface |
| `unify/events/event_bus.py` | Typed event backbone |
| `unify/memory_manager/memory_manager.py` | Offline consolidation pipeline |

---

## Project structure

```text
unify/
├── unify/             # Main package: cli, actor, conversation_manager, function_manager, guidance_manager, db, common
├── tests/             # Pytest suite (cached LLM responses, per-process SQLite store)
├── scripts/           # Skill import, builtins seeding, dev tooling
└── docs/              # Design writeups
```

---

## License

MIT. See [LICENSE](LICENSE).

Built by the team at [unify](https://unify.ai).
