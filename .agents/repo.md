# Unify

The assistant's brain, and the cognitive core of the platform.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first for the system design. This file
covers *how to work on the code*, not *what the code does*.

## What Unify is

Unify implements an AI assistant's brain as a **distributed back office**. A central `Actor` orchestrates specialized **state managers** (`ContactManager`, `KnowledgeManager`, `TaskScheduler`, `TranscriptManager`, `GuidanceManager`, `FunctionManager`, ...) through code-first plans. Most public manager methods run inside an **async LLM tool loop** and return a **steerable handle** that supports `ask`, `interject`, `pause`, `resume`, `stop` — all the way down the nesting tree. Typed catalogues such as Knowledge and Guidance expose direct CRUD/lifecycle methods as Actor JSON tools (`KnowledgeManager_*`, `GuidanceManager_*`) rather than NL tool loops or `primitives.*`.

Sibling repos consumed via editable installs (see `[tool.uv.sources]` in `pyproject.toml`):
- **`unisdk`** — Python SDK wrapping the Orchestra REST API
- **`unillm`** — LLM client with caching, provider normalization, observability

The open agent runtime (`unify`, `unisdk`, `unillm`) talks to the **hosted Orchestra backend** (`ORCHESTRA_URL`, default `https://api.unify.ai/v0`). `orchestra` and `console` are private/hosted and are not part of the open-source repo set.

Unify consumes data from Orchestra via `unisdk`, makes LLM calls via `unillm`, and
triggers external actions through the hosted communication stack in `unify-deploy`.
Console provides observability into its operations.

## Run the agent locally (public path)

The open-source runtime runs on your machine against the **hosted** Orchestra
backend. Provision a `UNIFY_KEY` and an assistant (`ASSISTANT_ID`) at
[console.unify.ai](https://console.unify.ai), then:

```bash
curl -fsSL https://raw.githubusercontent.com/unifyai/unify/staging/scripts/install.sh | bash
unify            # interactive local chat (alias: unify chat)
unify serve      # headless: ConversationManager + gateway
unify setup      # re-run the key/credential wizard
```

- `unify` is the CLI shim the installer drops in `~/.local/bin/`. From a
  checkout, the equivalents are `.venv/bin/python -m
  sandboxes.conversation_manager.sandbox` (chat) and `bash scripts/local.sh
  start --full` (headless).
- Configuration lives in `unify/.env`: `UNIFY_KEY`, `ASSISTANT_ID`,
  `ORCHESTRA_URL` (hosted), one LLM provider key, and optional voice/research
  keys (`scripts/prompt_byok_keys.sh`).
- No Docker, local Orchestra, or Console is involved in the public path. The
  onboarding flow, inbound channels, workspace connect, third-party app
  integrations, and screen-share are part of the hosted product.

**Internal full-local self-host stack.** The "all-repo fully local" stack
(local Orchestra + Console + Coordinator + gateway, via Docker Compose) is an
internal-only path and lives in the private **`unify-deploy`** repo under
`selfhost/` (`stack.sh`, `setup.sh`, `service.sh`, compose bundle). It drives
sibling `unify`/`console`/`orchestra` checkouts under `UNIFY_STACK_ROOT`.

## Development environment

```bash
# First-time setup (fresh clone)
pip install uv && uv sync --all-groups
```

- **Python interpreter:** always use `.venv/bin/python`. Never the system Python.
- **Bootstrap:** if `.venv/` is missing, `uv sync --all-groups` recreates it.
- **`uv.lock` and `package-lock.json` are protected** — never hand-edit them. Use the package manager.
- **Do not output `.env` or `*.key` contents to chat.**

### Running tests

Tests run in tmux sessions, each test in its own session, with logs streamed to `logs/pytest/`. The runner blocks until everything completes.

```bash
# Default — one session per test, max concurrency
tests/parallel_run.sh tests/contact_manager/

# Specific test
tests/parallel_run.sh tests/contact_manager/test_ask.py::test_name

# Serial mode (one session per file) for large suites
tests/parallel_run.sh -s tests/

# With timeout
tests/parallel_run.sh --timeout 300 tests/contact_manager/
```

Each agent (or terminal) gets an **isolated tmux server automatically**, so concurrent agents don't collide.

### When a test fails

1. **Never inspect tmux panes directly.** Read the corresponding log in `logs/pytest/<YYYY-MM-DDTHH-MM-SS_socket>/`.
2. **Use `Read` (not `cat`/`tail`)** — `logs/` is gitignored, so `Grep`/`Glob` won't find files there.
3. **Add temporary debug logs via `CURSOR_DEBUG_LOG`** — the only permitted logging mechanism for debugging. Grep for it (`rg CURSOR_DEBUG_LOG`) to find the project's util, then import and use it. Remove all calls before finalizing the fix.
4. **Clean up failed sessions** with `tests/kill_failed.sh` (or `tests/kill_server.sh` for everything).

### When CI is "stuck cancelling" / new matrices stay pending

Ordinary `gh run cancel` can no-op while jobs stay on `Run tests` for an hour+
and hold the Tests concurrency group. That is a **zombie run**, not
`if: always()` wind-down. Force-cancel immediately:

```bash
bash scripts/dev/force_cancel_stuck_tests.sh staging
```

Full decision tree: [`.agents/rules/ci-tests-cancel-zombies.md`](.agents/rules/ci-tests-cancel-zombies.md).

### Pre-commit

Install the hooks once per checkout (they run automatically on commit; CI runs the same pinned hooks):

```bash
./scripts/install-git-hooks.sh   # or: pre-commit install
```

Run them manually any time:

```bash
pre-commit run --all-files
```

## Provider integrations: no custom retries

Orchestra ``run_tool`` retries transient provider failures by default for
**every** backend (Composio, Pipedream, …): HTTP 429/5xx, timeouts, empty
bodies, GitHub GraphQL "Something went wrong while executing your query" on
**read** tools, and Pipedream action-level failures embedded in an HTTP-ok
body when attribution looks like network / upstream 429–5xx. Unify
``integrations.ops.run_tool`` additionally retries transient Orchestra
connectivity failures.

When authoring CodeAct plans, stored functions, or brain ticks that call
``primitives.integrations.*`` / Composio / Pipedream via Orchestra:

- Call the tool **once** and handle the final envelope (`ok`,
  `connect_required`, `confirmation_required`, `provider_error`, …).
- Do **not** wrap provider calls in ad-hoc `for attempt in range…` /
  `time.sleep` loops for flakiness — that duplicates policy and often misses
  GraphQL-in-payload or Pipedream embedded-action errors that Orchestra
  already classifies.
- Domain-specific **long** waits (e.g. sitting through a GitHub primary
  rate-limit reset for a multi-hour crawl) may remain at the call site; short
  transport/platform blips belong in the shared layer.

Opt out (ops/debug only): `ORCHESTRA_INTEGRATION_RETRY_MAX_ATTEMPTS=1` and/or
`UNIFY_INTEGRATION_TRANSPORT_RETRY_MAX_ATTEMPTS=1`.

## Testing philosophy

We **never** mock the LLM client. All tests use real LLM calls via `unillm.AsyncUnify`, with responses cached per unique input (`UNILLM_CACHE=true`, the default). First run is slow; subsequent runs replay from cache in milliseconds.

Tests sit on a **spectrum** between two paradigms — there's no binary classification:

- **Symbolic tests** use the LLM as a deterministic stub to exercise infrastructure (async tool loops, steering, state mutations). Failures = regression in programmatic code.
- **Eval tests** verify end-to-end *capability* ("did the assistant answer correctly?"). Failures may indicate prompt issues, tool design problems, or capability gaps.

**Never rely on sleeps** — use the trigger helpers in `tests/async_helpers.py` for deterministic ordering across cached (ms) and live (sec–min) timing.

### The cache is never the problem

"We just need to update the cache" is **never** a valid conclusion when debugging failures. The cache is a faithful replay mechanism keyed on the exact LLM input. If you change prompts or docstrings, the cache key changes automatically and you get fresh inference. If a cached response causes a failure, an LLM *actually made that decision* given that exact input — that's a prompt issue, not a stale-cache issue. Clearing the cache to "fix" a failing test is a category error.

### Tagging eval tests

```python
import pytest
pytestmark = pytest.mark.eval  # whole file

@pytest.mark.eval                # single test
async def test_natural_language_query(): ...
```

## No fast paths or heuristics

If a method needs to respond correctly to a class of user input, **always** address this by prompting the model and/or improving tool docstrings. Never apply regex-based or substring-based routing on user commands. The LLM is the router.

## State manager design

The public API of each state manager is defined by the abstract methods on `Base{SomeManager}` in `base.py`. These docstrings are the **LLM-facing contract** — they're attached to concrete implementations via `@functools.wraps`.

### Docstring rules

- **Implementation-agnostic.** Public docstrings must never reference other managers (cross-references rot) or the manager's own internal tools.
- **Tool-specific guidance lives in the tool's own docstring** — never in the prompt builder.
- **Compositional guidance (when to use tool A vs B, multi-tool patterns) lives in the prompt builder** — never in individual tool docstrings.

### Routing playbook (which manager owns what)

| Concern | Manager / primitive |
|---|---|
| People, contact records | `primitives.contacts.*` |
| Conversation history search | `primitives.transcripts.*` |
| Domain facts, typed knowledge claims | `KnowledgeManager_*` (top-level JSON tools, not primitives) |
| Durable tasks (create, execute) | `primitives.tasks.*` / `TaskScheduler` |
| Files (parse, query) | `primitives.files.*` |
| Storing new data or files, from any source | `primitives.ingestion.*` (`submit` — there is no `primitives.data.ingest`) |
| Generative UI (interactive views) | `primitives.canvas.*` |
| Web research (lightweight) | `primitives.web.*` |
| Secrets (metadata only via `ask`) | `primitives.secrets.*` |
| Procedural how-tos, SOPs | `GuidanceManager_*` (top-level JSON tools, not primitives) |
| Ephemeral live action | `Actor.act` (via ConversationManager) |
| Durable, tracked work | `TaskScheduler.execute` — never `update` to start work |

Full role descriptions are in [`.agents/rules/state-manager-roles.md`](.agents/rules/state-manager-roles.md).

### Cross-manager images

Images flow between managers **by filesystem path**, not by `image_id`. Receiving managers resolve to persistent storage via `ImageManager.filter_images(filter="filepath == '...'")` when needed. Managers with first-class image fields (e.g. `GuidanceManager`) accept structured `ImageRefs` types at their own API boundary.

## Additional git constraints

Beyond the shared git rules below: never use `git rebase -i` or `git add -i`
(interactive flags don't work in non-interactive shells), and never edit
`git config`.

## Repo map

```
unify/
├── unify/                   # Main package
│   ├── actor/               # CodeAct Actor, central orchestrator
│   ├── conversation_manager/ # Slow brain, live chat orchestration
│   ├── contact_manager/     # People + relationships
│   ├── knowledge_manager/   # Typed claim ledger (facts, policies, …)
│   ├── task_scheduler/      # Durable tasks, schedules, triggers
│   ├── transcript_manager/  # Conversation history
│   ├── guidance_manager/    # Procedures, SOPs
│   ├── function_manager/    # User Python functions + primitives registry
│   ├── file_manager/        # File parsing and registry
│   ├── image_manager/       # Image storage and vision queries
│   ├── web_searcher/        # Web research
│   ├── secret_manager/      # Encrypted secrets
│   ├── blacklist_manager/   # Blocked contacts
│   ├── data_manager/        # Low-level data ops
│   ├── memory_manager/      # Offline consolidation
│   ├── events/              # Typed event bus
│   ├── common/              # Async tool loop, shared infra
│   ├── deploy_runtime/      # Hosted deployment SPI (local default)
│   └── gateway/             # External comms gateway
├── agent-service/           # TypeScript service for browser-using agents
├── tests/                   # Pytest suite
├── sandboxes/               # Per-manager dev sandboxes
├── scripts/                 # Install, dev tooling
├── deploy/                  # Cloud Build, Docker, deploy configs
├── ARCHITECTURE.md          # System design (read first)
├── README.md
├── CONTRIBUTING.md
└── pyproject.toml
```

## When in doubt

- Check [`.agents/rules/`](.agents/rules/) for fuller context on any topic above.
- `ARCHITECTURE.md` is canonical for design questions.
- Code is canonical when this document and the implementation disagree — open a PR to update this doc.
