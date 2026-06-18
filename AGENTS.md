# AGENTS.md

Guidance for AI coding assistants (Cursor, Claude Code, Codex, Aider, Cline, etc.) working in this repository. Conventions are the same as for human contributors; this file just collects the load-bearing ones in one place.

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first for the system design. This file covers *how to work on the code*, not *what the code does*.

---

## What Unity is

Unity implements an AI assistant's brain as a **distributed back office**. A central `Actor` orchestrates specialized **state managers** (`ContactManager`, `KnowledgeManager`, `TaskScheduler`, `TranscriptManager`, `GuidanceManager`, `FunctionManager`, ...) through code-first plans. Every public manager method runs inside an **async LLM tool loop** and returns a **steerable handle** that supports `ask`, `interject`, `pause`, `resume`, `stop` — all the way down the nesting tree.

Sibling repos consumed via editable installs (see `[tool.uv.sources]` in `pyproject.toml`):
- **`unify`** — Python SDK wrapping the Orchestra REST API
- **`unillm`** — LLM client with caching, provider normalization, observability

The open agent runtime (`unity`, `unify`, `unillm`) talks to the **hosted Orchestra backend** (`ORCHESTRA_URL`, default `https://api.unify.ai/v0`). `orchestra` and `console` are private/hosted and are not part of the open-source repo set.

---

## Run the agent locally (public path)

The open-source runtime runs on your machine against the **hosted** Orchestra
backend. Provision a `UNIFY_KEY` and an assistant (`ASSISTANT_ID`) at
[console.unify.ai](https://console.unify.ai), then:

```bash
curl -fsSL https://raw.githubusercontent.com/unifyai/unity/staging/scripts/install.sh | bash
unity            # interactive local chat (alias: unity chat)
unity serve      # headless: ConversationManager + gateway
unity setup      # re-run the key/credential wizard
```

- `unity` is the CLI shim the installer drops in `~/.local/bin/`. From a
  checkout, the equivalents are `.venv/bin/python -m
  sandboxes.conversation_manager.sandbox` (chat) and `bash scripts/local.sh
  start --full` (headless).
- Configuration lives in `unity/.env`: `UNIFY_KEY`, `ASSISTANT_ID`,
  `ORCHESTRA_URL` (hosted), one LLM provider key, and optional voice/research
  keys (`scripts/prompt_byok_keys.sh`).
- No Docker, local Orchestra, or Console is involved in the public path. The
  onboarding flow, inbound channels, workspace connect, third-party app
  integrations, and screen-share are part of the hosted product.

**Internal full-local self-host stack.** The "all-repo fully local" stack
(local Orchestra + Console + Coordinator + gateway, via Docker Compose) is an
internal-only path and lives in the private **`unity-deploy`** repo under
`selfhost/` (`stack.sh`, `setup.sh`, `service.sh`, compose bundle). It drives
sibling `unity`/`console`/`orchestra` checkouts under `UNIFY_STACK_ROOT`.

---

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

Each Cursor agent (or terminal) gets an **isolated tmux server automatically**, so concurrent agents don't collide.

### When a test fails

1. **Never inspect tmux panes directly.** Read the corresponding log in `logs/pytest/<YYYY-MM-DDTHH-MM-SS_socket>/`.
2. **Use `Read` (not `cat`/`tail`)** — `logs/` is gitignored, so `Grep`/`Glob` won't find files there.
3. **Add temporary debug logs via `CURSOR_DEBUG_LOG`** — the only permitted logging mechanism for debugging. Grep for it (`rg CURSOR_DEBUG_LOG`) to find the project's util, then import and use it. Remove all calls before finalizing the fix.
4. **Clean up failed sessions** with `tests/kill_failed.sh` (or `tests/kill_server.sh` for everything).

### Pre-commit

```bash
.venv/bin/python -m pre_commit run --all-files
```

---

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

---

## Code style and philosophy

### Aggressive refactoring, zero backward compatibility

This is a rapidly evolving prototype. **Assume no backward compatibility** unless the user explicitly asks for it.

- **Break APIs freely.** Update all call sites in the same change. Do not introduce adapters, aliases, or optional parameters to soften the change.
- **Destructive over additive.** When requirements change, *rewrite* the affected code to support the new requirements optimally — don't "staple" new logic on top of old logic.
- **Delete aggressively.** If code is no longer the best way to do something, delete it. Don't comment it out. Don't keep it "just in case".
- **No defensive coding.** No `try/except` to "prevent crashes". No preemptive null checks. Fail loud and fast. Code should look like the happy path.

### No fast paths or heuristics

If a method needs to respond correctly to a class of user input, **always** address this by prompting the model and/or improving tool docstrings. Never apply regex-based or substring-based routing on user commands. The LLM is the router.

### No temporal or chat-specific comments

Comments must be **timeless** and describe the code as it currently exists.

- **No "new/updated/added" markers.** Code is "new" for a moment, then it's old. Git tracks novelty; comments rot.
- **No chat context.** No "per user request", "as discussed in this chat", "for the new requirement". The codebase must stand alone.
- **Explain *why*, not *what*.** Don't narrate what the code obviously does. Comment only on non-obvious intent, trade-offs, or constraints the code can't convey.

### No test info in production code

If a test is failing, never special-case production code to satisfy it. No hardcoded values matching test inputs. No conditional branches that only exist to pass a test. All fixes must be fully general and broader than the specific failing test.

---

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
| Domain facts, structured knowledge | `primitives.knowledge.*` |
| Durable tasks (create, execute) | `primitives.tasks.*` / `TaskScheduler` |
| Files (parse, query) | `primitives.files.*` |
| Web research (lightweight) | `primitives.web.*` |
| Secrets (metadata only via `ask`) | `primitives.secrets.*` |
| Procedural how-tos, SOPs | `GuidanceManager_*` (top-level JSON tools, not primitives) |
| Ephemeral live action | `Actor.act` (via ConversationManager) |
| Durable, tracked work | `TaskScheduler.execute` — never `update` to start work |

Full role descriptions are in `.cursor/rules/state-manager-roles.mdc`.

### Cross-manager images

Images flow between managers **by filesystem path**, not by `image_id`. Receiving managers resolve to persistent storage via `ImageManager.filter_images(filter="filepath == '...'")` when needed. Managers with first-class image fields (e.g. `GuidanceManager`) accept structured `ImageRefs` types at their own API boundary.

---

## Git safety

### Pull before editing

Run `git pull --rebase` once per repo per session before making file edits. Skip only if the user explicitly asks. After a push rejection + rebase, re-read any files you plan to edit (your in-memory copies are stale).

### Explicit-path commits (race-condition safety)

When multiple agents run in parallel, the shared git index creates race conditions:
- Agent A: `git add fileA`
- Agent B: `git add fileB`
- Agent A: `git commit -m "msg"` → **commits both fileA and fileB**

**Always pass explicit filenames to `git commit`:**

```bash
# Correct (modified file)
git commit myfile.json -m "Update myfile"

# Correct (new file)
git add myfile.json
git commit myfile.json -m "Add myfile"

# WRONG — uses shared index
git add myfile.json && git commit -m "Update myfile"
```

### Push only when explicitly asked

Never push without an explicit request from the user. Never force-push to `main` / `master`. Never use `git rebase -i` or `git add -i` (interactive flags don't work in non-interactive shells). Never edit `git config`.

### Worktree mode = direct commits

If running in a worktree, commit **directly to the current branch**. Do not create feature branches. Do not open PRs. The worktree itself is the isolation — adding branch overhead defeats the purpose.

---

## Git history for debugging

When direct code analysis stalls on a regression, ask the user for a known-good commit hash, then use the **aggregate diff**, not commit-by-commit:

```bash
git log --oneline <hash>..HEAD -- <path>
git diff <hash>..HEAD -- <path>
```

The aggregate diff is mathematically equivalent to composing serial diffs but far more token-efficient. Don't ask the user to paste diffs — ask for the hash and run the commands yourself.

---

## Repo map

```
unity/
├── unity/                   # Main package
│   ├── actor/               # CodeAct Actor, central orchestrator
│   ├── conversation_manager/ # Slow brain, live chat orchestration
│   ├── contact_manager/     # People + relationships
│   ├── knowledge_manager/   # Structured domain facts
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

---

## When in doubt

- Check `.cursor/rules/` for fuller context on any topic above.
- `ARCHITECTURE.md` is canonical for design questions.
- Code is canonical when this document and the implementation disagree — open a PR to update this doc.
