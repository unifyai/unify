# Contributing to Droid

## Getting started

Droid depends on two sibling repositories. Clone all three as siblings:

```bash
git clone https://github.com/unifyai/droid.git
git clone https://github.com/unifyai/unify.git
git clone https://github.com/unifyai/unillm.git

cd droid
pip install uv && uv sync --all-groups
```

This installs Droid and its sibling dependencies (linked via `[tool.uv.sources]` in `pyproject.toml`).

## Running tests

Tests use real LLM calls with cached responses. After the first run, cached responses replay instantly.

```bash
source .venv/bin/activate

# Run all tests
tests/parallel_run.sh tests/

# Run a specific module
tests/parallel_run.sh tests/contact_manager/

# Run a specific test
tests/parallel_run.sh tests/contact_manager/test_ask.py::test_name
```

Tests require a running backend (Orchestra). The test runner starts a local instance automatically via Docker (requires Docker Desktop). See [tests/README.md](tests/README.md) for the full testing philosophy.

## Code style

We use `black` for formatting and `autoflake` for unused import removal. Pre-commit hooks run automatically on commit:

```bash
.venv/bin/python -m pre_commit run --all-files
```

## CI on forks

The full test suite requires org-level secrets (API keys, backend access). Fork PRs run lint checks only. A maintainer will trigger the full test suite on your PR after review.

## Pull requests

- Open PRs against the `staging` branch
- Keep PRs focused — one concern per PR
- Tests should pass locally before opening a PR
- We don't require backward compatibility (see the project's aggressive refactoring philosophy)

## Design principles

- **No regex routing.** If the system handles something wrong, fix the prompt or tool docstring, not a hardcoded rule.
- **No defensive coding.** Don't wrap things in try/except unless you're handling a specific, recoverable error. Fail loud.
- **English as API.** Managers communicate through natural-language interfaces. The Actor orchestrates through English-language primitives.
- **Real LLMs in tests.** We never mock the LLM client. Responses are cached for speed, not faked.

See [`VISION.md`](VISION.md) for the bets the project is making — including the things it deliberately *isn't* trying to be. Most "why isn't there a PR for X?" questions are explained by that document.

## Maintainers

Droid is maintained by [Unify](https://unify.ai). The current maintainer team (in commit-count order, deduplicated via [`.mailmap`](.mailmap)):

- **Daniel Lenton** ([@djl11](https://github.com/djl11)) — project lead
- **Yusha Arif** ([@YushaArif99](https://github.com/YushaArif99))
- **Ved Patwardhan** ([@vedpatwardhan](https://github.com/vedpatwardhan))
- **JG** ([@juliagsy](https://github.com/juliagsy))
- **Haris Mahmood** ([@hmahmood24](https://github.com/hmahmood24))
- **Mostafa Hany** ([@CatB1t](https://github.com/CatB1t))
- **Yasser** ([@Infrared1029](https://github.com/Infrared1029))
- **Nassim Berrada** ([@nassimberrada](https://github.com/nassimberrada))

### Area familiarity

The repository's [`.github/CODEOWNERS`](.github/CODEOWNERS) is the canonical routing file — anything not matched by a specific rule requires `@unifyai/Engineers` review.

For PRs that touch a specific subsystem, the table below is a rough guide to who has the deepest familiarity (derived from commit history; team members rotate and overlap). You don't need to tag a reviewer manually — opening a PR is enough, we'll route. The list is a hint for when a fast review matters.

| Area | Reviewers (rough) |
|---|---|
| `droid/actor/` (CodeAct Actor) | @YushaArif99, @djl11 |
| `droid/conversation_manager/` (slow + fast brain) | @djl11, @vedpatwardhan, @juliagsy |
| `droid/task_scheduler/` | @djl11 |
| `droid/contact_manager/`, `droid/knowledge_manager/`, `droid/transcript_manager/` | @djl11 |
| `droid/file_manager/` (parsing) | @hmahmood24, @djl11 |
| `droid/function_manager/`, `droid/web_searcher/` | @djl11, @YushaArif99, @juliagsy |
| `droid/secret_manager/` | @djl11 (high-blast-radius — see CODEOWNERS) |
| `droid/gateway/`, `droid/comms/` | @djl11 |
| `agent-service/` (TypeScript browser-use service) | @juliagsy, @YushaArif99 |
| `scripts/install.sh`, `scripts/local.sh`, `scripts/voice.sh` | @djl11 |
| `tests/conftest.py`, `tests/parallel_run.sh` | @djl11, @CatB1t |

## Questions?

- **Architectural questions** — [GitHub Discussions](https://github.com/unifyai/droid/discussions)
- **Quick questions / chat** — [Discord](https://discord.com/invite/sXyFF8tDtm)
- **Security** — see [`SECURITY.md`](SECURITY.md); do not open public issues for security vulnerabilities.
