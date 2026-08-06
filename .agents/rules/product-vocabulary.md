# Product Vocabulary: One Noun Per Concept

The assistant reasons in the words we hand it. When two concepts share a
word, the model has to guess which one a prompt means, and the guess is
invisible until it routes to the wrong manager.

This rule exists because we already paid for that once. `Workflow` shipped
as a first-class type in August 2026 while "workflow" was load-bearing
prose in ~180 places across the actor and task prompts, meaning *multi-step
procedure* and *recurring task*. The design session that proposed it had
flagged the collision and concluded "give the new thing an unused name" —
then the code shipped anyway and the rename came two days later, across 39
files, invalidating LLM caches for the whole actor suite. The rename was
correct; the sequencing was the mistake.

## The canonical nouns

| Concept | Word | Owner |
|---|---|---|
| Installable package that sets the assistant up for a recurring job | **workflow** | `WorkflowManager` |
| Durable unit of scheduled or triggered work | **task** | `TaskScheduler` |
| One run of a task | **execution** | `Tasks/Executions` |
| Written-down multi-step how-to | **procedure** | `GuidanceManager` |
| Executable unit the assistant calls | **function** | `FunctionManager` |
| Durable sourced statement about the world | **claim** | `KnowledgeManager` |
| What the actor writes and runs to satisfy one request | **plan** | `Actor` |
| Ordered instructions inside a docstring | **Steps** (section header) | — |

Rule of thumb: **"workflow" is a noun you install. Everything that used to
borrow the word is a procedure, a task, or a plan.**

Do not write "workflow" to mean a multi-step anything. Do not write
"recurring workflow" — that is a recurring task. Do not title a docstring
section `### Workflow` — it now reads as a type reference; use `Steps` or
`Procedure`.

## The three stores, and how to tell them apart

`FunctionManager`, `GuidanceManager` and `KnowledgeManager` are the three
places durable know-how lives, and they are distinguished by *what kind of
thing they hold*, not by topic:

| | FunctionManager | GuidanceManager | KnowledgeManager |
|---|---|---|---|
| Role | the **what** | the **how** | the **is** |
| Holds | one callable | a multi-step procedure | one typed claim |
| Content | executable implementation | natural-language recipe | sourced statement |
| Analogy | a tool's docstring | a prompt that references tools | a fact with provenance |

Deciding where something belongs:

- **Can it run?** Function. If it is code that executed successfully and
  would be worth calling again, it is a function — not a procedure
  describing the code.
- **Does it tell someone how to act?** Procedure (guidance). Reach for it
  when composing several functions is non-obvious, or when a durable rule
  or policy governs how work is done. A procedure links the functions it
  composes via `function_ids`, which is also how a rule change finds every
  implementation that embeds it.
- **Is it true regardless of how you act on it?** Claim (knowledge).
  Facts, policies, definitions, decisions, constraints, preferences —
  carrying `source_refs` when provenance is known.

The common error is storing a procedure that merely restates one
function's docstring. If a single function's docstring already explains
its inputs, behaviour and use, store the function and stop.

Negative scope matters as much: people belong in `ContactManager`,
credentials in `SecretManager`, file bytes in `FileManager`. None of the
three stores is a dumping ground for "stuff we learned".

## Before naming a new first-class type

1. **Grep first.** `rg -ic "<candidate>" unify/ tests/`. If the word
   appears in prompts, docstrings or examples meaning something else, you
   have a collision.
2. **Prefer an unused word** over renaming existing prose. An unused word
   is free; a rename costs a sweep plus fresh LLM inference everywhere a
   prompt changed.
3. **If you keep the colliding word, do the cleanup in the same change.**
   Not "later" — the bill compounds with every prompt written against the
   ambiguous meaning, and a half-renamed codebase teaches the model both
   meanings at once.
4. **Watch for the near-synonym trap.** A new noun must not collide
   *conceptually* either. "Recipe" and "playbook" are both unused as
   types, but the three-stores table above calls guidance a
   "natural-language recipe" and a playbook *is* a procedure — either one
   would have recreated the same problem one table over.

## What gets renamed, and what does not

Rename **our** vocabulary: prompts, docstrings, tool labels, example
titles, section headers, internal identifiers, test names and fixtures.

Leave alone:

- **Simulated user speech in eval tests.** Users really do say "workflow"
  loosely about business processes. Scrubbing it removes exactly the
  ambiguity the assistant must survive, and re-runs paid inference for
  negative value.
- **Third-party product names.** HubSpot and Salesforce ship features
  called Workflows; renaming makes our integration docs factually wrong.
- **GitHub Actions terminology.** `.github/workflows/`,
  `workflow_dispatch`, `workflow_run` and `workflow_call` are API
  keywords. Renaming breaks CI.
- **The shared `global-agent-rules` submodule.** Edit it in its own repo,
  never here.

After editing anything under `.agents/`, regenerate the aggregate:
`python3 .agents/global-rules/build_agents_md.py`. Never hand-edit
`AGENTS.md`.

## Known overloads, accepted

- **`surface`** means two things: in workflows, a manager a bundle plants
  content into (`SCOPED_SURFACES`, `Surface.sync`); in canvas, a place a
  view renders (chat embed, assistants tab, standalone). Both readings are
  established and neither is user-facing, so they coexist. If this ever
  causes a real mistake, the workflow sense is the one to rename (to
  `target`), because the canvas sense is shared with Console.

## Console (`unifyai/console`) mapping

Console is the user-facing surface and does not have to mirror internal
names, but the mapping must be **deliberate and consistent** — the same
rows must not carry two different labels in two places.

| unify | Console user-facing |
|---|---|
| `guidance` rows / procedures | **Procedures** |
| `knowledge` claims | **Knowledge** |
| `functions` | **Functions** |
| `tasks` | **Tasks** |
| workflow bundle | **Workflow** |

When adding a Console surface that renders manager content, check the
label against this table rather than the unify context name.
