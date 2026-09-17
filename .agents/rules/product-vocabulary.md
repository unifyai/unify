# Product Vocabulary: One Noun Per Concept

The assistant reasons in the words we hand it. When two concepts share a
word, the model has to guess which one a prompt means, and the guess is
invisible until it routes to the wrong manager.

## The canonical nouns

| Concept | Word | Owner |
|---|---|---|
| Written-down multi-step how-to | **procedure** | `GuidanceManager` |
| Executable unit the assistant calls | **function** | `FunctionManager` |
| Durable sourced statement about the world | **claim** | `KnowledgeManager` |
| What the actor writes and runs to satisfy one request | **plan** | `Actor` |
| One dispatched, steerable piece of work | **action** | `ConversationManager` |
| Ordered instructions inside a docstring | **Steps** (section header) | — |

Do not write "workflow" to mean a multi-step anything: it is a procedure,
an action, or a plan. Do not title a docstring section `### Workflow`; use
`Steps` or `Procedure`.

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

**`skill` is an umbrella, not a synonym.** "Skills" means *anything worth
storing across the three stores* — the `store_skills` tool, the
`"Storing reusable skills"` review label. That is a legitimate superset
covering functions, procedures and claims together. It is wrong only when
used for one specific member.

## Before naming a new first-class type

1. **Grep first.** `rg -ic "<candidate>" unify/ tests/`. If the word
   appears in prompts, docstrings or examples meaning something else, you
   have a collision.
2. **Prefer an unused word** over renaming existing prose. An unused word
   is free; a rename costs a sweep plus fresh LLM inference everywhere a
   prompt changed.
3. **If you keep the colliding word, do the cleanup in the same change.**
   A half-renamed codebase teaches the model both meanings at once.
4. **Watch for the near-synonym trap.** A new noun must not collide
   *conceptually* either: "recipe" and "playbook" are both unused as
   types, but the three-stores table calls guidance a "natural-language
   recipe" and a playbook *is* a procedure.

## What gets renamed, and what does not

Rename **our** vocabulary: prompts, docstrings, tool labels, example
titles, section headers, internal identifiers, test names and fixtures.

Leave alone:

- **Simulated user speech in eval tests.** Users really do say "workflow"
  loosely about business processes. Scrubbing it removes exactly the
  ambiguity the assistant must survive.
- **Third-party product names.**
- **The shared `global-agent-rules` submodule.** Edit it in its own repo,
  never here.

After editing anything under `.agents/`, regenerate the aggregate:
`python3 .agents/global-rules/build_agents_md.py`. Never hand-edit
`AGENTS.md`.
