# Vision

Unify is the open runtime that powers our virtual teammates. We open-sourced
the runtime because what makes a virtual teammate worth using is the *shape*
of the orchestration layer — and that shape can only be evaluated, criticised,
extended, or forked if it's legible to the people considering using it.

This document records what Unify is making an architectural bet on, and the
things it is deliberately *not* trying to be. Both lists matter: most "why
isn't there a PR for X?" questions are explained by the second list.

---

## What Unify is

A back-office runtime for an AI assistant, designed around two ideas:

### A persistent reasoning loop sitting above the tool-caller

Most agent frameworks have a single loop: the model picks a tool, the tool
runs, the result feeds the next decision. Unify puts a second, *persistent*
loop above that — the `ConversationManager` — which stays present with the
user across every medium, keeps thinking while dispatched work is in flight,
and supervises the inner tool-calling loop (the `Actor`) rather than running
as that loop itself.

This is the same shape that [Thinking Machines'
interaction-models](https://thinkingmachines.ai/blog/interaction-models/)
post recently articulated; we arrived at it at the harness level. When
interaction-native models ship publicly, they would replace this split
end-to-end.

### Steerable handles, all the way down

Every public manager method returns the same type — a `SteerableToolHandle`
with `ask`, `interject`, `pause`, `resume`, `stop`. These handles nest: a
correction the user makes in chat propagates *down* through the dispatched
action, into whatever inner manager call is currently running. Mid-flight
steering is a first-class signal, not an opportunistic abort.

These two bets define everything else: the typed back office of state
managers (one tool loop per manager, each returning a steerable handle); the
CodeAct `Actor` that writes one Python plan per turn over typed
`primitives.*`; the dual-brain split that lets a real-time voice agent
coexist with a deliberate slow brain.

---

## What Unify is not trying to be

Listed here so contributors can route ideas appropriately, and so observers
can see the project's bet clearly.

### Not a channel-breadth product

[OpenClaw](https://github.com/openclaw/openclaw) is excellent at this: many
messaging platforms, a Gateway dispatcher tier that maps platform messages
to agent runs, a wide plugin marketplace. Unify's gateway supports a
smaller set of channels by design (chat, voice, video, SMS, email, phone)
and the channel layer is intentionally thin because the project's
investment is upstream — in the slow-brain / Actor / back-office tier
above it.

If you want a personal-assistant **product** with broad channel coverage and
a thriving plugin ecosystem, OpenClaw is the project we'd recommend.

### Not a single monolithic agent loop

[Hermes Agent](https://github.com/NousResearch/hermes-agent) does this very
well: a single ~12k-LOC `AIAgent` core with text-injection-based steering,
a polished skills library, mature cron + webhook automation. It's the
right shape if you want maximum legibility of one agent loop.

Unify makes the opposite bet: a back office of *many* tool loops, each
responsible for one slice of persistent state, each returning a steerable
handle. The cost is more moving parts; the win is that the structure
*itself* is what makes interruption-mid-task and mid-flight steering work
at every depth of the call stack.

If you want a polished single-loop agent product, Hermes Agent is the
project we'd recommend.

### Not a coding agent

Unify's `Actor` writes Python plans over typed `primitives.*` to *act on
the world* — search, communicate, schedule, remember. It is not built for
"edit my source tree, run my tests, ship the diff." There are excellent
projects for that, and the CodeAct technique itself is well-suited to both
— but Unify's primitives surface is shaped around assistant tasks, not
codebase tasks.

### Not regex-routed

Production code does not look at a user message and decide what to do
based on substring detection. If the system handles something wrong, the
fix is always to improve a prompt, a tool docstring, or a manager's
public API — never to add a heuristic shortcut. This is a hard rule; PRs
that pattern-match on user input get sent back.

### Not configured via cron and webhook YAML

Recurring schedules and event triggers are described to the agent in
natural language and stored as `Task` rows; the in-process timer wheel
fires them through the same `Actor` that handles live work. There is no
separate cron daemon, no `triggers.yml`, no webhook configuration file.
Inbound-event triggers (*"ping me whenever Alice emails about invoices"*)
are matched on the comms event stream by the same machinery.

### Not backward-compatible by default

Unify is a rapidly-evolving prototype. We break APIs freely and update
all call sites in the same change. This will probably soften when there
are downstream forks worth not breaking; today, it doesn't.

### Not committed to its current LLM-client / Python-SDK / backend split

Unify is the *cognitive core* — the brain. It currently depends on two
sibling repos (`unisdk` for storage access, `unillm` for LLM inference)
and the hosted Orchestra persistence backend. Those splits exist to keep
concerns separate, not because the boundaries are sacred. If a
better-shaped open-source LLM client or persistence layer arrives, Unify
should adopt it.

---

## What's open, what's not

The local install is the full local runtime. The runtime itself, the LLM
client (`unillm`), and the Python SDK (`unisdk`) are MIT-licensed and on
GitHub. The persistence backend (Orchestra) is a hosted service the
runtime talks to over `ORCHESTRA_URL`; it is not open source.

The hosted product at [console.unify.ai](https://console.unify.ai) wraps
Unify in a commercial UI: multi-tenant identity, hosted telephony, channel
integrations, organisations, billing, deployment management, observability
tiles.

The `unify.deploy_runtime` Service Provider Interface is the boundary
between the open runtime and the hosted scaffolding. Local installs use
no-op implementations of every hook; the hosted product supplies its own.
Forks of Unify can supply their own too — Kubernetes, Nomad, a custom
orchestrator, whatever fits.

---

## How this document evolves

This is a "where the project is aiming" document, not a roadmap. Roadmap-
shaped changes (what's shipping next) go in [`CHANGELOG.md`](CHANGELOG.md).
Architectural choices (how the system is structured) live in
[`ARCHITECTURE.md`](ARCHITECTURE.md). This file is for *the bet itself* —
what Unify is and isn't trying to be — and changes only when one of those
bets visibly changes.
