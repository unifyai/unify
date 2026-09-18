---
title: "Execution is a coordinate, not a call"
description: "Most agent frameworks give the model one way to run code. Unify makes every execution a point in a space — three state modes, named sessions, one persistent workspace environment — and lets the model pick."
status: draft
---

# Execution is a coordinate, not a call

Our agent writes Python to get things done. That part isn't novel — plenty of systems do code-as-actions now, and the argument for it is well made elsewhere. What I want to write about is the thing underneath: *where* that code runs. Most frameworks answer with a single tool that evaluates a string and hands back stdout. That works right up until the model tries to do a day's work with it.

Watch an agent under a one-shot executor for a while and you see the same shape of failure. It reads a 2 GB export into a dataframe, prints a summary, and the sandbox closes. Next step, it needs a different cut of the same data, so it reads the file again. And again. Ten minutes of wall time and a lot of tokens go into reconstructing state that existed and was thrown away, because the executor had no concept of before and after.

The obvious fix is a persistent interpreter, and it swaps one problem for a worse one. Now everything shares a namespace forever: a variable set during a half-abandoned experiment shadows something later, an import at step three changes behaviour at step thirty, and a task that should be independent inherits whatever the last one left lying around. You've traded amnesia for contamination.

## Three modes, not two

Both of those are the right answer sometimes, which is the actual insight. So `execute_code` takes a `state_mode`, and the model chooses it per call. `stateful` runs in a session where variables and imports survive — for a local Python cell it is the default, targeting the sandbox that lives and dies with the surrounding act. `stateless` is a fresh sandbox, closed the moment it returns.

The third one is the interesting one. `read_only` takes an existing session, copies its globals into a throwaway sandbox, runs there, and discards everything the code wrote. You can look at what's in a session and compute against it, and nothing you do can damage it.

I didn't expect that to earn its place and it turns out to be the mode I'd least want to remove. It's what makes an expensive session safe to explore. Something has spent four minutes building a dataframe; the model now wants to try a reshape it's only about 70% sure about. Under stateful, a bad guess corrupts the thing it took four minutes to build, and the recovery path is to rebuild it. Under `read_only`, it tries the reshape, reads the result, and the original is untouched because it was never reachable. What-if without consequences.

## The other axis: which session

A mode isn't enough on its own, because "stateful" only means something relative to a particular pile of state. Every cell is Python — a command-line tool is run from Python via `subprocess`, so its output arrives as data the same cell can work on, rather than as a separate language with its own state to track — and every cell runs in-process, in the same interpreter as the runtime. What varies is which session's globals it sees.

So an execution isn't a call, it's a point: which state mode, which session. The model names the coordinates it wants and the runtime resolves them. Sessions are keyed by an integer id, and a stateless call has none.

## Sessions you can name and look inside

Sessions get integer ids, but the model is nudged toward names, because `session_name="audit"` survives being reasoned about across twenty intervening steps in a way that `session_id=3` does not. There's a registry mapping names to the underlying key, a cap of twenty live sessions per actor, and two tools — `list_sessions()` and `inspect_state()` — so the model can ask what exists and what's in it rather than guessing. A surprising amount of good behaviour comes from just letting it look.

There's one special case worth knowing about, because it caused us some confusion before we named it properly. Every `act()` call already has its own Python sandbox — that's the thing running the plan. Python session 0 is bound to that sandbox rather than being a separate entry in the pool. So the default stateful session is the plan's own namespace, and anything with a higher id is a deliberately separate world. Two layers of isolation: one per task, one per session within it.

## One environment, and what a function remembers about it

Packages are the coordinate we deliberately took away. There is one workspace environment — a single `uv`-built venv under the assistant's home — and everything runs in-process against it. `install_python_packages` puts a package there and it stays: the next task, and the next session, can import it without asking. Nothing is ever removed.

A stored function doesn't get its own environment; it gets a list. When the storage review keeps a function that imports something third-party, it records the pip specifiers that installed it as the function's `dependencies`, and the store refuses a function whose imports come without them. Before the function runs — from a fresh machine, or after a home directory was wiped — the runtime checks each specifier against what's installed and installs only what's missing, then runs the function exactly as it would run one that imports nothing. The function carries the fact of its dependency; the environment carries the package.

The trade is explicit. Two functions that want incompatible versions of the same library cannot both be right in one environment, and we don't pretend otherwise — the later install wins, and the runtime's own dependencies always win over both. What we get in return is that a package installed for one task is a package the next task has, that every cell and every stored function shares a single set of packages the model can reason about, and that there is no process boundary for a `primitives.*` call to be marshalled across. Solving the general version-conflict problem is not our problem to solve; making the common case — "I need pandas" — cost one install, ever, is.

## Where the constraint is real

Remote surfaces — the assistant's own VM, or a machine the user has linked — are stateless one-shots. No sessions. That looks like an omission and isn't: a persistent session implies a process the runtime is confident is still alive and still yours, and that guarantee is much harder to make honestly across a network boundary and a machine somebody might close the lid on. Rather than offer a session that silently evaporates, the tool rejects the combination with an error that says which arguments to drop.

## The unglamorous half

Everything above is the design. What makes it survive contact with a language model is a handful of much smaller decisions.

Generated code assigns to obvious names, and one of the most obvious is `primitives` — the object every managed capability hangs off. A model that writes `primitives = get_primitives()` at the top of a stateful session has just broken every subsequent call in that session, and it'll be baffling to debug because the code that broke it ran fine. Rather than ask the prompt to please not do that, an AST pass rewrites assignments to that name into a harmless local. Reads pass through untouched. The model can write whatever it likes and the injected globals survive.

Argument validation returns structured errors rather than raising — a dict with a message and a `suggestion` field naming the arguments to change. Asking for `read_only` without a session, or a session on a remote surface, comes back as something the model can read and immediately correct on the next call. An exception is a much worse conversation to have with a model than a sentence explaining what to pass instead.

Python execution keeps REPL semantics: a trailing expression is rewritten into a return, so the last line of a block is the result without anyone remembering to write `return`.

## Why one tool

The alternative to all this is three tools: `run_python`, `run_python_stateful`, `run_remote`. It's a reasonable instinct and we went the other way, because a tool surface that grows with the cross-product of your options is a surface nobody can hold in their head — model included. Every new capability multiplies rather than adds, the docstrings drift apart, and the model ends up choosing between tools whose differences it has to infer from their names.

One tool with orthogonal arguments asks a different question. Not "which of my executors is this?" but "do I need this to persist? which state should it see? whose machine?" Those are properties of the work, and a model reasons about properties considerably better than it picks from a menu.

I think this is the general lesson from the whole exercise. Handing a model a REPL is an afternoon. Giving it a space it can navigate — where the axes are independent, the combinations mean what you'd expect, and the illegal ones fail with an explanation — is most of the actual work, and it's the part that decides whether the thing can do a day's work or just a demo.

## Where to look

All open at [github.com/unifyai/unify](https://github.com/unifyai/unify):

- The `execute_code` tool, its docstring, and the session registry: [`unify/actor/code_act_actor.py`](https://github.com/unifyai/unify/blob/main/unify/actor/code_act_actor.py)
- Mode resolution, validation errors, and the shadowing guard: [`unify/actor/execution/session.py`](https://github.com/unifyai/unify/blob/main/unify/actor/execution/session.py)
- The workspace environment and dependency installs: [`unify/environment.py`](https://github.com/unifyai/unify/blob/main/unify/environment.py)
- Stored functions and their recorded dependencies: [`unify/function_manager/function_manager.py`](https://github.com/unifyai/unify/blob/main/unify/function_manager/function_manager.py)
- Execution surfaces: [`unify/actor/execution/surface.py`](https://github.com/unifyai/unify/blob/main/unify/actor/execution/surface.py)
