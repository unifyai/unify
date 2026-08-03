# Plan: steering out-of-process execution

**Status:** planned, not started. In-process steering is shipped and tested.

Live steering — correcting a block of code while it is still running — works for
every in-process Python path. This plan extends it to the three paths that run
somewhere else: venv-backed functions, shell scripts, and non-local execution
surfaces.

Read `unify/function_manager/steering.py` first. This plan assumes its four
parts (probes, idempotency cache, retry via source splice, bounded lifetime)
and reuses them rather than replacing them.

---

## Where things stand

**Covered today.** Every in-process Python path, whichever sandbox it runs in:

- `PythonExecutionSession.execute` — all state modes, including the `stateless`
  default that `execute_code` uses
- `FunctionManager._execute_in_default_env` — the route that skips the sandbox
  entirely and runs a stored function directly

Within those: loops, `if` and `try` arms at any depth; awaits nested inside
expressions (comprehensions, `gather`); nested primitive namespaces such as
`primitives.integrations.slack.send_message`; stored function bodies, because
`execute_function` synthesises the implementation as a preamble.

**Not covered.** Three paths, all of which run the user's code in another
process or on another machine:

| Path | Entry | Runs where |
|---|---|---|
| Venv-backed Python | `venv_id` set on `execute_code` / a venv-backed stored function | subprocess via `VenvPool` |
| Shell | `language` in `bash`/`zsh`/`sh`/`powershell` | subprocess via `ShellPool` |
| Non-local surface | `surface != "local"` on `execute_code` | assistant desktop or user desktop, via `unify/actor/execution/targets` |

---

## The key finding: the channel already exists

The obvious reading of this problem is "we need a bidirectional event stream to
and from the child process". We already have one.

`unify/function_manager/venv_runner.py` documents the protocol in its module
docstring:

```
{"type": "rpc_call",   "id": str, "path": str, "kwargs": dict}   # child → parent
{"type": "rpc_result", "id": str, "result": Any}                 # parent → child
{"type": "rpc_error",  "id": str, "error": str}                  # parent → child
```

Every `primitives.*` call made inside a venv or shell child round-trips to the
parent and **blocks the child until the parent replies**. `shell_runner.py`
speaks the same shape.

Two things follow, and they are the whole reason this is tractable:

1. **The parent already sees `path` and `kwargs` for every dispatch.** Those are
   exactly the inputs to the cache key `(tool, serialized_args, occurrence)`.
   The cache needs no new information and no child cooperation.
2. **The child is already suspended at every dispatch.** A checkpoint that only
   needs to fire at side effects therefore needs no new mechanism — just a new
   thing the parent is allowed to say in reply.

So the work is not building a channel. It is widening a reply, and moving the
existing parent-side session to sit across it.

### Two handlers, not one

There are **two** implementations of `_handle_rpc_call`, and both need the
change:

- `FunctionManager._handle_rpc_call` — used by `execute_in_venv` and
  `execute_shell_script`
- `_VenvConnection._handle_rpc_call` — used by the pooled persistent-venv path
  (`_VenvConnection.execute` → nested `handle_rpc_loop`)

Missing the second would leave pooled venv sessions silently unsteerable, which
is the same class of bug as binding the session to the wrong sandbox object was
in-process: it looks wired and does nothing.

---

## Phase 1 — parent-side cache and interrupt

One change, no child changes, and it benefits venv, shell and remote at once
because all three converge on the RPC handler.

**Cache.** In `_handle_rpc_call`, derive the key from `(path, kwargs)` and the
active `SteeringSession`. On a hit, return the memoised result without
dispatching. On a miss, dispatch and memoise. Replay then works out-of-process
with no child involvement.

**Interrupt.** Add one message type:

```
{"type": "rpc_interrupt", "id": str, "reason": str}   # parent → child
```

The child raises `ControlledInterruption` at that call site instead of returning
a result. Since the child is already blocked on the reply, this is the `_int`
probe realised without instrumentation.

**Pause.** The parent delays the reply. Nothing to build.

**Retry.** The parent splices the patch and re-sends `execute` with the new
source. The re-run's `rpc_call`s hit the parent cache and return memoised
results without re-dispatching. This is *cleaner* than in-process, where the
cache and the code live on the same side and the ownership has to be reasoned
about; here the parent unambiguously owns it.

### What Phase 1 gets you

Correction at every side effect that goes through primitives, plus replay, on
all three out-of-process paths. That is most of what matters, because the reason
to steer is to stop a side effect and side effects are overwhelmingly dispatches.

### What Phase 1 does not get you

A loop in the child that does no primitive call is invisible to the parent. Two
sub-cases, and they are not equally important:

- **Pure computation** — nothing to correct. Not a real loss.
- **A direct third-party call**, e.g. `requests.post` inside the venv. A real
  side effect the parent never sees. In-process, statement and loop probes catch
  this; here nothing does.

---

## Phase 2 — instrumented child source (venv only)

Full parity, and the only part with a performance question.

The parent holds the source before it ships it, so the existing AST pass can run
parent-side and the child receives already-instrumented code. `_cp`, `_int` and
`_around_cp` become shims in the child that consult the parent.

**The cost is the problem.** In-process a checkpoint is ~10 µs (measured). A
round trip over a pipe is 0.1–1 ms. A per-iteration checkpoint on a
1000-iteration loop turns ~10 ms of overhead into up to a second of pure IPC.
Naively making every checkpoint an RPC is not viable.

**The fix is a second channel.** Keep the existing RPC channel
request/response, and add a one-way parent→child control channel that the child
reads **non-blockingly** at each checkpoint:

- idle cost ≈ a non-blocking pipe read, on the order of microseconds
- a pending correction is a byte on that channel; the child then raises

This is what makes per-iteration probes affordable out-of-process, and it is the
piece to design carefully rather than the AST work, which is already written.

---

## What cannot reach parity, and why

State these as boundaries rather than as work items:

**Shell** has no AST and no Python, so statement-level probes are not possible
by any design. It tops out at dispatch-boundary steering (Phase 1), which it
gets for free.

**Non-local surfaces** are dispatch-boundary in practice regardless of design:
at 10–100 ms per network hop, per-iteration checkpoints are not viable. Phase 1
applies; Phase 2 does not.

**Synchronous blocking code** remains opaque, exactly as in-process. A blocking
call holds the loop for its duration, and during that window a correction cannot
arrive. Out-of-process this is arguably *better*, since the parent is a separate
process and stays responsive — but the child still cannot observe anything until
it yields.

**Retraction** is still impossible. Replay records that a side effect happened;
it cannot undo one. A patch redirects work that has not happened yet. This is
unchanged by anything in this plan and should not be presented as a gap it
closes.

---

## Open questions to resolve before starting

1. **Does `surface != "local"` use this RPC shape at all?** `_execute_on_surface`
   routes through `unify/actor/execution/targets` and `ExecutionSurface`, which
   has not been traced. If it uses a different transport, Phase 1 needs a third
   integration point rather than riding on `_handle_rpc_call`.
2. **Child-local state across a retry.** For `stateful` venv sessions, attempt 2
   runs in a namespace still holding residue from attempt 1. In-process has the
   same property, so this is not new — but out-of-process it is worth deciding
   explicitly whether the retry reuses the child or replaces it.
3. **Where the session lives for a pooled venv.** In-process the session travels
   by contextvar and whichever sandbox runs binds it. The pooled-venv equivalent
   needs the same "follows the call, not the connection" property, or a
   long-lived `_VenvConnection` will outlive the session that bound it.

---

## Suggested order

1. Trace open question 1, so the scope of Phase 1 is known.
2. Phase 1 in `FunctionManager._handle_rpc_call`, with tests on the venv path.
3. Phase 1 in `_VenvConnection._handle_rpc_call`, with a test that specifically
   covers a *pooled* session, since that is the one that silently no-ops if
   missed.
4. Shell coverage — same parent change, so mostly a test exercise confirming a
   shell script's primitive calls are cached and interruptible.
5. Phase 2 only if a real workload needs correction inside a non-dispatching
   loop. Design the control channel first; the AST pass already exists.

Phase 1 is worth doing on its own merits. If Phase 2 never happens,
out-of-process execution is still steerable at every primitive side effect,
which is the case the mechanism exists for.
