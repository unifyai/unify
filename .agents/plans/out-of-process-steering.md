# Plan: steering out-of-process execution

**Status:** Phase 1 built and tested — venv (one-shot and pooled) and shell.
Phase 2 (instrumented child source) not started, deliberately. Non-local
surfaces turned out not to share the RPC transport at all (see below) and are
out of scope for this mechanism.

Live steering — correcting a block of code while it is still running — works
for every in-process Python path and, at the dispatch boundary, for code
running in venv subprocesses and shell scripts. This document records how, and
what was learned closing the plan's open questions.

Read `unify/function_manager/steering.py` first. Its four parts (probes,
idempotency cache, retry via source splice, bounded lifetime) are reused
out-of-process rather than replaced; the out-of-process entry point is
`dispatch_with_steering` in the same module.

## Read this before deciding how much more to build

The work split into two phases with very different costs, and **Phase 1 is
worth having on its own**. It is not groundwork for Phase 2; it is the whole
feature for most cases.

Phase 1 makes out-of-process execution correctable at every side effect that
goes through primitives, and that is the case the mechanism exists for — the
reason to steer is to stop a side effect before it happens, and side effects
are overwhelmingly primitive dispatches. It needed no changes to how the child
executes code, and it covers venv and shell at once.

Phase 2 closes one specific remaining gap: a loop in the child that does no
primitive call, and so is invisible to the parent. It is venv-only, it is the
only part with a performance question, and it should be built when a real
workload needs it rather than for completeness.

If Phase 2 never happens, this feature is not half-finished.

---

## Where things stand

**Covered.** Every in-process Python path (statement-level, via AST probes):

- `PythonExecutionSession.execute` — all state modes, including the `stateless`
  default that `execute_code` uses
- `FunctionManager._execute_in_default_env` — the route that skips the sandbox
  entirely and runs a stored function directly

And every out-of-process path that round-trips primitives over JSON-RPC
(dispatch-boundary, no instrumentation):

- One-shot venv subprocesses — `FunctionManager.execute_in_venv`
- Pooled persistent venv sessions — `_VenvConnection.execute`
- Shell scripts using the `unity-primitive` bridge —
  `FunctionManager.execute_shell_script` (cache and pause only; see the
  boundaries section for why interrupts cannot fire on shell)

**Not covered.** Non-local surfaces (`surface != "local"`), and any child-side
work between dispatches (Phase 2).

---

## The key finding: the channel already existed

The obvious reading of this problem was "we need a bidirectional event stream
to and from the child process". We already had one.

`unify/function_manager/venv_runner.py` documents the protocol in its module
docstring:

```
{"type": "rpc_call",      "id": str, "path": str, "kwargs": dict}   # child → parent
{"type": "rpc_result",    "id": str, "result": Any}                 # parent → child
{"type": "rpc_error",     "id": str, "error": str}                  # parent → child
{"type": "rpc_interrupt", "id": str, "reason": str}                 # parent → child
```

Every `primitives.*` call made inside a venv or shell child round-trips to the
parent and **blocks the child until the parent replies**. Two things follow,
and they are the whole reason this was tractable:

1. **The parent sees `path` and `kwargs` for every dispatch.** Those are
   exactly the inputs to the cache key `(tool, serialized_args, occurrence)` —
   and `path` is the same string in-process memoisation records, so entries
   mean the same thing on both sides of the process boundary.
2. **The child is suspended at every dispatch.** A checkpoint that fires at
   side effects needed no new mechanism — just a new thing the parent is
   allowed to say in reply (`rpc_interrupt`, the one message type added).

So the work was not building a channel. It was widening a reply.

### How Phase 1 is wired

One checkpoint, one convergence point, three loops that translate:

- `dispatch_with_steering` (`steering.py`) is the parent-side checkpoint: it
  honours pause, collects interjections, raises `ControlledInterruption` when
  a pending correction targets the running block, replays memoised dispatches,
  and memoises fresh ones.
- `FunctionManager._handle_rpc_call` routes **every** RPC through that
  checkpoint before resolving the path (`_dispatch_rpc_path`). The plan's
  "two handlers, not one" risk — the pooled `_VenvConnection._handle_rpc_call`
  looking wired while doing nothing — was resolved structurally: it now
  delegates to `FunctionManager._handle_rpc_call` instead of duplicating it,
  so the pooled path cannot drift.
- The two venv execute loops (one-shot `execute_in_venv`, pooled
  `_VenvConnection.execute`) catch `ControlledInterruption` at the rpc_call
  site, reply `rpc_interrupt` so the blocked child unwinds, discard the
  child's completion, and re-raise into `run_with_steering` — which splices
  the patch and re-sends `execute` with the new source. A one-shot retry is a
  fresh subprocess; a pooled retry reuses the connection. Both replay the
  completed prefix from the parent's cache.
- The child (`venv_runner.py`) raises its own standalone
  `ControlledInterruption` at the blocked call site when the reply is an
  `rpc_interrupt`. That is the entire child-side change; the runner script is
  re-synced into venvs by `prepare_venv` on every call, so parent and child
  protocol versions cannot drift apart.
- The interrupt-targeting question ("is the child inside the patched
  function?") cannot be answered from the parent, so the nearest sound
  reading is used: fire when a patch names any function the shipped source
  defines (`_targets_running_block`). Firing early costs one replay-backed
  retry; not firing would drop the correction.

Tests: `tests/function_manager/python/test_venv_steering.py` (boundary units,
one-shot flow, pooled flow — including that a pooled connection outlives the
session that steered it) and `tests/function_manager/shell/test_shell_steering.py`.

---

## Phase 2 — instrumented child source (venv only, not started)

Full parity inside non-dispatching loops, and the only part with a performance
question.

The parent holds the source before it ships it, so the existing AST pass can
run parent-side and the child receives already-instrumented code. `_cp`,
`_int` and `_around_cp` become shims in the child that consult the parent.

**The cost is the problem.** In-process a checkpoint is ~10 µs (measured). A
round trip over a pipe is 0.1–1 ms. A per-iteration checkpoint on a
1000-iteration loop turns ~10 ms of overhead into up to a second of pure IPC.
Naively making every checkpoint an RPC is not viable.

**The fix is a second channel.** Keep the existing RPC channel
request/response, and add a one-way parent→child control channel that the child
reads **non-blockingly** at each checkpoint:

- idle cost ≈ a non-blocking pipe read, on the order of microseconds
- a pending correction is a byte on that channel; the child then raises

This is what makes per-iteration probes affordable out-of-process, and it is
the piece to design carefully rather than the AST work, which is already
written. Build it when a real workload needs correction inside a
non-dispatching loop.

---

## What cannot reach parity, and why

State these as boundaries rather than as work items:

**Non-local surfaces do not share the transport.** This was the plan's
load-bearing open question, and the answer is no: `surface != "local"` routes
through `unify/actor/execution/targets`, whose `AgentServiceExecClient` POSTs
the whole command to the desktop agent-service `/api/exec` and blocks for a
single JSON response — python is shipped as a base64'd `python3 -c` one-liner.
There is no primitives round-trip, no mid-run message of any kind, and so no
dispatch boundary to steer at. Steering a remote surface means changing the
agent-service protocol itself (a streaming or bidirectional exec endpoint),
which is a separate piece of work, not a third integration point on this
mechanism.

**Shell interrupts cannot fire.** Shell has no AST and no Python, so there is
no function a patch can name: `_targets_running_block` finds nothing in shell
source, and the patch author (`steering_patcher.LLMPatchAuthor`) already
declines to author patches when the source defines no functions. Shell
therefore gets dispatch memoisation and pause from the shared checkpoint, and
nothing else. An interjection against a running shell script is recorded and
reported, not acted on. Making shell stoppable would need its own decision
path (e.g. "any interjection terminates the script"), which is a product
question before it is a mechanism.

**Child-local state across a retry** (was open question 2). One-shot venv
retries run in a fresh subprocess — a genuinely clean slate, stronger than
in-process. Pooled venv retries re-send `execute` on the same connection, so
the persistent namespace still holds residue from the abandoned attempt —
exactly the property in-process stateful sessions have. Accepted, not fixed.

**Where the session lives for a pooled venv** (was open question 3). Resolved
by the same contextvar that fixed this in-process: `active_session()` is read
per `execute` call, inside the connection lock, so the session follows the
call and a long-lived `_VenvConnection` never holds one. There is a test
pinning that a later call on the same connection runs unsteered.

**Synchronous blocking code** remains opaque, exactly as in-process. A
blocking call holds the child between dispatches, and during that window a
correction cannot land. (One extra wrinkle out-of-process: the child's RPC
wait has a 300 s timeout, so a pause held longer than that fails the blocked
call rather than extending it.)

**Retraction** is still impossible. Replay records that a side effect
happened; it cannot undo one. A patch redirects work that has not happened
yet. This is unchanged by anything here and should not be presented as a gap
this closes.
