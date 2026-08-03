# Plan: steering out-of-process execution

**Status:** Phases 1 and 2 built and tested. Phase 1 — dispatch-boundary
steering over the RPC channel — covers venv (one-shot and pooled) and shell.
Phase 2 — instrumented child source plus a push control channel — closes the
non-dispatching-loop gap for venv. Non-local surfaces turned out not to share
the RPC transport at all (see below) and are out of scope for this mechanism.

Live steering — correcting a block of code while it is still running — works
for every in-process Python path, at the dispatch boundary for venv and shell
subprocesses, and between dispatches for venv. This document records how, and
what was learned closing the plan's open questions.

Read `unify/function_manager/steering.py` first. Its four parts (probes,
idempotency cache, retry via source splice, bounded lifetime) are reused
out-of-process rather than replaced; the out-of-process entry points are
`dispatch_with_steering` and `SteeringSession.relay_corrections` in the same
module.

---

## Where things stand

**Covered.** Every in-process Python path (statement-level, via AST probes):

- `PythonExecutionSession.execute` — all state modes, including the `stateless`
  default that `execute_code` uses
- `FunctionManager._execute_in_default_env` — the route that skips the sandbox
  entirely and runs a stored function directly

Venv subprocesses, with the same statement-level probes the in-process paths
get — the parent instruments the source before shipping it, and interrupts
land both at RPC replies and, via the control channel, inside loops that make
no primitive call:

- One-shot venv subprocesses — `FunctionManager.execute_in_venv`
- Pooled persistent venv sessions — `_VenvConnection.execute`

Shell scripts using the `unity-primitive` bridge get dispatch-boundary
steering only — `FunctionManager.execute_shell_script` (cache and pause; see
the boundaries section for why interrupts cannot fire on shell).

**Not covered.** Non-local surfaces (`surface != "local"`), synchronous
functions (no awaits, so no probes — parity with in-process), and blocking
calls between checkpoints.

---

## The key finding: the channel already existed

The obvious reading of this problem was "we need a bidirectional event stream
to and from the child process". We already had one.

`unify/function_manager/venv_runner.py` documents the protocol in its module
docstring:

```
{"type": "rpc_call",      "id": str, "path": str, "kwargs": dict}    # child → parent
{"type": "rpc_result",    "id": str, "result": Any}                  # parent → child
{"type": "rpc_error",     "id": str, "error": str}                   # parent → child
{"type": "rpc_interrupt", "id": str, "reason": str}                  # parent → child
{"type": "control", "action": "interrupt",
 "reason": str, "functions": [str]}                                  # parent → child, unsolicited
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

## Phase 2 — instrumented child source over a push control channel

Phase 1 fires only when the child dispatches; a loop that makes no primitive
call is invisible. Phase 2 closes that for venv: the parent ships instrumented
source, and a correction reaches the child as a pushed directive its next
checkpoint reads at in-process cost.

**Instrumentation is parent-side and shared.** `_instrument_for_child`
(`function_manager.py`) runs the same AST pass in-process code uses, on the
clean source each attempt ships — the patch splice always happens on
uninstrumented source, exactly as in-process. Source that does not parse ships
unchanged so the child reports the SyntaxError normally.

**The child runs the probes against shims, not the parent.** `venv_runner`
installs `_cp` / `_int` / `_around_cp` / `runtime` in the sandbox globals.
`runtime` is a no-op position sink (out-of-process the cache keys by
occurrence alone); `_int(name)` raises the child's `ControlledInterruption`
when a pending directive names `name`. An interrupted run's completion carries
an `"interrupted"` marker, which the parent's execute loops translate back
into the retry.

**The "second channel" needed no second pipe.** The plan expected a dedicated
control fd read non-blockingly per checkpoint (~µs). What shipped is cheaper:
control directives are multiplexed onto stdin, and a single daemon thread owns
the descriptor for the child's lifetime, routing RPC replies to their blocked
callers, directives into module state, and runner commands to the main thread.
An idle checkpoint pair (`_cp` + `_int`) is then two attribute reads —
**0.1 µs/iteration measured**, against ~10 µs in-process and the 0.1–1 ms
round trip the plan ruled out. The performance question dissolved rather than
needing tuning.

Two constraints the reader thread has to honour, both learned the hard way:

- **It must read the raw fd (`os.read`), not buffered `readline`.** A thread
  blocked in `readline` holds the TextIOWrapper lock; a fork()ed
  multiprocessing child inherits that held lock from a thread it doesn't
  have, and deadlocks in its own bootstrap closing `sys.stdin`. This is also
  what makes unsolicited directives deliverable at all — a select-over-
  buffered-readline loop leaves a directive glued to an RPC reply invisible
  in the buffer.
- **Interrupt state clears on each `execute`.** A pooled child is persistent;
  a directive consumed by attempt N must not fire at attempt N+1's first
  probe. Ordering is safe because directive and execute share one pipe and
  one reader.

**The parent side is a watcher, not a poller of the child.**
`SteeringSession.relay_corrections` runs alongside each attempt's message
loop, collecting interjections while the child runs between dispatches —
without it, nothing would even author a patch until the next RPC arrived. When
a correction targets the running block it sends one directive and returns; the
attempt's loops handle the rest through the same retry path as Phase 1.

Tests: the "corrections between dispatches" section of
`test_venv_steering.py` — a 2000-iteration loop with no primitive calls is cut
short mid-run on both the one-shot and pooled paths, replaying its completed
prefix, and a stale directive provably does not leak into the next request on
the same pooled connection.

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
correction cannot land; synchronous ``def``s get no await probes on either
side of the boundary. (One extra wrinkle out-of-process: the child's RPC wait
has a 300 s timeout, so a pause held longer than that fails the blocked call
rather than extending it.)

**Pause is not propagated between dispatches.** The parent holds RPC replies
while paused, which stalls the child at its next dispatch, but the child's
``_cp`` shim is a plain yield point. Nothing in the runtime drives
``SteeringRuntime.pause`` today; if something starts to, the control channel
is where a pause directive would ride.

**Retraction** is still impossible. Replay records that a side effect
happened; it cannot undo one. A patch redirects work that has not happened
yet. This is unchanged by anything here and should not be presented as a gap
this closes.
