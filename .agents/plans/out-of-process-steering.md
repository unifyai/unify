# Plan: steering out-of-process execution

**Status:** built and tested end to end. Phase 1 — dispatch-boundary
steering over the RPC channel — covers venv (one-shot and pooled) and shell.
Phase 2 — instrumented child source plus a push control channel — closes the
non-dispatching-loop gap for venv, including synchronous busy-loops via the
sync interrupt probe. Stop requests end any path cleanly while preserving
completed work; pause freezes subprocesses at the OS level; non-local
surfaces take stop and pause through the agent-service `/exec/signal` route.
What remains excluded is deliberate — see the boundaries section.

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

Shell scripts using the `unity-primitive` bridge get dispatch-boundary cache
and pause. They can also be stopped between dispatches: a stop request
terminates the subprocess group and returns the same structured stopped
outcome as Python execution. Function patches remain Python-only.

Non-local surfaces (`surface != "local"`) get the two process-level verbs —
stop and pause — through the agent-service's `/exec/signal` route, addressed
at a client-chosen exec id while `/api/exec` blocks. Dispatch-level steering
(memoise, patch, replay) deliberately does not exist there; see the
boundaries section.

**Not covered.** Blocking calls between checkpoints (freezable via pause,
not correctable), and the deliberate exclusions in the boundaries section.

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
 "reason": str, "functions": [str], "stop": bool}                   # parent → child, unsolicited
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
  a pending correction targets the running block or requests a stop, replays
  memoised dispatches, and memoises fresh ones.
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
a correction targets the running block, or asks to stop it, the watcher sends
one directive and returns; the attempt's loops handle a patch through the same
retry path as Phase 1 and translate a stop into a clean terminal outcome.

Tests: the "corrections between dispatches" section of
`test_venv_steering.py` — a 2000-iteration loop with no primitive calls is cut
short mid-run on both the one-shot and pooled paths, replaying its completed
prefix, and a stale directive provably does not leak into the next request on
the same pooled connection.

---

## What cannot reach parity, and why

State these as boundaries rather than as work items:

**Non-local surfaces are stop/pause-only, by design.** They never shared the
RPC transport: `AgentServiceExecClient` POSTs the whole command to the
desktop agent-service `/api/exec` and blocks for one JSON response, with no
primitives round-trip and therefore no dispatch record. What rolled out is
the honest subset: the client supplies its own exec id, the agent registers
the running process group, and a second route (`POST /exec/signal`, actions
`stop`/`pause`/`resume`) reaches it mid-run — SIGTERM-with-SIGKILL-sweep and
SIGSTOP/SIGCONT on the group (`agent-service/src/execControl.ts`). A stop
becomes the run's outcome only when the agent *acknowledged* it; an agent too
old to know the route answers 404 and the run proceeds unsteered, which is
how old deployed desktops keep working until they update.

Patches are deliberately inert remotely, not merely unbuilt: replay is what
makes a patched re-run safe, and with no dispatch record a re-run would
repeat every side effect. The client watches corrections with empty targeting
source so only stop requests can fire. Dispatch-level steering remotely means
building a remote primitives bridge first — a different project.

**Shell patches cannot fire, but stops can.** Shell has no AST and no Python,
so there is no function a patch can name: `_targets_running_block` finds
nothing in shell source. The patch author can nevertheless decide that a
correction revokes the task or makes all remaining work wrong and return a
language-independent stop request. `execute_shell_script` then terminates the
subprocess group. Corrections that need to alter and continue shell source are
still recorded but cannot be applied live.

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

**Synchronous code is narrower than async, but no longer opaque.** Raising
needs no await, so sync ``def``s now carry a synchronous interrupt probe
(``_int_s``) at entry and per loop iteration. In-process that fires only when
a correction was already pending as the sync frame began — the frame holds
the event loop, so nothing can *collect* while it runs. In a venv child the
stdin reader keeps the directive state fresh from its own thread, which makes
a sync busy-loop genuinely interruptible mid-run — a case in-process
execution fundamentally cannot reach. What remains out of reach on both
sides: a single blocking *call* (``time.sleep``, sync HTTP) between probes,
which pause (below) can at least freeze. (One extra wrinkle out-of-process:
the child's RPC wait has a 300 s timeout, so a pause held longer than that
fails the blocked call rather than extending it.)

**Pause is process-level, not protocol-level** (was: not propagated). The
tool handle's pause event now feeds the steering session
(`_sandbox_call_binding` → `SteeringRuntime(pause_event=...)`), in-process
checkpoints hold on it as before, and each subprocess boundary runs a
`relay_pause` watcher that mirrors state changes into SIGSTOP/SIGCONT on the
child's process group (`FunctionManager._set_process_paused`). Freezing the
process holds it *anywhere* — mid-loop, mid-`time.sleep`, inside a C
extension — which no checkpoint could. Terminate paths thaw first, so a
frozen child never sits on an undeliverable SIGTERM. Windows has no stop
signal, so pause degrades to the dispatch-boundary hold there.

**Stateful shell sessions** (`ShellPool`) have no primitives bridge and no
per-command subprocess: the session *is* the long-lived process, so a stop
that kills it destroys the state the mode exists to keep. Stopping one
command inside a persistent shell means signalling the pty's foreground
group, which is its own piece of work if a real need appears.

**Pooled retry residue stays.** A pooled venv retry could snapshot and
restore child state around attempts, but state serialization is best-effort
— a half-restored namespace is worse than the residue, and in-process
stateful sessions have the identical property. Parity is the correct
behaviour, not a gap.

**Windows children cannot pause.** No stop signal exists; pause degrades to
the dispatch-boundary hold, and remote pause on a Windows VM answers
`unsupported`. Stop works everywhere.

**Retraction** is still impossible. Replay records that a side effect
happened; it cannot undo one. A patch redirects work that has not happened
yet. This is unchanged by anything here and should not be presented as a gap
this closes.
