ConversationManager Sandbox
==========================

Interactive playground for the `ConversationManager` component (`unity/conversation_manager/`). OSS users normally reach this through the **`unity`** CLI; developers can also run the module directly.

## Quick start (OSS)

```bash
unity
```

That opens the ConversationManager sandbox REPL against your hosted assistant (`UNIFY_KEY` + `ASSISTANT_ID` in `~/.unity/unity/.env`). Type `help` at the `>` prompt for the current command list.

Developer entry point (same REPL, from a repo checkout):

```bash
python -m sandboxes.conversation_manager.sandbox --project_name Sandbox --overwrite
```

Optional flags: `--gui` (Textual UI), `--show-trace` (auto-print CodeAct trace after each turn).

## Loom walkthrough

https://www.loom.com/share/44171c4c1aa2475abd539d1251e1baab

## Steering while work is in-flight

There are **no** `/ask`, `/i`, `/pause`, or `/stop` REPL commands. Mid-task steering works like production:

- **Text:** send another inbound chat line with `msg <content>`. Example: `msg actually include emails too`.
- **Voice:** run `meet`, speak through the LiveKit browser playground mic (same Unify Meet path as hosted voice).

The ConversationManager receives your message and the slow brain decides whether to answer, interject into a running action, stop work, etc. That is the same model as Console chat — not a separate sandbox steering layer.

Manager sandboxes under `sandboxes/*/` (contacts, knowledge, transcripts, …) still use the shared `await_with_interrupt` controls (`/i`, `/ask`, `/stop`, …) because they hold a `SteerableToolHandle` directly. The CM sandbox does not expose that handle at the REPL.

## Command reference (REPL + GUI command bar)

Notation: `<arg>` = required, `[arg]` = optional. Type `help` in the REPL for the live list.

### Meta
- `help` / `h` / `?`: show help
- `quit` / `exit`: exit sandbox
- `reset`: clear sandbox + CM state (best-effort)
- `save_project` / `sp`: snapshot the Unify project

### Inbound events
- `msg <content>`: send a test Unify chat message to the assistant (also used for in-flight steering)
- `sms <content>`: send a test inbound SMS
- `meet`: start a LiveKit voice session (opens browser playground)
- `end_meet`: end the active LiveKit voice session

### File attachments
- `attach <path>`: queue a local file for the next `msg`
- `attach`: list queued attachments
- `detach`: clear the queue

### Display / debugging
- `trace [N]`: show last N CodeAct execution turns (default 3)
- `tree`: show the current manager call event tree
- `show_logs <cm|actor|manager|all>` / `collapse_logs <...>`: expand/collapse log categories
- `agent_logs [N]`: show last N lines of agent-service logs (default 80)

CLI flag: `--show-trace` auto-prints the trace after each CodeAct turn (REPL only).

## Voice / LiveKit (`meet`)

`meet` creates a LiveKit room, spawns the production voice agent subprocess, opens the local Agents Playground in your browser, and runs the full fast-brain + slow-brain loop. Speak through the browser mic; type `end_meet` when done.

Requires `LIVEKIT_*` (a LiveKit Cloud project — `LIVEKIT_URL`/`LIVEKIT_API_KEY`/`LIVEKIT_API_SECRET`), `DEEPGRAM_API_KEY`, and a TTS key (`CARTESIA_API_KEY` or `ELEVEN_API_KEY`). LiveKit is always the configured Cloud project; only the Agents Playground UI runs locally (it auto-starts on subsequent sandbox launches).

## Computer use

The sandbox auto-bootstraps a Docker desktop container and local gateway (for UniLLM proxy + optional outbound SMS). Relevant flags:

- `--agent-server-url http://localhost:3000`
- `--agent-service-bootstrap auto` (default): start agent-service when needed

See `deploy/desktop/README.md` and the main [README](../../README.md) for BYOK keys.

## Environment variables

All env vars are read from `~/.unity/unity/.env` (OSS install) or `.env` at the repo root (developer checkout). See the main [README](../../README.md) for LLM, voice, and identity variables.

For Event Tree and Manager Logs in the GUI/REPL, set `EVENTBUS_PUBLISHING_ENABLED=true`.

### Orchestra-only execution logging (optional)

To keep interactive EventBus traffic sparse in Orchestra while still retaining a
**dense ManagerMethod + ToolLoop tree under ActiveTask runs**:

```bash
export EVENTBUS_PUBLISHING_ENABLED=true
export EVENTBUS_ORCHESTRA_PERSIST_MODE=allowlist
export EVENTBUS_ORCHESTRA_PERSIST_TOOLS=execute_code,execute_function
# optional Live Actions stream (unchanged by the allowlist):
# export EVENTBUS_PUBSUB_STREAMING=true
```

In ``allowlist`` mode:

- **Outside** a task run: only allowlisted tools (default ``execute_code`` /
  ``execute_function``) are written to ``Events/*``.
- **Inside** an ``ActiveTask`` (``CURRENT_TASK_RUN_LINEAGE`` / payload
  ``run_key`` + ``task_id``/``instance_id``): the full ManagerMethod + ToolLoop
  tree is persisted and stamped for join from ``Tasks/Executions``.

`EVENTBUS_ORCHESTRA_PERSIST_MODE=all` (default) restores legacy “write every
event to `Events/*`” behavior when publishing is enabled.

Load a run’s EventBus tree **one level at a time** (avoid saturating the
observation with a full dump):

```python
kids = await primitives.tasks.get_run_event_children(run_key=run_key)
# drill: await primitives.tasks.get_run_event_children(run_key=..., parent=kids["children"][0]["node_id"])
# detail: await primitives.tasks.get_run_event(run_key=..., node_id=...)
```

Or use the low-level helper:

```python
from unify.task_scheduler.task_run_events import fetch_task_run_events, project_immediate_children

tree = fetch_task_run_events(
    run_key,  # Tasks/Executions.run_key
    events_base_context="{user}/{assistant}/Events",
)
children = project_immediate_children(tree.rows, run_key=run_key)
```

## Troubleshooting

### Event Tree is empty / Manager Logs show "(no logs)"
Set `EVENTBUS_PUBLISHING_ENABLED=true`.

### `/ask` or `/stop` says unknown command
Expected — those commands are not part of the CM sandbox. Use `msg <text>` for in-flight steering (see above).

### Computer use / agent-service fails
Ensure Docker is running, the local gateway started (`[gateway] Ready on port 8787`), and agent-service logs are reachable via `agent_logs`.
