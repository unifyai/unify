# ConversationManager

The `ConversationManager` is the assistant's persistent interaction loop. It
reads the chat, decides whether to speak or wait, dispatches work to the
`Actor` and steers that work while it runs. It is the "front office" that
faces the user; the `Actor` is the brain that carries out the work.

There is one medium, the in-app chat, and the loop is reactive: it only ever
acts on a message it received or on the work that message started.

## Running

The chat CLI boots the loop in-process:

```bash
.venv/bin/python -m unify
```

`unify/cli.py` publishes `UnifyMessageReceived` events on the in-memory event
broker and renders the `UnifyMessageSent` replies. Any client that speaks the
same two events over the broker drives the same loop. Programmatic callers use
`run_conversation_manager()` from `main.py`.

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          Chat front end (cli.py)                          │
│        UnifyMessageReceived  ──▶   broker   ──▶  UnifyMessageSent         │
└───────────────────────────────────────────────────────────────────────────┘
                                    │  In-memory event broker (app:comms:*)
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                          ConversationManager                              │
│                                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌───────────────────────────┐  │
│  │  ContactIndex   │  │ NotificationBar │  │          Brain            │  │
│  │  (live state)   │  │    (pending)    │  │     (async tool loop)     │  │
│  └─────────────────┘  └─────────────────┘  └───────────────────────────┘  │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  EventHandler — routes broker events, requests brain turns          │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────┘
                                    │  actions (steerable handles)
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                                 Actor                                     │
│        Runs code-first plans over the state managers' primitives          │
└───────────────────────────────────────────────────────────────────────────┘
```

## Key components

### `ConversationManager` (`conversation_manager.py`)

The central object. It holds the live conversation state (`ContactIndex`,
`NotificationBar`, the in-flight and completed actions) and runs the brain,
an async tool loop that reads that state and decides what to do next.

- `wait_for_events()` subscribes to the broker channels and dispatches each
  event through `EventHandler`.
- `request_llm_run()` asks for a brain turn through the `Debouncer`, which
  coalesces bursts of events into one turn and never cancels a running one.
- `run_llm()` executes one brain turn: render the state panes, call the LLM,
  run the tools it chose.
- As transcript windows fill, the loop hands them to `MemoryManager` for
  offline consolidation.

### The brain (`domains/brain.py`, `brain_tools.py`, `brain_action_tools.py`)

The brain's tools fall into three groups:

- **Speaking**: `send_unify_message` and the wait/no-op choice.
- **Direct manager access**: `ask_about_contacts`, `update_contacts`,
  `query_past_transcripts`.
- **Actions**: `act` dispatches a request to the `Actor` and returns a
  steerable handle. Six fixed, handle-addressed tools (`interject_action`,
  `ask_action`, `pause_action`, `resume_action`, `stop_action`,
  `answer_clarification_action`) steer any in-flight action;
  `task_actions.py` renders the ready-to-use invocations shown in the state
  panes.

Prompt composition lives in `prompt_builders.py`; `domains/renderer.py`
renders the panes the brain reads each turn.

### `ConversationManagerHandle` (`handle.py`)

The handle the `Actor` receives so a running plan can reach back into the
conversation:

- `ask(question, response_format)` starts a nested tool loop that either
  answers from the recent transcript (PATH 1) or posts the question to the
  chat and waits for the reply (PATH 2). While a PATH 2 question is open the
  handle is registered as `active_ask_handle`, and the next inbound chat
  message is delivered to it as the answer instead of waking the brain.
- `interject(message)` injects information into the conversation.
- `get_full_transcript()` returns recent conversation history.

### `EventHandler` (`domains/event_handlers.py`)

Registry-based dispatcher: `@EventHandler.register(EventClass)` binds a
handler. Inbound and outbound chat messages are logged to the transcript,
pushed into the `ContactIndex`, and summarised on the `NotificationBar`;
actor lifecycle events update the action panes; `Error` events surface as
notifications so the brain can recover.

### Events (`events.py`)

Dataclass events carried over the broker:

- **Chat**: `UnifyMessageReceived`, `UnifyMessageSent`, `DirectMessageEvent`.
- **Actor**: `ActorRequest`, `ActorResponse`, `ActorHandleStarted`,
  `ActorResult`, `ActorNotification`, `ActorClarificationRequest` /
  `ActorClarificationResponse`, `ActionStopRequested`.
- **State**: `NotificationInjectedEvent`, `NotificationUnpinnedEvent`,
  `StoreChatHistory` / `GetChatHistory`, `InitializationComplete`, `Error`.

The broker itself (`event_broker.py`, `in_memory_event_broker.py`) is an
in-process pub/sub keyed by channel prefix (`app:comms:*`, `app:actor:*`).

### `ContactIndex` (`domains/contact_index.py`)

Live per-contact conversation state: the contact record, its recent messages
and attachments, and the fallback contacts a front end can register before
`ContactManager` is ready.

## Tests

```bash
tests/parallel_run.sh tests/conversation_manager/
```

| Directory | Covers |
|---|---|
| `core/` | Initialisation, event handling, brain turns, resilience |
| `handle/` | The `ask` flow, PATH 1 and PATH 2, interjections |
| `actions/` | Action dispatch and steering; `actions/integration/` drives a real `CodeActActor` |
| `flows/` | End-to-end conversation flows |

The parent conftest pins the registry's actor to the simulated one so brain
tests stay cheap; the integration suite builds a `CodeActActor` directly.
