Use this to decide which component owns what and where its jurisdiction ends. Keep manager docstrings implementation‑agnostic; this guide is only for high‑level routing and composition.

### ConversationManager
- **Role**: The persistent interaction loop. Reads the chat, decides whether to speak or wait, routes work to `Actor` for code-first execution and steers it (pause/resume/interject/stop/ask) while it runs.
- **Scope**: One user, one chat. Owns the chat history (in memory, persisted in its own store table), the notification bar and the in-flight and completed actions. Attachments are file paths passed along with a message.
- **Connections**:
  - **Steered by**: the chat front end (`unify/cli.py`, or any client that publishes `UnifyMessageReceived` on the in-memory event broker).
  - **Steers**: `Actor.act`; relays in‑flight handles.

### Actor
- **Role**: Central intelligence. Generates and executes Python programs in a persistent sandbox to satisfy a request, discovering stored skills first.
- **Scope**: Code-first execution via `act()`. A plan is plain Python plus the sandbox's namespaces: `primitives.actor.*` (nested actors for parallel or delegated work), `execute_function` for stored functions, and the `FunctionManager_*` / `GuidanceManager_*` JSON tools for the skill libraries. Files anywhere on the machine are read where they are; outputs go into the workspace. Wires in‑flight handles back to `ConversationManager` for real‑time steering.
- **Connections**:
  - **Steered by**: `ConversationManager` (primary caller of `act()`).
  - **Steers**: nested actors, the skill libraries' JSON tools, and the `ConversationManager` handle (`ask`/`interject`/`get_full_transcript`).

### Actor routing playbook
- **Before writing code**: `FunctionManager_search_functions` / `FunctionManager_filter_functions` for a stored function that already does it; `GuidanceManager_search` / `GuidanceManager_filter` for a procedure that says how. The discovery-first policy gates the other tools until both were consulted.
- **A single stored function or primitive call** → `execute_function` (a bare steerable handle; never `execute_code` for one call).
- **Anything else** → `execute_code`: plain Python, loops, control flow, files, packages installed into the workspace environment.
- **Parallel or delegated work** → `primitives.actor.act`.
- **Asking the user** → `request_clarification` (bubbles up through every layer to the chat).
- **After a run** the storage review decides what to keep: a callable that worked becomes a function, a non-obvious composition becomes guidance.

### FunctionManager
- **Role**: Catalogue of stored Python functions (the **what**) and their pip dependencies.
- **Scope**: add/list/filter/search/delete over functions, execution in-process with dependencies ensured in the workspace environment, and the read-only builtins catalogue of every primitive the Actor can call.
- **Connections**:
  - **Steered by**: `Actor` (discovers and executes functions during plans; the storage review stores new ones).
  - **Steers**: —

### GuidanceManager
- **Role**: Owner of procedural how-to information (the **how**): step-by-step instructions, walkthroughs, and strategies for composing functions together.
- **Scope**: CRUD (search, filter, add_guidance, update_guidance, delete_guidance) exposed as `GuidanceManager_*` JSON tools on the Actor. Read tools are gated by the discovery-first policy.
- **Builtins library**: reads also federate over a global, read-only guidance catalogue (`Guidance` context in the `Builtins` project) holding entries imported from the Agent Skills ecosystem with stable hash-based ids and `is_builtin=True`. Seeded from the committed snapshot `unify/guidance_manager/builtins_guidance.json`; `update_guidance`/`delete_guidance` refuse builtin ids.
- **Connections**:
  - **Steered by**: `Actor` (via `GuidanceManager_*` JSON tools).
  - **Steers**: reads functions from the shared "Functions" context to surface linked functions.

### EventBus
- **Role**: Cross‑cutting, in‑process publish/subscribe backbone and searchable event log used by every component for telemetry and coordination.
- **Scope**: Components publish structured events (notably `ManagerMethod` for incoming/outgoing method calls) via a thin logging wrapper; the bus supports `publish`, `search` over a bounded in-memory ring, `register_callback`/`unregister_callback`, and `join_callbacks` for deterministic joins.
- **Connections**:
  - **Steered by**: all public manager methods (through the logging decorator).
  - **Steers**: —

### Precedence and source of truth
- **Code is canonical**: This guide is descriptive. If the implementation contradicts it, the code takes precedence.
- **Keep in sync**: As components evolve, update this document alongside changes to cross‑component wiring, public surfaces, or prompt composition.
- **Where to update**: Prefer updating manager base docstrings (public API contracts) and prompt builders for tool composition guidance, and reflect those changes here.
