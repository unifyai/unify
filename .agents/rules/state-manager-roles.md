---
description: Which manager owns what, and where each one's jurisdiction ends
---

Use this to decide which manager to call, what each owns, and where its jurisdiction ends. Keep manager docstrings implementation‑agnostic; this guide is only for high‑level routing and composition.

### ConversationManager
- **Role**: The persistent interaction loop. Reads inbound chat events, decides whether to speak or wait, routes work to `Actor` for code-first execution and steers it (pause/resume/interject/stop/ask) while it runs.
- **Scope**: Conversation‑level control and message flow over the single in-app chat medium; relays steerable handles from inner tools.
- **Connections**:
  - **Steered by**: The chat front end (`unify/cli.py`, or any client that publishes `UnifyMessageReceived` on the in-memory event broker).
  - **Steers**: `Actor.act`; relays in‑flight handles.

### Actor
- **Role**: Central intelligence that orchestrates all state managers through code-first plans. Generates and executes Python plans that call primitives and top-level JSON tools.
- **Scope**: Code-first execution via `act()`. Plans orchestrate `primitives.contacts.*`, `primitives.transcripts.*`, `primitives.files.*`, `primitives.data.*`, `primitives.ingestion.*`, `primitives.secrets.*` and `primitives.actor.*`, plus top-level JSON tools such as `GuidanceManager_*`, `KnowledgeManager_*` and `FunctionManager_*`. Wires in‑flight handles back to `ConversationManager` for real‑time steering.
- **Connections**:
  - **Steered by**: `ConversationManager` (primary caller of `act()`).
  - **Steers**: State manager primitives, the typed catalogue JSON tools, and the `ConversationManager` handle (`ask`/`interject`/`get_full_transcript`). Uses `FunctionManager` for function discovery and execution.

### Actor routing playbook
- **Read‑only questions**
  - Contacts → `primitives.contacts.ask`
  - Transcripts → `primitives.transcripts.ask` (may call `primitives.contacts.ask` for participants)
  - Knowledge → `KnowledgeManager_search` / `KnowledgeManager_filter` / `KnowledgeManager_get_knowledge`
  - Secrets (metadata/placeholders only) → `primitives.secrets.ask`
  - About a specific received file (filename known) → `primitives.files.ask`
- **Mutations (create/edit/delete/merge)**
  - Contacts → `primitives.contacts.update`
  - Knowledge claims → `KnowledgeManager_add_knowledge` / `KnowledgeManager_update_knowledge` / `KnowledgeManager_invalidate_knowledge` / `KnowledgeManager_supersede_knowledge` / `KnowledgeManager_delete_knowledge`
  - Guidance → `GuidanceManager_add_guidance` / `GuidanceManager_update_guidance` / `GuidanceManager_delete_guidance`
  - Secrets → `primitives.secrets.update`
- **Storing new data or files (any source)**
  - Rows in hand, specific files, whole folders, or a reshape of a stored table → `primitives.ingestion.submit(source, target)`. One verb for every source/target pairing; returns a run handle immediately.
  - Observe and recover with `primitives.ingestion.get_status` / `get_logs` / `wait` / `retry` / `cancel` / `pause` / `resume`. `status.next_step` states the one action that makes sense.
  - Close a run with `primitives.ingestion.reconcile` before calling the data ready: it reports rows landed against rows expected **and** the columns that are blank in every row sampled.
  - There is no `primitives.data.ingest`.
- **File → knowledge distillation**
  - Parse with `primitives.files.parse`, then distill durable statements into typed claims via `KnowledgeManager_add_knowledge` (attach `source_refs` pointing at the file / transcript / user statement).
- **Images**
  - Images are referenced **by filesystem path** across the entire stack, relative to the workspace root (`<UNIFY_HOME>/workspace`). Managers and plans reference images via their relative filepath.

### ImageManager
- **Role**: Persistent image store and metadata registry. Provides durable `image_id`‑keyed storage in the `Images` context, backing filesystem images with queryable metadata.
- **Data model & identity**:
  - Every stored image has a unique numeric `image_id`.
  - Image rows store base64 bytes plus metadata (caption, timestamp, mime/type) and an optional `filepath` recording where the image sits in the workspace.
- **ImageHandle wrapper**: internal code operates on an `ImageHandle` exposing `image_id`, `caption`/metadata, `filepath`, `raw()` and `ask(question)` (a vision‑capable model answers about the image).
- **Cross‑manager image convention**: filesystem paths are the universal image reference at the orchestration boundary; a receiving manager resolves a path to an `image_id` via `ImageManager.filter_images(filter="filepath == '...'")` when persistent linkage is needed. Managers with a first-class `images` field (e.g. `GuidanceManager`) accept structured `ImageRefs` at their own API boundary.
- **Connections**:
  - **Steered by**: managers that persist or query images (`GuidanceManager`, `TranscriptManager`).
  - **Steers**: —

### KnowledgeManager
- **Role**: Passive typed claim ledger for durable domain knowledge (facts, policies, definitions, decisions, constraints, insights, preferences) with provenance (`source_refs`) and lifecycle status (active / superseded / invalidated).
- **Scope**: CRUD and lifecycle operations (`search`, `filter`, `get_knowledge`, `add_knowledge`, `update_knowledge`, `delete_knowledge`, `invalidate_knowledge`, `supersede_knowledge`, `reconcile_sources`, `clear`) exposed as first-class JSON tools on the Actor (`KnowledgeManager_*`). No natural-language `ask` / `update` tool loops.
- **Negative scope**: Does **not** own people/contacts (ContactManager), procedural how-tos/SOPs (GuidanceManager), user Python functions (FunctionManager), received file bytes/parsing (FileManager), or secrets/credentials (SecretManager).
- **Writers**: the Actor / ConversationManager (user-requested claim storage), the storage review loop (trajectory distillation into claims), and MemoryManager (offline consolidation).
- **Connections**:
  - **Steered by**: `Actor` (via `KnowledgeManager_*` JSON tools); `MemoryManager`.
  - **Steers**: —

### ContactManager
- **Role**: Source of truth for people/contact records, including the assistant's own contact and the user's (boss) contact.
- **Scope**: ask (read‑only), update (create/edit/delete/merge contacts).
- **Connections**:
  - **Steered by**: `Actor` (via `primitives.contacts.*`); `ConversationManager` (direct `ask_about_contacts` / `update_contacts` tools); read‑only usage by `TranscriptManager.ask`.
  - **Steers**: —

### TranscriptManager
- **Role**: Store and retrieval surface for message transcripts.
- **Scope**: ask (read‑only retrieval, filtering, analysis); logs every inbound and outbound chat message.
- **Edge**: Summarize conversations here; write long‑term distilled facts to `KnowledgeManager` if needed.
- **Connections**:
  - **Steered by**: `Actor` (via `primitives.transcripts.*`); `ConversationManager` (`query_past_transcripts`).
  - **Steers**: `ContactManager.ask` (for participant lookup in transcript answers).

### FileManager
- **Role**: Read‑only registry and parsing for files in the workspace and chat attachments.
- **Scope**: exists/list, parse, ask about a specific file (read‑only tool loop), describe (storage discovery).
- **Connections**:
  - **Steered by**: `Actor` (via `primitives.files.*`).
  - **Steers**: `DataManager` (internally delegates filter/search/reduce/join operations).

### DataManager
- **Role**: Low‑level data operations on any store context.
- **Scope**: filter, search, reduce, join, insert, update, delete, vectorize, plot. `ingest` is the low-level chunked write engine and is **not** exposed to the Actor — storing new data routes through `IngestionManager` so every write is recorded, checkpointed and recoverable.
- **Connections**:
  - **Steered by**: `FileManager`, `IngestionManager`, `Actor` (via `primitives.data.*`).
  - **Steers**: — (pure primitives module, no high‑level tool loops).

### IngestionManager
- **Role**: The one verb for storing data and files from anywhere — `submit(source, target)` — with a resumable, checkpointed engine behind it.
- **Scope**: `submit`, `get_status`, `get_logs`, `wait`, `list_runs`, `retry`, `cancel`, `pause`, `resume`, `reconcile` via `primitives.ingestion.*`. Sources: `RowsSource`, `FilesSource` / `FolderSource`, `TableSource`. Targets: `TableTarget` or `CollectionTarget`. Runs and their events are rows in `Ingestion/Runs` + `Ingestion/Events`.
- **Negative scope**: does not query or reshape-in-place (DataManager), does not answer questions about file contents (FileManager `ask`).
- **Connections**:
  - **Steered by**: `Actor` (via `primitives.ingestion.*`); `FileManager` (attachment ingestion).
  - **Steers**: `DataManager.ingest` and the file parse pipeline.

### SecretManager
- **Role**: Owner of secrets.
- **Scope**: ask (metadata/placeholder answers only), update (create/edit/delete secrets).
- **Connections**:
  - **Steered by**: `Actor` (via `primitives.secrets.*`).
  - **Steers**: —

### FunctionManager
- **Role**: Catalogue of stored Python functions, their venvs and their verification ledger (effect class, contract, tier‑0 checks, trust).
- **Scope**: add/list/filter/search/delete over functions, execution in-process or in a per-function venv, and the read-only builtins catalogue of every primitive the Actor can call.
- **Connections**:
  - **Steered by**: `Actor` (discovers and executes functions during plans; the storage review loop stores new ones).
  - **Steers**: —

### GuidanceManager
- **Role**: Owner of procedural how-to information: step-by-step instructions, SOPs, software walkthroughs, and strategies for composing functions together.
- **Scope**: CRUD (search, filter, add_guidance, update_guidance, delete_guidance) exposed as `GuidanceManager_*` JSON tools on the Actor. Read tools are gated by the discovery-first policy.
- **Builtins library**: reads also federate over a global, read-only guidance catalogue (`Guidance` context in the `Builtins` project) holding entries imported from the Agent Skills ecosystem with stable hash-based ids and `is_builtin=True`. Seeded from the committed snapshot `unify/guidance_manager/builtins_guidance.json`; `update_guidance`/`delete_guidance` refuse builtin ids.
- **Connections**:
  - **Steered by**: `Actor` (via `GuidanceManager_*` JSON tools).
  - **Steers**: reads functions from the shared "Functions" context to surface linked functions.

### MemoryManager
- **Role**: Offline memory maintenance (periodic, non‑interactive).
- **Scope**: One‑shot methods that return strings (no live handles): updating contacts, bios, rolling summaries, response policies and knowledge from transcript windows.
- **Connections**:
  - **Steered by**: `ConversationManager` (every fifty messages) and event-bus callbacks.
  - **Steers**: `ContactManager.update`, `KnowledgeManager` typed claim APIs.

### EventBus
- **Role**: Cross‑cutting, in‑process publish/subscribe backbone and searchable event log used by all managers for telemetry and coordination.
- **Scope**: Managers publish structured events (notably `ManagerMethod` for incoming/outgoing method calls) via a thin logging wrapper; the bus supports `publish`, `search`, `join_published`/`join_callbacks` for deterministic flushing, per‑type window sizing, and callback registration. Persisting events to `Events/*` contexts is off by default.
- **Connections**:
  - **Steered by**: All public manager methods (through the logging decorator).
  - **Steers**: `MemoryManager` (registers callbacks to react to message and `ManagerMethod` events).

### Precedence and source of truth
- **Code is canonical**: This guide is descriptive. If the implementation contradicts it, the code takes precedence.
- **Keep in sync**: As managers evolve, update this document alongside changes to cross‑manager wiring, public surfaces, or prompt composition.
- **Where to update**: Prefer updating manager base docstrings (public API contracts) and prompt builders for tool composition guidance, and reflect those changes here.
