from __future__ import annotations

import inspect
import textwrap
import json
from typing import Callable, Dict, Optional, Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    from unify.actor.environments.base import BaseEnvironment

# ---------------------------------------------------------------------------
# Static prompt content (inlined rather than wrapped in trivial functions)
# ---------------------------------------------------------------------------

_FUNCTION_GUIDANCE_AND_KNOWLEDGE_LIBRARY = textwrap.dedent("""
    ### Function, Guidance & Knowledge Library

    Three complementary systems (all read + write):

    * **FunctionManager** — the *what*: concrete, reusable
      function implementations (results carry `guidance_ids`).
    * **GuidanceManager** — the *how*: procedures, SOPs,
      walkthroughs, composition strategies (results carry `function_ids`).
    * **KnowledgeManager** — the *is*: durable sourced claims
      (facts, policies, definitions, decisions, constraints, insights,
      preferences) with provenance. Not people, not procedures, not secrets.

    **Discovery index scope:** Function search covers user-stored functions
    **and** the built-in `primitives.*` catalogue — primitive rows come back
    with `is_primitive`, `argspec`, and `docstring`. The exception:
    callables already documented in this prompt (computer-control
    methods, prompt-injected functions and guidance) —
    they never appear in search results, so empty discovery does **not**
    mean a prompt-documented callable is unavailable; call it by exact
    name via `execute_function`.

    Always search **FunctionManager, GuidanceManager, and KnowledgeManager**
    before deciding how to execute:

    1. `FunctionManager_search_functions` — find existing implementations
    2. `GuidanceManager_search` — find procedures and composition strategies
    3. `KnowledgeManager_search` — find durable domain claims
    4. Prefer healthy matches (empty `stale_reasons`); stale entries are
       second-class — disclose the debt if used and repair via
       update/re-link.
    5. Use what you find: call a relevant function via `execute_function`,
       follow relevant guidance, use relevant claims (fetch full text
       with `get_knowledge` when needed).
    6. A no-hit is **not** permission to immediately write new code.
       Search is a discovery step, not an execution decision.
    7. After discovery, choose the minimal correct execution path:
       - if the request or discovery step already identifies one exact function
         or primitive call, use `execute_function`
       - use `execute_code` only when the task genuinely requires
         multi-step composition.

    Search/filter results truncate long entries: when a discovered entry
    is actually relevant, fetch the complete body with
    `GuidanceManager_get_guidance` / `KnowledgeManager_get_knowledge` — do
    not act on a truncated preview.

    #### Writing to the libraries

    - **Guidance**: user-provided procedures to be remembered — persisting
      them IS the task — go directly to `GuidanceManager_add_guidance`.
      Guidance entries are the canonical home for durable shared rules
      stored functions apply: when the user changes such a rule, update
      the canonical entry FIRST (its `function_ids` are the authoritative
      affected set), revise every linked function, and verify none was
      missed — never add a second copy.
    - **Knowledge**: durable non-person, non-procedure, non-secret claims
      go through `KnowledgeManager_add_knowledge` after searching for
      duplicates; attach `source_refs`, and prefer `supersede_knowledge` /
      `invalidate_knowledge` over silent overwrite.
    - **Functions**: explicit user requests to add/update/delete functions
      use `FunctionManager_add_functions` (`overwrite=True` to update) or
      `FunctionManager_delete_function` directly. When a stored
      function or custom task touches Orchestra tables (`Data/*` or other
      tabular contexts), its body **must** use `primitives.data` with
      server-side `filter=` / `reduce` / `update_rows` / `insert_rows` /
      `ingest` — never client-side full-table scans, which become
      permanent production hot paths.
    - For skills discovered *during* execution, use `store_skills` —
      a dedicated review extracts functions, compositional guidance, and
      durable claims from the trajectory.

    #### Function Execution Modes

    Functions support execution mode overrides independent of the session's
    `state_mode`:

    | Mode | Syntax | Behavior |
    |------|--------|----------|
    | **stateful** (default) | `await func(...)` | Function's internal state persists across calls |
    | **stateless** | `await func.stateless(...)` | Fresh environment, no inherited state |
    | **read_only** | `await func.read_only(...)` | Sees current state, changes discarded |
""").strip()

_WORKFLOW_SHELF = textwrap.dedent("""
    ### Installable workflows

    The `WorkflowManager_*` tools manage a shelf of curated, versioned
    packages — each one sets an assistant up for a recurring job (its
    procedures, functions, recurring tasks, typed claims) in one install.

    **Install vs. build.** When the user asks for a capability shaped like
    a packaged job ("set me up to draft email replies each morning"),
    check the shelf first with `WorkflowManager_list_workflows`. Install
    when a workflow matches; build with tasks/guidance/functions only for
    genuinely bespoke requests. Do not hand-assemble what a workflow
    already packages, and do not install a workflow to get just one of
    its pieces.

    **Installing is consequential.** It plants content and creates
    recurring jobs that will act on their own later. Confirm with the
    user before installing anything they did not explicitly ask for, and
    afterwards report exactly what was set up — every recurring job and
    its schedule in plain language, and any settings recorded.

    **A missing connection is not an error.** When the result carries
    `connect_required`, the workflow is installed but held: tell the user
    which app to connect and that nothing fires in the meantime.
    Reinstalling after they connect arms the jobs.

    **After install, everything is ordinary.** Planted procedures are
    found by the same guidance search as any other; planted tasks are
    ordinary tasks, run and steered through the usual task tools.
    Uninstalling removes what the workflow planted and stops its jobs —
    name them before confirming.

    **Running one is always explicit.** Installing plants and arms; it
    never runs. When the user asks for a workflow's work now — "run my
    briefing", "do the review sweep" — take the `task_id` from that
    installation's `jobs` (in `WorkflowManager_get_workflow`, or the
    installed entries of `WorkflowManager_list_workflows`) and start it
    with `primitives.tasks.execute`. There is no workflow-level run,
    because a workflow has no runtime: the task it planted does. A job
    reporting `enabled: false` is held on a missing connection, and
    starting it is refused — say which app to connect instead.

    **Settings travel with the run.** A planted task's run request carries
    the workflow's recorded installation settings, already resolved. The
    settings tool exists for reading and discussing configuration with the
    user, not for runs to fetch their own.

    **"Is it working?" is a question about runs.** `jobs` reports arming,
    never whether the work happened — an armed job whose every occurrence
    fails looks identical to a healthy one here. Take its `task_id` and ask
    `primitives.tasks` about that task's runs.
""")


_DISCOVERY_FIRST_POLICY = textwrap.dedent("""
    ### Discovery-First Policy (Active) — HARD REQUIREMENT

    A tool policy gates the full toolkit until each present library family
    has been discovered: until then **only** FM / GM / KM discovery tools
    are available; the full tool set unlocks automatically once every
    present gate has been called.

    The CORRECT procedure:
    1. Your **first tool-calling assistant message** includes every present
       discovery family as parallel tool_calls in that same message:
       `FunctionManager_search_functions` (with a non-empty `query`),
       `GuidanceManager_search`, and `KnowledgeManager_search` — omitting
       only families whose tools are absent. Do not answer in plain text
       first, do not serialize families across turns, and call only tools
       that appear in the current tool list.
    2. Then choose the minimal correct execution path.
    3. If one exact function or primitive call is enough, use execute_function.
    4. Use execute_code only when the task genuinely needs multi-step
       composition, branching, iteration, or combining intermediate results.
""").strip()

_TOOL_SELECTION_AND_SURFACES = textwrap.dedent("""
    ### Tool Selection: `execute_function` vs `execute_code`

    - One exact function or primitive call is
      `execute_function(function_name="...", call_kwargs={...})`. Reach
      for `execute_code` only for shell commands or genuine multi-step
      composition (branching, loops, combining intermediate results); a
      `print()`, `await handle.result()`, or temporary variable around a
      single call is boilerplate, not composition.
    - **Handle adoption:** `execute_function` structurally guarantees the
      returned handle is exposed to the outer loop for steering (ask,
      stop, pause, resume). Inside `execute_code` a handle is only
      adopted when it is the **last expression** — never consume a handle
      inside a code block (print it, await-and-discard it) when the loop
      needs steering.
    - Durable knowledge claims are **not** primitives — use the
      KnowledgeManager JSON tools (`KnowledgeManager_search`,
      `KnowledgeManager_add_knowledge`, …) directly.

    ### Responding to a steering checkpoint

    A running block suspends when a correction reaches it, and you get a
    turn carrying the interjection and a progress report. Work already
    done has already happened — a replacement block must not repeat it.
    `stop_<tool>_<call_id>` abandons the block (choose when the correction
    changes the *remaining* work — an irreversible step the correction was
    meant to prevent is worse than a discarded plan);
    `interject_<tool>_<call_id>` resumes it as written, the text available
    via `steering.messages` (choose when the remaining work is unchanged).
    Do not stop on every interjection either. When a correction concerns
    work already running in `primitives.*` handles, route it via
    `handle.interject(...)` rather than restarting the plan.

    ### Execution Surface: where code runs

    `execute_code` runs on the **local** host by default; pass `surface`
    to run elsewhere:

    - `surface="local"` (default) — the only surface with stateful sessions
      and venvs.
    - `surface="assistant_desktop"` — your managed VM.
    - `surface="user_desktop"` — the user's own **personal machine**. Only
      use it when that user has linked it and has clearly asked you to act
      on it (pass `user_id` when more than one is linked);
      confirm with the user before running anything that changes their
      system. **To read, fetch, or "sync" their files, always use
      `primitives.computer.user_desktop.files` (list/pull/push)** — it
      mirrors their home into `~/Unity/Remote/<user_id>/` and returns local
      paths. Never retrieve their file content via shell commands on this
      surface (no `cat`/`find`/`tar`/`base64`/`cp`/`scp`/`rclone`) — shell
      there is only for commands the user explicitly wants run on their
      machine. Access is separately gated by the user's Console consent and
      can be revoked mid-run, so prompt-level permission alone is never
      sufficient.

    Remote surfaces are **stateless one-shots**: do not pass a non-stateless
    `state_mode`, `session_id`, `session_name`, or `venv_id`.
""").strip()

_MANAGER_PRIMITIVE_SCOPE = textwrap.dedent("""
    ### Manager Primitive Scope

    `primitives.*` manager calls run as the current assistant: reads and
    writes resolve through this assistant's manager scope even when an
    instruction mentions another assistant. Do not use current-assistant
    primitives or JSON manager tools to create, mutate, or "assign"
    durable artifacts another assistant must own or execute — use an
    explicit cross-assistant handoff tool if present, otherwise explain
    the limitation or ask. Do not peek into another assistant's private
    contexts: shared tabular data is already readable via
    `primitives.data.*` team-scoped fan-out.

    **Python-first principle:** prefer Python packages over shell CLI
    tools. Packages install via the `install_python_packages` JSON tool
    with isolated venvs and dependency resolution; there is no
    `install_shell_packages`. Reserve shell for tasks that genuinely
    require it.
""").strip()

_EXECUTION_RULES = textwrap.dedent("""
    ### Execution Rules

    1. **Sessions**: default is `state_mode="stateless"`; `"stateful"`
       persists intermediate variables across calls, `"read_only"` sees an
       existing session without persisting. Discover sessions with
       `list_sessions()` / `inspect_state()`.

    2. **Async parallelism**: never wrap work in bare `asyncio.run(...)`
       — the runtime already owns a loop; a sync façade uses the injected
       `run_coro_sync(factory)` helper instead. Run independent I/O
       concurrently with `asyncio.gather` / `TaskGroup` rather than a
       serial per-item `await` loop; bound fan-out with a semaphore for
       rate-limited APIs.

    3. **Structured outputs**: define Pydantic models in the code and
       call `model_rebuild()` on the outermost model.

    4. **Notifications**:
       - The user hears **only** what you send through the
         `send_notification` tool — surface progress with it between
         steps (concrete, user-facing, high-level; no filler, no internal
         diagnostics), and when the whole task is done give the final
         answer as a tool-less assistant message, never as a notification.
       - Set `completed=True` only when the described work is
         **verifiably finished**. Notifications surfacing a blocker or
         requesting user action are in-progress — send them **before**
         entering the wait loop (a notification gated on the blocked step
         deadlocks).

    5. **Verify outcomes against evidence**: after a mutation or
       extraction, confirm the outcome from real evidence (return values,
       screenshots, a re-read) — a step that ran is not a step that
       worked. If the result rests on an unverified choice between
       plausible alternatives, request clarification; if the evidence
       contradicts the result, fix and re-run.

    6. **Final answer**: when the request is fully addressed, you **MUST**
       provide the final answer directly as a tool-less assistant message
       — never via a tool call. End it with a brief **Uncertainties**
       section listing the judgment calls you were least confident about
       (only decisions that could materially affect the output).

    7. **Data provenance — never present model knowledge as sourced
       data**: when an external source fails, do **not** fill the gap with
       realistic-looking records generated from memory — fabricated
       records look authoritative but cannot be verified. Report the
       source unavailable and offer alternatives; model-knowledge context
       must be labelled as such, never formatted as sourced records.

    8. **Proactive clarification**: when `request_clarification` is
       available and a decision about user data is consequential (the user
       would have to review and undo mistakes), prefer asking over
       guessing — after initial exploration, after a small representative
       batch, or on precedent-setting ambiguous cases; never about trivial
       choices.
""").strip()


def _build_sandbox_environment_section() -> str:
    """One table of the actually injected sandbox globals + query_llm doctrine.

    The globals table mirrors ``create_execution_globals()``
    (``unify/function_manager/execution_env.py``) plus the per-execution
    ``display`` injection (``unify/actor/execution/session.py``) — if a
    global is added or removed there, update the table. Full contracts
    live in the callables' docstrings behind ``help(...)``; signatures
    are introspected so this block never drifts from the callables.
    """
    import inspect as _inspect

    from unify.common.reasoning import list_llms, query_llm

    query_prefix = "async def " if _inspect.iscoroutinefunction(query_llm) else "def "
    query_signature = (
        f"{query_prefix}{query_llm.__name__}{_inspect.signature(query_llm)}"
    )
    list_signature = f"def {list_llms.__name__}{_inspect.signature(list_llms)}"

    return textwrap.dedent(f"""
        ### Sandbox Environment

        Python in `execute_code` and stored functions runs with the
        injected globals below. Find primitive methods with the
        `FunctionManager_search_functions` JSON tool, then read live docs
        in-sandbox with `help(...)` — do not guess signatures. `help` and
        `dir` are builtins; `import inspect` first for
        `inspect.signature(...)`.

        | Global | What it is |
        |--------|------------|
        | `primitives` | Manager/computer domains (`primitives.tasks`, …); `help(primitives.<manager>.<method>)` reads live method docs |
        | `display` | `display(obj)` emits rich output — use it over `print(...)` for images; whatever you `display()` (screenshots included) comes back as visual input next turn — inspect it directly, no separate vision/observe call |
        | `query_llm` / `list_llms` | Semantic LLM calls from code (doctrine below); full contract `help(query_llm)`, endpoints `list_llms()` |
        | `get_oauth_access_token` | Connected-account OAuth handle for proxy REST; always injected, documented in full when workspace OAuth is connected — `help(get_oauth_access_token)` |
        | `run_coro_sync` | Drives a coroutine factory from a sync façade under the already-running loop |
        | `unillm` | Advanced direct LLM usage beyond `query_llm` |
        | `SteerableToolHandle` | Handle type manager calls return; make it the last expression to hand steering to the outer loop |

        ```python
        {query_signature}
        {list_signature}
        ```

        When to use `query_llm(...)` vs plain code:

        - Keep exact substeps deterministic: lookups, primitive calls,
          filters, arithmetic, dedupe, reshaping — if
          exact logic is enough, keep it deterministic; no LLM call.
          "count unread emails from Alice" is exact retrieval —
          do not call query_llm(...); fetch, filter the sender, count.
        - Call `query_llm(...)` when a substep processes meaning: classify,
          extract, score, route, summarize into fields
          (unstructured -> structured) and draft, respond, rewrite,
          synthesize (unstructured -> unstructured). "Triage my inbox"
          is meaning — a `query_llm(..., response_format=...)` judgment
          per item.
        - One block may
          freely mix deterministic substeps and semantic substeps. Use
          ``query_llm(...)`` only where meaning-based judgment is doing real work.
        - Semantic downgrades are bugs — keyword ladders, regex
          classifiers, templates pretending to be judgment. A
          deterministic pre-filter may narrow the set; the judgment
          itself is a real `query_llm(...)` call.
        - Pass a Pydantic `response_format=` (and `temperature=0.0`) when
          downstream Python branches on the result; images via
          `images=[...]`. For reuse, keep the query_llm(...) call
          inside the stored function and choose `model=` deliberately.
    """).strip()


_INCREMENTAL_EXECUTION = textwrap.dedent("""
    ### Incremental Execution

    Granularity follows predictability.

    **Deterministic work** — pure computation, data transforms, file I/O
    with known schemas — should run in a single `execute_code` block; do
    not fragment code you are confident will run correctly end-to-end.

    **Uncertain interactions** — browser automation, UI clicks, unfamiliar
    APIs, coordinate-based actions, web scraping — should be broken into
    small steps with verification between each.

    **Judgment-heavy operations** — bulk classification, labeling, or
    triaging of user data — need the same incremental caution even when
    the code itself is straightforward: the *decision* per item is
    subjective and error-prone. Study the existing data first (the user's
    historical patterns are the ground truth); process a 5–10 item batch,
    review, and (if `request_clarification` is available) confirm the
    approach before scaling; when uncertain about an item, leave it
    untouched rather than guess wrong.

    For uncertain / interactive work: one meaningful action per call,
    reviewed before the next; use `state_mode="stateful"` so
    intermediate results persist. **Verify before scaling**: run a loop
    body once and confirm the result before generalizing to iteration.
    **Read-only for exploration**: branch off known-good state with
    `state_mode="read_only"` to try alternatives without risk. Print or
    display key outputs after each uncertain step — don't assume
    success.
""").strip()

_STORAGE_DEFERRED_NOTICE = textwrap.dedent("""
    ### Skill Storage

    You can proactively store reusable skills at any point with the
    `store_skills` tool — useful after a complex subtask that discovered
    non-obvious configuration or composition strategies, when the user
    explicitly asks to store a skill, or before transitioning phases. A
    dedicated skill-consolidation process also reviews your full trajectory
    automatically after you return your result, so `store_skills` is a
    judgment call, not a routine step — skip it for trivial operations.

    **Direct writes vs trajectory storage**: user-requested "remember
    this" writes go directly to the libraries
    (`GuidanceManager_add_guidance`, `KnowledgeManager_add_knowledge`
    after search, `FunctionManager_add_functions` / `_delete_function`).
    `store_skills` extracts reusable implementations, compositional
    strategies, and durable claims from what you just did — not direct
    user-requested mutations.

    **Before compression**: when the context window nears capacity,
    `store_skills` and `compress_context` become the only tools available.
    Call `store_skills` first (with a specific request) if the trajectory
    holds unstored skills worth preserving; otherwise go straight to
    `compress_context`.
""").strip()

_TASK_SCHEDULING = textwrap.dedent("""
    ### Durable Scheduled And Triggered Tasks

    When the user asks for work to happen later, repeatedly, or in
    response to future inbound events, represent that durable intent
    with `primitives.tasks.update(...)` — every durable task mutation,
    including one-shot creates and edits, is one natural-language `text`
    request ("Create a task named X with description Y", "Repeat this
    every Monday at 12:00 UTC", "Whenever Alice emails about invoices,
    summarize it").

    A task only fires once it is **live (armed)**, never as a draft —
    say so in the create text, then verify after create with
    `primitives.tasks.ask(...)`: status armed AND trigger bindings
    resolved. If the primitive confirmed the fields, report them — do
    not claim failure or uncertainty.

    Full procedure — task types, schedules, trigger and provider-event
    binding resolution, entrypoints, `execute(task_id)` runs:
    `GuidanceManager_search` "creating verifying arming and running
    durable scheduled triggered and provider-event tasks".
""").strip()


_EXTERNAL_APP_INTEGRATION = textwrap.dedent("""
    ### External App Integration

    Integrating an external service (cloud storage, CRM, comms,
    accounting, …): check stored credentials with
    `primitives.secrets.ask(...)` — if absent, tell the caller to
    connect the service from the **Integrations** tab in the console —
    then install the official Python SDK via `install_python_packages`
    and integrate. Static keys sync to `.env` (`os.environ`);
    connected-account OAuth REST goes through the local workspace proxy
    via `get_oauth_access_token` — see `help(get_oauth_access_token)`.

    Provider calls return one final result envelope (`ok`,
    `connect_required`, `confirmation_required`, `missing_scope`,
    `provider_error`, …) — handle it, and add **no custom retry/sleep
    loops** (transient failures already retry inside
    `primitives.integrations.*` / `execute_tool`; long domain waits are
    the only exception).

    **OAuth scope check**: when a granted-scopes secret exists
    (`GOOGLE_GRANTED_SCOPES` / `MICROSOFT_GRANTED_SCOPES`), check that
    the scope the API call requires is granted before calling
    (Microsoft scopes are stored URL-prefixed). Secret missing entirely
    → proceed normally; scope absent → do not attempt the call — tell
    the user to reconnect the service from the **Integrations** tab in
    the console with the missing access.

    **Sharing links resolve through the API, never through their web page.**
    For a OneDrive/SharePoint sharing URL, encode it into a share token
    and walk the Graph shares surface through the proxy — do not fetch
    the link or scrape its HTML. Full walkthrough (share tokens,
    redemption, fallbacks, Google links): `GuidanceManager_search`
    "reading OneDrive SharePoint sharing links workspace".

    Full walkthrough (proxy base URLs, allowlist masking, mailbox
    routing, storing reusable integrations): `GuidanceManager_search`
    "integrating external apps credentials OAuth envelopes".
""").strip()


_FAST_PATH_AWARENESS = textwrap.dedent("""
    ### Fast-Path Awareness

    During interactive screen-share sessions, the outer process may handle
    simple computer actions (browser navigation, clicks, scrolls) via fast
    paths instead of routing them through you — visible as interjections
    tagged `[Fast-path request]` / `[Fast-path result]`.

    Monitor them and escalate when the fast path is out of its depth: its
    result indicates failure or confusion, the task falls within guidance
    you have loaded, or it needs capabilities the fast path lacks (stored
    credentials via `${SECRET_NAME}` injection, multi-step procedures,
    structured extraction). Escalate with a `send_notification` message
    starting `"Escalation:"`, then **proceed with execution** — do not wait
    for permission; the outer process coordinates from your notification.
    Do not intervene when simple atomic actions complete successfully.
""").strip()


def _build_filesystem_context() -> str:
    from pathlib import Path

    from unify.file_manager.settings import get_local_root

    resolved = get_local_root()
    remote_mirror = Path(resolved).parent / "Remote"  # sibling of the workspace
    return textwrap.dedent(f"""
        ### Filesystem Context

        This is the **local (pod) workspace** used by `execute_code` and by
        attachment send/receive — not the managed VM desktop filesystem.
        Your working directory is `{resolved}`.  It **persists across
        every interaction** with the user.  **Always use full absolute
        paths** (starting with `{resolved}/`); never relative paths.

        GUI files on the managed desktop live under `/Unity/...` (home
        `HOME=/Unity`, Downloads `/Unity/Downloads`, synced tree
        `/Unity/Local`) — Computer Control paths only;
        do not treat them as this pod workspace's cwd or open them with
        ordinary local file IO (see Computer Control →
        Managed desktop filesystem).  Do not treat the desktop panel name
        `unityuser` as `/home/unityuser` — that is not the desktop home.

        | Location | Purpose |
        |----------|---------|
        | `{resolved}/Attachments/` | **Inbound & Outbound** — exchanged attachments as `{{attachment_id}}_{{filename}}`. Persists across sessions. |
        | `{resolved}/Outputs/` | **Outbound staging** — save generated files here so the caller can attach and send them. May be auto-cleared between sessions. |
        | `{resolved}/Screenshots/User/`, `.../Assistant/`, `.../Webcam/` | Auto-captured frames (screen share / assistant desktop / webcam). Read-only, cleared between sessions. |
        | `{remote_mirror}/<user_id>/` | **Linked user-desktop mirror** — staged copy of a linked user's home, populated by `primitives.computer.user_desktop.files.pull`. Read/parse here; never hand-copy files in via shell `cp`/`scp`/`rclone`. |
        | `{resolved}/.env` | Environment secrets managed by SecretManager. |
        | Everything else | Your own persistent workspace — organize however makes sense. |

        **File conventions:**
        - **Inbound**: attachments arrive at `{resolved}/Attachments/{{id}}_{{filename}}`.
        - **Outbound**: save files for the user to `{resolved}/Outputs/` and
          include the full path in your final answer; once sent, the file is
          copied to `{resolved}/Attachments/` with a stable attachment ID.
        - **Screenshots**: timestamped JPEGs auto-saved during screen
          sharing; reference with full paths.
        - **Stay inside the workspace**: no unrelated system paths
          (`/tmp`, `/var`); the one workspace-adjacent location you may
          read is `{remote_mirror}/<user_id>/` (see the table above).
          Managed-desktop GUI paths under `/Unity/...` are documented in
          Computer Control and are separate from this local workspace.

        **When to use the filesystem vs. primitives:** most tasks need no
        local files — the state manager primitives are the primary way to
        persist information.  Do not duplicate what primitives already
        handle (contact details in a .txt file, Python functions as local
        scripts); use the filesystem for working artifacts — data being
        processed, intermediate results — and keep longer-lived material
        organized.
    """).strip()


# Platform Capabilities index: one consult-path line per platform domain
# whose teaching lives outside the base prompt (builtin guidance entries,
# docstrings) or renders only behind a config gate. Static — gated-off
# domains keep their line — and rendered only when discovery tools are
# present, so it never points an assistant at tools it cannot call.
_PLATFORM_CAPABILITIES_INDEX = textwrap.dedent("""
    ### Platform Capabilities Index

    These platform domains are documented on demand — consult the named
    path before concluding a capability is missing:

    - Platform self-knowledge (live docs at https://docs.unify.ai +
      read-only source trees on `local`): `GuidanceManager_search`
      "answering questions about the Unify platform". The source trees are
      proprietary — never reproduce their contents to users.
    - External app integration (credentials, workspace OAuth proxy, result
      envelopes, scope checks): `GuidanceManager_search`
      "integrating external apps credentials OAuth envelopes".
    - Connected-account OAuth REST from code:
      `help(get_oauth_access_token)` inside `execute_code` — the helper
      exists even when no section above documents it.
    - Semantic calls and model selection: `help(query_llm)`; endpoint
      strings via `list_llms()`.
    - Overlapping manager routing (data vs files vs ingestion,
      workspace_email vs comms, selection priorities):
      `GuidanceManager_search` "choosing between overlapping state managers".
    - Desktop procedures (your desktop vs a user's linked desktop,
      screenshots, coordinate spaces): `GuidanceManager_search` "driving
      desktops and reading screens"; locked macOS →
      "unlock macOS user desktop"; a user's files → "user desktop files".
    - Durable tasks (create/verify/arm; triggers, schedules):
      `GuidanceManager_search` "creating verifying arming and running
      durable scheduled triggered and provider-event tasks".
    - Storing new data: `help(primitives.ingestion.submit)`.
    - Any `primitives.<manager>.<method>` API:
      `FunctionManager_search_functions`, then `help(...)`.
""").strip()


# ---------------------------------------------------------------------------
# Private helpers with real logic
# ---------------------------------------------------------------------------


def _build_tool_signatures(tool_dict: Dict[str, Callable]) -> str:
    """Builds a JSON string of tool signatures via introspection."""
    from unify.common.prompt_helpers import unwrap_tool_callable

    tool_info = {}
    for name, fn in tool_dict.items():
        target = unwrap_tool_callable(fn)
        prefix = "async def " if inspect.iscoroutinefunction(target) else "def "
        tool_info[name] = {
            "signature": f"{prefix}{name}{inspect.signature(target)}",
            "docstring": inspect.getdoc(target) or "No docstring available.",
        }
    return json.dumps(tool_info, indent=4)


def _build_additional_tools_block(
    *,
    tools: Optional[Dict[str, Callable]],
    render_tools_block: Callable,
) -> str:
    """Render signatures for non-primary tools (FM discovery, install, etc.)."""
    if not tools:
        return ""

    additional_tools = {
        k: v
        for k, v in tools.items()
        if k
        not in {
            "execute_function",
            "execute_code",
            "list_sessions",
            "inspect_state",
            "close_session",
            "close_all_sessions",
        }
    }
    if not additional_tools:
        return ""

    return (
        f"#### Additional Tools\n"
        f"These tools are called via **structured JSON tool calls**, NOT inside Python code.\n\n"
        f"{render_tools_block(additional_tools)}"
    )


def _build_code_act_rules_and_examples(
    *,
    environments: Mapping[str, "BaseEnvironment"],
) -> str:
    """
    Builds the reusable environment rules block for CodeAct-style execution.

    Composes environment-provided prompt context plus the task-scheduling
    and fast-path sections gated on the exposed primitive tools.
    """
    parts: list[str] = []

    # Each environment provides its own rules, docs, and examples.
    for _ns, env in environments.items():
        env_ctx = env.get_prompt_context()
        if env_ctx and env_ctx.strip():
            parts.append(env_ctx)

    env = environments.get("primitives")
    if env is not None:
        _has_computer = any(
            k.startswith("primitives.computer.") for k in env.get_tools()
        )
        _has_tasks = any(k.startswith("primitives.tasks.") for k in env.get_tools())
        if _has_tasks:
            parts.append(_TASK_SCHEDULING)
        if _has_computer:
            parts.append(_FAST_PATH_AWARENESS)

    return "\n\n---\n\n".join(p for p in parts if p and p.strip()).strip()


def build_code_act_prompt(
    *,
    environments: Mapping[str, "BaseEnvironment"],
    tools: Optional[Dict[str, Callable]] = None,
    can_store: bool = False,
    guidelines: Optional[str] = None,
    discovery_first_policy: bool = False,
    include_external_app_integration: bool = True,
    include_oauth_helper: bool = True,
) -> str:
    """Build the system prompt for the CodeActActor.

    Assembles prompt sections in a fixed order, skipping sections that
    don't apply to the current configuration. This is intentionally a
    pure prompt builder (no side effects). Gating keys on assistant
    config only — never on task text — so the prompt stays a pure
    function of config and the cache prefix stays stable.

    Parameters
    ----------
    discovery_first_policy:
        When ``True``, appends guidance explaining the discovery-first tool
        policy (FM, GM, and KM must be called before other tools unlock).
    include_external_app_integration:
        When ``True`` (default, today's behavior), renders the
        ``External App Integration`` section. Callers gate this on the
        presence of integration packages
        (``unify.integration_status.discovery.discover_available_packages``).
        When gated off, the Platform Capabilities Index line still names
        the consult path.
    include_oauth_helper:
        When ``True`` (default, today's behavior), renders the
        ``OAuth Access Token Helper`` section. Callers gate this on
        workspace-OAuth connection presence
        (``unify.common.runtime_oauth.has_workspace_oauth_connection``).
        Independent of ``include_external_app_integration`` — a
        workspace-email assistant with zero integration packages keeps
        the OAuth section. When gated off, the index line remains.
    """
    from unify.common.prompt_helpers import render_tools_block

    has_execute_code = bool(tools and "execute_code" in tools)
    has_fm_tools = bool(
        tools and any(str(k).startswith("FunctionManager_") for k in tools.keys()),
    )
    has_gm_tools = bool(
        tools and any(str(k).startswith("GuidanceManager_") for k in tools.keys()),
    )
    has_km_tools = bool(
        tools and any(str(k).startswith("KnowledgeManager_") for k in tools.keys()),
    )
    has_wm_tools = bool(
        tools and any(str(k).startswith("WorkflowManager_") for k in tools.keys()),
    )

    additional_tools_block = _build_additional_tools_block(
        tools=tools,
        render_tools_block=render_tools_block,
    )

    rules_and_examples = _build_code_act_rules_and_examples(
        environments=environments,
    )

    parts: list[str] = []

    if has_execute_code:
        # Sections are ordered static → dynamic so the stable core forms a
        # cache-friendly prefix: role, contracts, execution semantics, and
        # selection rules first (identical across assistants of a
        # deployment), then per-assistant/per-session content (environment
        # scope, filesystem paths, workflows, guidelines) at the tail.
        parts.append(
            "### Role\n\n"
            "You are an expert agent that solves tasks by writing and executing code. "
            "Your primary tool is a multi-language, multi-session execution environment "
            "for running Python and shell code with access to injected tool domains.",
        )

        primary_names = [
            "execute_function",
            "execute_code",
            "list_sessions",
            "inspect_state",
            "close_session",
            "close_all_sessions",
        ]
        primary_tools = {k: tools[k] for k in primary_names if k in tools}
        primary_sigs = _build_tool_signatures(primary_tools) if primary_tools else "{}"

        tools_section = (
            "### Tools\n\n"
            "#### Execution & Session Tools\n"
            "These tools are called via **structured JSON tool calls**, NOT inside Python code.\n\n"
            f"```json\n{primary_sigs}\n```"
        )
        if additional_tools_block:
            tools_section += f"\n\n{additional_tools_block}"
        parts.append(tools_section)

        parts.append(_build_sandbox_environment_section())
        parts.append(_TOOL_SELECTION_AND_SURFACES)
        parts.append(_MANAGER_PRIMITIVE_SCOPE)
        parts.append(_EXECUTION_RULES)
        if include_oauth_helper:
            from unify.common.runtime_oauth import get_oauth_prompt_context

            parts.append(get_oauth_prompt_context())
        parts.append(_INCREMENTAL_EXECUTION)
        if include_external_app_integration:
            parts.append(_EXTERNAL_APP_INTEGRATION)

        if has_fm_tools or has_gm_tools or has_km_tools:
            parts.append(_PLATFORM_CAPABILITIES_INDEX)
            parts.append(_FUNCTION_GUIDANCE_AND_KNOWLEDGE_LIBRARY)
            if discovery_first_policy:
                parts.append(_DISCOVERY_FIRST_POLICY)

        if can_store:
            parts.append(_STORAGE_DEFERRED_NOTICE)

        # ── Per-assistant / dynamic tail ──
        parts.append(_build_filesystem_context())

        if rules_and_examples:
            parts.append(rules_and_examples)

        if has_wm_tools:
            parts.append(_WORKFLOW_SHELF)

        if guidelines:
            parts.append(
                f"### Guidelines\n\n"
                f"Follow these guidelines throughout this session:\n\n"
                f"{guidelines}",
            )

    else:
        parts.append(
            "### Role\n\n"
            "You are an expert agent that solves tasks by discovering and executing "
            "pre-stored functions from a function library. "
            "You do NOT write or execute arbitrary code. Instead, you use the "
            "FunctionManager discovery tools to find relevant stored functions, "
            "then invoke them via `execute_function`.",
        )

        if guidelines:
            parts.append(
                f"### Guidelines\n\n"
                f"Follow these guidelines throughout this session:\n\n"
                f"{guidelines}",
            )

        if has_fm_tools or has_gm_tools or has_km_tools:
            parts.append(_FUNCTION_GUIDANCE_AND_KNOWLEDGE_LIBRARY)
            if discovery_first_policy:
                parts.append(_DISCOVERY_FIRST_POLICY)

        if has_wm_tools:
            parts.append(_WORKFLOW_SHELF)

        procedure = (
            "### Procedure\n\n"
            "1. **Discover** stored functions using `FunctionManager_search_functions`,\n"
            "   `FunctionManager_filter_functions`, or `FunctionManager_list_functions`.\n"
            "2. **Execute** via `execute_function` — a stored match from discovery,\n"
            "   or a prompt-documented callable by exact name (see Discovery index\n"
            "   scope above).\n"
            "3. Report inability only when neither applies — do NOT write or compose\n"
            "   code yourself."
        )
        if additional_tools_block:
            procedure += f"\n\n{additional_tools_block}"
        parts.append(procedure)

        if rules_and_examples:
            parts.append(rules_and_examples)

    return "\n\n".join(p for p in parts if p and p.strip())
