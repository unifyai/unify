Knowledge Manager Sandbox
=========================

Interactive playground for the typed `KnowledgeManager` claim ledger
(`unify/knowledge_manager/`). Seed durable claims, run typed CRUD, and inspect
outcomes before wiring Knowledge into the Actor / ConversationManager.

What is the `KnowledgeManager`?
-------------------------------
`KnowledgeManager` stores durable **typed claims** (facts, policies, definitions,
decisions, constraints, insights, preferences) with provenance (`source_refs`)
and lifecycle status (active / superseded / invalidated).

It exposes first-class methods (also available as Actor JSON tools
`KnowledgeManager_*` — **not** `primitives.knowledge.*`):

* **`search` / `filter` / `get_knowledge`** — read claims
* **`add_knowledge` / `update_knowledge` / `delete_knowledge`** — write claims
* **`invalidate_knowledge` / `supersede_knowledge` / `reconcile_sources`** — lifecycle
* **`clear`** — wipe the ledger (sandbox / tests)

There is no natural-language `ask` / `update` / `refactor` tool loop.

Running the sandbox
-------------------

```bash
# Basic text-only session
python -m sandboxes.knowledge_manager.sandbox

# Voice I/O via Deepgram + Cartesia
python -m sandboxes.knowledge_manager.sandbox --voice
```

CLI flags match the shared helpers in `sandboxes/utils.py`
(`--voice`, `--project_name`, `--overwrite`, `--project_version`,
`--log_in_terminal`, `--log_tcp_port`, `--http_log_tcp_port`).

Interactive commands
~~~~~~~~~~~~~~~~~~~~

* `us {description}` — seed claims via `ScenarioBuilder` (`add_knowledge` / `search`)
* `usv` — same with a voice description (`--voice` only)
* `search [ref=text] [k=N]` — semantic search
* `filter [filter="expr"] [offset=N] [limit=N]` — expression filter
* `get knowledge_id=N` — full claim body
* `add title=... content=... [kind=fact] [topics='["a"]'] ...` — create claim
* `update knowledge_id=N ...` — in-place field update
* `delete` / `invalidate` / `supersede` / `reconcile` — lifecycle ops
* `clear` — wipe ledger
* `save_project` / `sp` — project snapshot
* `help` / `quit`

Keyword values are parsed as Python literals when possible
(`topics='["warranty"]'`, `source_refs='[{"kind":"manual"}]'`).

### Example session

```text
$ python -m sandboxes.knowledge_manager.sandbox
command> add title="Battery warranty" content="Eight years" kind=fact topics='["warranty","tesla"]'
{
  "outcome": "knowledge created successfully",
  "details": {"knowledge_id": 1}
}

command> search ref="battery warranty" k=5
[ ... claim previews ... ]

command> get knowledge_id=1
{ "knowledge_id": 1, "title": "Battery warranty", ... }
```

Scenario generation
-------------------
`us` / `usv` build synthetic claims through the public typed tools. Prefer
`source_refs` with `kind=manual` (or transcript/file when relevant).

Troubleshooting
---------------
* Voice mode needs `DEEPGRAM_API_KEY` and `CARTESIA_API_KEY`.
* Backend access needs `UNIFY_KEY` / `ORCHESTRA_URL`.
* Optional: `UNITY_KNOWLEDGE_IMPL` selects real vs simulated KnowledgeManager
  via the manager registry (same as other sandboxes).
