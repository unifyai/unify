import logging
from typing import List, Dict, Optional, Callable, Any, Tuple, Type, Union
from pydantic import BaseModel
import asyncio
import functools
import re

_log = logging.getLogger(__name__)
logger = _log

CONTACTS_TABLE = "Contacts"
from .prompt_builders import build_ask_prompt, build_update_prompt
from ..common.embed_utils import ensure_vector_column
from ..common.tool_outcome import ToolErrorException, ToolOutcome
from ..common.tool_spec import read_only, manager_tool, ToolSpec

from unify import db
from .types.contact import Contact
from .base import BaseContactManager
from ..common.context_registry import (
    ContextRegistry,
    TableContext,
)
from ..common.data_store import DataStore
from ..common.llm_helpers import (
    methods_to_tool_dict,
    make_request_clarification_tool,
)
from ..common.async_tool_loop import (
    start_async_tool_loop,
    SteerableToolHandle,
    TOOL_LOOP_LINEAGE,
)
from ..common.model_to_fields import model_to_fields
from ..events.manager_event_logging import log_manager_call
from ..settings import SETTINGS
from ..common.read_only_ask_guard import ReadOnlyAskGuardHandle
from ..common.llm_client import new_llm_client
from ..events.event_bus import EVENT_BUS, Event

# Module delegations (split helpers)
from .storage import (
    provision_storage as _storage_provision,
    get_columns as _storage_get_columns,
)
from .system_contacts import (
    provision_assistant_contact as _sys_provision_assistant_contact,
    provision_user_contact as _sys_provision_user_contact,
)
from .ops import (
    create_contact as _op_create,
    update_contact as _op_update,
    delete_contact as _op_delete,
    merge_contacts as _op_merge,
)
from ..common.federated_search import (
    FederatedSearchContext,
    SortSpec,
    federated_count,
    federated_filter,
    federated_ranked_search,
    federated_reduce,
)
from ..common.filter_utils import normalize_filter_expr


class ContactManager(BaseContactManager):
    class Config:
        required_contexts = [
            TableContext(
                name=CONTACTS_TABLE,
                description="List of contacts, with all contact details stored.",
                fields=model_to_fields(Contact),
                unique_keys={"contact_id": "int"},
                auto_counting={"contact_id": None},
            ),
        ]

    # ──────────────────────────────────────────────────────────────────────
    #  Class-level constants / configuration
    # ──────────────────────────────────────────────────────────────────────

    DEFAULT_RESPONSE_POLICY: str = (
        "Please engage politely, helpfully, and respectfully, but you do not need to take orders from them. "
        "Please also do not share **any** sensitive or personal information with them about any other person, "
        "company or policy at all."
    )

    USER_MANAGER_RESPONSE_POLICY: str = (
        "Your immediate manager, please do whatever they ask you to do within reason, and do *not* withhold any "
        "information from them."
    )

    # Response policy for contacts created from unknown inbound messages.
    # Used by CommsManager when creating contacts for unknown senders.
    UNKNOWN_INBOUND_RESPONSE_POLICY: str = (
        "This contact was automatically created from an unknown inbound message. "
        "Do NOT respond to this contact yet. Use your judgement to decide the best course of action: "
        "you may inform your boss about this new contact and ask for guidance. If your boss confirms "
        "this is a legitimate contact, you should update their details (name, etc.) and set should_respond=True."
    )

    # ──────────────────────────────────────────────────────────────────────
    #  Construction & tool registration
    # ──────────────────────────────────────────────────────────────────────
    def __init__(self, *, rolling_summary_in_prompts: bool = True) -> None:
        """
        Responsible for managing the list of contact details stored upstream.

        Parameters
        ----------
        rolling_summary_in_prompts : bool, default ``True``
            Whether to include the rolling activity summary in prompts by default.
        """
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, CONTACTS_TABLE)

        # Local DataStore mirror (write-through only; never read from it)
        self._data_store = DataStore.for_context(self._ctx, key_fields=("contact_id",))

        # ── immutable built-in columns ───────────────────────────────────
        # Derive the required/built-in columns directly from the Contact model so
        # that there is a single source-of-truth for field names across the
        # code-base.  Any future change to the Contact schema will
        # automatically propagate here.
        self._BUILTIN_FIELDS: Tuple[str, ...] = tuple(Contact.model_fields.keys())
        self._REQUIRED_COLUMNS: set[str] = set(self._BUILTIN_FIELDS)

        # ── public tool dictionaries ─────────────────────────────────────
        # ask-side tools are read-only, so they never change
        ask_tools: Dict[str, Callable] = {
            **methods_to_tool_dict(
                ToolSpec(fn=self._list_columns, display_label="Listing contact fields"),
                ToolSpec(fn=self.filter_contacts, display_label="Filtering contacts"),
                ToolSpec(fn=self._search_contacts, display_label="Searching contacts"),
                ToolSpec(fn=self._reduce, display_label="Summarising contact data"),
                include_class_name=False,
            ),
        }
        self.add_tools("ask", ask_tools)

        # update-side tools can read and write
        update_tools: Dict[str, Callable] = {
            **methods_to_tool_dict(
                ToolSpec(fn=self.ask, display_label="Querying contact book"),
                ToolSpec(
                    fn=self._create_contact,
                    display_label="Creating a new contact",
                ),
                ToolSpec(fn=self.update_contact, display_label="Updating a contact"),
                ToolSpec(fn=self._delete_contact, display_label="Deleting a contact"),
                ToolSpec(
                    fn=self._merge_contacts,
                    display_label="Merging duplicate contacts",
                ),
                include_class_name=False,
            ),
        }
        self.add_tools("update", update_tools)

        # rolling activity inclusion flag
        self._rolling_summary_in_prompts = rolling_summary_in_prompts

        # Ensure context/schema exist
        self._provision_storage()

        # Ensure assistant self and boss contacts exist and are up to date.
        self._sync_required_contacts()

    def _pack_contacts(self, contacts: list[Contact]) -> Dict[str, Any]:
        """Return the standard ContactManager tool payload for contact rows."""

        if not contacts:
            return {"contacts": []}
        return {
            "contact_keys_to_shorthand": Contact.shorthand_map(),
            "contacts": contacts,
            "shorthand_to_contact_keys": Contact.shorthand_inverse_map(),
        }

    # ──────────────────────────────────────────────────────────────────────
    #  Public API (English-only entrypoints for the LLM)
    # ──────────────────────────────────────────────────────────────────────
    @functools.wraps(BaseContactManager.ask, updated=())
    @manager_tool
    @log_manager_call(
        "ContactManager",
        "ask",
        payload_key="question",
        display_label="Checking contact book",
    )
    async def ask(
        self,
        text: str,
        *,
        response_format: Optional[Type[BaseModel]] = None,
        _return_reasoning_steps: bool = False,
        _parent_chat_context: Optional[List[Dict[str, Any]]] = None,
        _clarification_up_q: Optional[asyncio.Queue[str]] = None,
        _clarification_down_q: Optional[asyncio.Queue[str]] = None,
        rolling_summary_in_prompts: Optional[bool] = None,
        _call_id: Optional[str] = None,
    ) -> SteerableToolHandle:
        client = new_llm_client()

        # Build a *live* tools-dict so the prompt never hard-codes
        # either the number of tools or their names/argspecs.
        tools = dict(self.get_tools("ask"))
        _clar_queues = None
        _on_clar_req = None
        _on_clar_ans = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

            async def _on_clar_req(q: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "ContactManager",
                                "method": "ask",
                                "action": "clarification_request",
                                "question": q,
                            },
                        ),
                    )
                except Exception:
                    pass

            async def _on_clar_ans(ans: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "ContactManager",
                                "method": "ask",
                                "action": "clarification_answer",
                                "answer": ans,
                            },
                        ),
                    )
                except Exception:
                    pass

        include_activity = (
            self._rolling_summary_in_prompts
            if rolling_summary_in_prompts is None
            else rolling_summary_in_prompts
        )

        _ask_prompt = build_ask_prompt(
            tools=tools,
            num_contacts=self._num_contacts(),
            columns=self._list_columns(),
            include_activity=include_activity,
        ).to_list()
        client.set_system_message(_ask_prompt)

        handle = start_async_tool_loop(
            client,
            text,
            tools,
            loop_id=f"{self.__class__.__name__}.{self.ask.__name__}",
            parent_lineage=TOOL_LOOP_LINEAGE.get([]),
            parent_chat_context=_parent_chat_context,
            tool_policy=self._default_ask_tool_policy,
            handle_cls=(
                ReadOnlyAskGuardHandle if SETTINGS.UNIFY_READONLY_ASK_GUARD else None
            ),
            response_format=response_format,
            clarification_queues=_clar_queues,
            on_clarification_request=_on_clar_req,
            on_clarification_answer=_on_clar_ans,
        )

        if _return_reasoning_steps:
            original_result = handle.result

            async def wrapped_result():
                answer = await original_result()
                return answer, client.messages

            handle.result = wrapped_result  # type: ignore

        return handle

    @functools.wraps(BaseContactManager.update, updated=())
    @log_manager_call(
        "ContactManager",
        "update",
        payload_key="request",
        display_label="Updating contact book",
    )
    async def update(
        self,
        text: str,
        *,
        response_format: Optional[Type[BaseModel]] = None,
        _return_reasoning_steps: bool = False,
        _parent_chat_context: Optional[List[Dict[str, Any]]] = None,
        _clarification_up_q: Optional[asyncio.Queue[str]] = None,
        _clarification_down_q: Optional[asyncio.Queue[str]] = None,
        rolling_summary_in_prompts: Optional[bool] = None,
        _call_id: Optional[str] = None,
    ) -> SteerableToolHandle:
        client = new_llm_client()

        tools = dict(self.get_tools("update"))
        _clar_queues = None
        _on_clar_req = None
        _on_clar_ans = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

            async def _on_clar_req(q: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "ContactManager",
                                "method": "update",
                                "action": "clarification_request",
                                "question": q,
                            },
                        ),
                    )
                except Exception:
                    pass

            async def _on_clar_ans(ans: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "ContactManager",
                                "method": "update",
                                "action": "clarification_answer",
                                "answer": ans,
                            },
                        ),
                    )
                except Exception:
                    pass

        include_activity = (
            self._rolling_summary_in_prompts
            if rolling_summary_in_prompts is None
            else rolling_summary_in_prompts
        )

        _upd_prompt = build_update_prompt(
            tools,
            num_contacts=self._num_contacts(),
            columns=self._list_columns(),
            include_activity=include_activity,
        ).to_list()
        client.set_system_message(_upd_prompt)
        handle = start_async_tool_loop(
            client,
            text,
            tools,
            loop_id=f"{self.__class__.__name__}.{self.update.__name__}",
            parent_lineage=TOOL_LOOP_LINEAGE.get([]),
            parent_chat_context=_parent_chat_context,
            tool_policy=self._default_update_tool_policy,
            response_format=response_format,
            clarification_queues=_clar_queues,
            on_clarification_request=_on_clar_req,
            on_clarification_answer=_on_clar_ans,
        )

        if _return_reasoning_steps:
            original_result = handle.result

            async def wrapped_result():
                answer = await original_result()
                return answer, client.messages

            handle.result = wrapped_result  # type: ignore

        return handle

    @functools.wraps(BaseContactManager.clear, updated=())
    def clear(self) -> None:
        db.delete_context(self._ctx)

        # Clear local cache so subsequent reads/writes operate against a
        # clean slate
        try:
            self._data_store.clear()
        except Exception:
            pass

        # Ensure the schema exists again via shared provisioning helper
        ContextRegistry.refresh(self, "Contacts")

        self._provision_storage()

        # Verify the context is visible before attempting reads
        try:
            import time as _time  # local import to avoid polluting module namespace

            for _ in range(3):
                try:
                    db.get_fields(context=self._ctx)
                    break
                except Exception:
                    _time.sleep(0.05)
        except Exception:
            pass

        # Recreate required assistant and boss contacts.
        self._sync_required_contacts()

    # (Optional) Public programmatic helpers (non-LLM)
    def get_contact_info(
        self,
        contact_id: Union[int, List[int]],
        fields: Optional[Union[str, List[str]]] = None,
        search_local_storage: bool = True,
    ) -> Dict[int, Dict[str, Any]]:
        """
        Return a mapping of requested fields for a single contact.

        Behaviour
        ---------
        - When search_local_storage is True, look in the local DataStore first.
          If not found, fall back to a backend read and resync the DataStore.
        - When fields is None or "all", include all allowed fields.
        - Vector/private fields are never loaded.

        Returns
        -------
        dict[int, dict]
            Mapping of contact_id → selected field→value. Missing ids are omitted.
        """
        allowed = set(self._allowed_fields())

        # Normalise requested fields
        if fields is None or (isinstance(fields, str) and fields.lower() == "all"):
            requested: List[str] = list(allowed)
        elif isinstance(fields, str):
            requested = [fields]
        else:
            requested = list(fields or [])

        # Intersect with allowed set to avoid accidental vector/private columns
        requested = [f for f in requested if f in allowed]
        if not requested:
            requested = list(allowed)

        # Normalise ids list
        if isinstance(contact_id, list):
            ids: List[int] = [int(x) for x in contact_id]
        else:
            ids = [int(contact_id)]

        results: Dict[int, Dict[str, Any]] = {}
        remaining = list(dict.fromkeys(ids))
        store = self._data_store

        misses: List[int] = []
        if search_local_storage:
            for cid in remaining:
                try:
                    row = store[cid]
                except KeyError:
                    misses.append(cid)
                else:
                    results[cid] = {k: v for k, v in row.items() if k in requested}
        else:
            misses = list(remaining)

        if misses:
            if len(misses) == 1:
                filt = f"contact_id == {misses[0]}"
            else:
                filt = f"contact_id in [{', '.join(str(x) for x in misses)}]"
            rows = db.get_logs(
                context=self._ctx,
                filter=filt,
                limit=len(misses),
                from_fields=list(allowed),
            )
            for lg in rows:
                try:
                    backend_row = lg.entries
                    cid_val = int(backend_row.get("contact_id"))
                except Exception:
                    continue
                if cid_val not in remaining:
                    continue
                try:
                    store.put(backend_row)
                except Exception:
                    pass
                results[cid_val] = {k: backend_row.get(k) for k in requested}

        return results

    # ──────────────────────────────────────────────────────────────────────
    #  Private tools (LLM-exposed to tool loops)
    #    – these are the underscore-prefixed methods you pass into add_tools
    # ──────────────────────────────────────────────────────────────────────
    # Read-only tools
    @read_only
    def _list_columns(
        self,
        *,
        include_types: bool = True,
    ) -> Dict[str, Any] | List[str]:
        """
        Return the list of available columns in the contacts table, optionally with types.

        Parameters
        ----------
        include_types : bool, default True
            Controls the shape of the returned value:
            - When True: returns a mapping ``{column_name: column_type}`` where
              ``column_type`` is a string label used by Unify (e.g. ``"str"``,
              ``"int"``, ``"bool"``, ``"list"``, ``"dict"``, ``"datetime"``).
            - When False: returns a ``set`` of column names (types omitted). This is
              useful to check for presence/absence without caring about data types.

        Returns
        -------
        Dict[str, Any] | List[str]
            - If ``include_types=True``: ``dict`` mapping column names to their types.
            - If ``include_types=False``: ``list`` of column names.

        Notes
        -----
        - Columns that store embeddings (those whose names end with ``"_emb"``)
          may exist in the backend but are not filtered out here; consumers that
          don't want to see private vector columns should filter them out
          themselves.
        - Column names follow snake_case. Built‑in columns are derived directly from
          the Pydantic ``Contact`` model and are immutable.
        """
        cols = self._get_columns()
        return cols if include_types else list(cols)

    @read_only
    def _reduce(
        self,
        *,
        metric: str,
        keys: str | list[str],
        filter: Optional[str | dict[str, str]] = None,
        group_by: Optional[str | list[str]] = None,
    ) -> Any:
        """
        Compute basic reduction metrics over the Contacts table.

        Parameters
        ----------
        metric : str
            Reduction metric to compute. Supported values (case-insensitive) are
            ``\"sum\"``, ``\"mean\"``, ``\"var\"``, ``\"std\"``, ``\"min\"``,
            ``\"max\"``, ``\"median\"``, ``\"mode\"``, and ``\"count\"``.
        keys : str | list[str]
            One or more numeric contact fields to aggregate, for example
            ``\"contact_id\"``. A single column name
            returns a scalar; a list of column names computes the metric
            independently per key and returns a ``{key -> value}`` mapping.
        filter : str | dict[str, str] | None, default None
            Optional row-level filter expression(s) in the same Python syntax as
            :py:meth:`filter_contacts`. When a string, the expression is applied
            uniformly; when a dict, each key maps to its own filter expression.
        group_by : str | list[str] | None, default None
            Optional contact field(s) to group by, for example ``\"should_respond\"``.
            Use a single column name for one
            grouping level, or a list such as ``[\"should_respond\", \"contact_id\"]``
            to group hierarchically in that order. When provided, the result
            becomes a nested mapping keyed by group values, mirroring
            :func:`db.get_logs_metric` behaviour.

        Returns
        -------
        Any
            Metric value(s) computed over the Contacts context:

            * Single key, no grouping  → scalar (float/int/str/bool).
            * Multiple keys, no grouping → ``dict[key -> scalar]``.
            * With grouping             → nested ``dict`` keyed by group values.
        """
        keys_list = [keys] if isinstance(keys, str) else list(keys)
        group_fields = [group_by] if isinstance(group_by, str) else list(group_by or [])
        result_by_key: dict[str, Any] = {}

        for key in keys_list:
            key_filter = filter.get(key) if isinstance(filter, dict) else filter
            contexts = [
                FederatedSearchContext(
                    context=self._ctx,
                    source=self._ctx,
                    allowed_fields=[key, *group_fields],
                ),
            ]
            result_by_key[key] = federated_reduce(
                contexts,
                metric=metric,
                columns=key,
                filter=normalize_filter_expr(key_filter),
                group_by=group_fields or None,
            )

        if isinstance(keys, str):
            return result_by_key[keys]
        return result_by_key

    @read_only
    def filter_contacts(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Filter contacts using a boolean Python expression evaluated per row.

        For exact, equality, inequality, membership checks and column-wise filtering (e.g. id or equality checks).

        Parameters
        ----------
        filter : str | None, default None
            A Python boolean expression evaluated with column names in scope. Examples:
            - ``"first_name == 'John' and surname == 'Doe'"``
            - ``"email_address.endswith('@company.com')"``
            - ``"email_address.endswith('@company.com')"``
            When ``None``, returns all contacts. String comparisons are case‑sensitive unless
            your expression applies a case‑normalisation. Supported grammar: comparisons
            (==, !=, <, <=, >, >=), membership tests (in / not in), and boolean combinators
            (and, or, not) over field names and literal values, plus a fixed set of helpers
            (``len()``, string methods like ``.lower()`` / ``.startswith()``, ``embed()``).
            Arbitrary Python calls outside that set — e.g. ``' '.join(x)`` or a list
            comprehension — are rejected.
        offset : int, default 0
            Zero‑based index of the first result to include.
        limit : int, default 100
            Maximum number of records to return. Must be <= 1000.

        Returns
        -------
        List[Contact]
            Matching contacts as Pydantic ``Contact`` models in creation order.

        Notes
        -----
        - Be careful with quoting inside the expression. Use single quotes to delimit string
          literals inside the filter string.
        - This tool is brittle for substring searches across text; prefer ``_search_contacts``
          for that purpose.
        """
        # Prefer a single backend call that whitelists the built‑in columns to
        # keep payloads small without a prior fields introspection request.
        # Fast-path: tighten the requested limit when the filter guarantees
        # at most a single match (unique equality) or a bounded small list.
        eff_limit = limit
        if isinstance(filter, str):
            # contact_id == <int>
            if re.fullmatch(r"\s*contact_id\s*==\s*\d+\s*", filter):
                eff_limit = min(eff_limit, 1)
            else:
                # Equality on unique fields → at most one row
                unique_eq_patterns = (
                    r"\s*email_address\s*==\s*(['\"])\S.*?\1\s*",
                    r"\s*phone_number\s*==\s*(['\"])\S.*?\1\s*",
                    r"\s*whatsapp_number\s*==\s*(['\"])\S.*?\1\s*",
                    r"\s*discord_id\s*==\s*(['\"])\S.*?\1\s*",
                    r"\s*slack_user_id\s*==\s*(['\"])\S.*?\1\s*",
                )
                if any(re.fullmatch(p, filter) for p in unique_eq_patterns):
                    eff_limit = min(eff_limit, 1)
                else:
                    # contact_id in [a, b, c] → cap at list length
                    m = re.fullmatch(
                        r"\s*contact_id\s*in\s*\[\s*([0-9,\s]+)\s*\]\s*",
                        filter,
                    )
                    if m:
                        count_ids = len(re.findall(r"\d+", m.group(1)))
                        if count_ids > 0:
                            eff_limit = min(eff_limit, count_ids)

        from_fields = list(self._BUILTIN_FIELDS)

        contexts = [
            FederatedSearchContext(
                context=self._ctx,
                source=self._ctx,
                allowed_fields=from_fields,
            ),
        ]
        try:
            # Sort server-side by contact_id so the documented creation-order
            # contract holds regardless of backend storage order, which varies
            # between sessions for identically-seeded data.
            annotated_rows = federated_filter(
                contexts,
                filter=normalize_filter_expr(filter),
                sorting=(SortSpec(field="contact_id"),),
                offset=offset,
                limit=eff_limit,
            )
        except ToolErrorException as exc:
            return exc.payload
        rows: list[dict] = []
        for annotated in annotated_rows:
            row = {
                key: value
                for key, value in annotated.items()
                if not key.startswith("_federated_")
            }
            # Write-through to the local DataStore mirror.
            try:
                self._data_store.put(row)
            except Exception:
                pass
            rows.append(row)
        return self._pack_contacts([Contact(**row) for row in rows])

    @read_only
    def _search_contacts(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> Dict[str, Any]:
        """
        Semantic search over contacts using one or more reference texts.

        Parameters
        ----------
        references : Dict[str, str] | None, default None
            Mapping of ``source_expr → reference_text`` terms that define the search space.
            - ``source_expr`` can be either a simple column name (e.g. ``"bio"``,
              ``"first_name"``) or a full Unify derived‑expression (e.g.
              ``"str({first_name}) + ' ' + str({surname})"``). For expressions, a stable
              derived source column is created automatically if needed.
            - ``reference_text`` is free‑form text which will be embedded using the
              configured embedding model.
            When ``None`` or an empty dict, semantic search is skipped and the most recent
            contacts are returned using backfill-only logic.
        k : int, default 10
            Maximum number of contacts to return. Must be a positive integer. Must be <= 1000.

        Returns
        -------
        List[Contact]
            Up to ``k`` Pydantic ``Contact`` models. When semantic references are provided,
            results are sorted by similarity (ascending cosine distance). When references
            are omitted/empty, returns the most recent contacts. Assistant self
            and boss system contacts are excluded.

        Notes
        -----
        - When a single term is provided, results are ranked by ``cosine(column_emb, ref)``.
        - When multiple terms are provided, results are ranked by the sum of cosines across
          all terms to favour contacts similar across several fields.
        """
        allowed_fields = list(self._BUILTIN_FIELDS)

        from ..session_details import SESSION_DETAILS

        system_filter = (
            f"contact_id != {int(SESSION_DETAILS.self_contact_id)} "
            f"and contact_id != {int(SESSION_DETAILS.boss_contact_id)}"
        )
        contexts = [
            FederatedSearchContext(
                context=self._ctx,
                source=self._ctx,
                row_filter=system_filter,
                allowed_fields=allowed_fields,
            ),
        ]
        rows = federated_ranked_search(
            contexts,
            references,
            limit=k,
            backfill=True,
        )

        visible_contacts: list[Contact] = []
        for row in rows:
            clean = {
                key: value
                for key, value in row.items()
                if not key.startswith("_federated_")
            }
            try:
                self._data_store.put(clean)
            except Exception:
                pass
            visible_contacts.append(Contact(**clean))
        return self._pack_contacts(visible_contacts)

    # Mutation tools
    def _create_contact(
        self,
        *,
        first_name: Optional[str] = None,
        surname: Optional[str] = None,
        email_address: Optional[str] = None,
        phone_number: Optional[str] = None,
        whatsapp_number: Optional[str] = None,
        discord_id: Optional[str] = None,
        slack_user_id: Optional[str] = None,
        bio: Optional[str] = None,
        job_title: Optional[str] = None,
        timezone: Optional[str] = None,
        rolling_summary: Optional[str] = None,
        should_respond: bool = True,
        response_policy: Optional[str] = None,
        is_system: bool = False,
        _contact_id: Optional[int] = None,
    ) -> ToolOutcome:
        """
        Create and persist a new contact.

        Parameters
        ----------
        first_name : str | None
            Given name. Allowed characters are Unicode letters/digits plus spaces,
            periods, apostrophes, and hyphens (underscores are not allowed). Optional.
        surname : str | None
            Family name (stored in the ``surname`` column). Same validation guidance as
            ``first_name``. Optional.
        email_address : str | None
            Email address. Must contain exactly one ``@`` with characters on both sides
            (basic validation). Must be unique across all contacts.
        phone_number : str | None
            Phone number. May start with ``+`` (only if explicitly provided by the user),
            otherwise digits only. Must be unique.
        whatsapp_number : str | None
            WhatsApp number. Digits only unless explicitly provided with leading ``+``.
        discord_id : str | None
            Discord snowflake id (digits only). Optional.
        slack_user_id : str | None
            Slack user id. Optional.
        bio : str | None
            Free‑form notes or description about the contact. Optional.
        job_title : str | None
            Free‑text job title / specialization (e.g. "Growth marketing",
            "QA engineer"). On the assistant self contact this mirrors the
            assistant's job title from the backend and is surfaced to the LLM
            via the broader-context prompt. Optional.
        timezone : str | None
            IANA Timezone identifier (e.g. "America/New_York"). Optional.
        rolling_summary : str | None
            Internal running summary of recent activity for this contact. Optional.
        should_respond : bool, default True
            Whether the assistant should reply to this contact by default when
            communicating in user‑facing experiences.
        response_policy : str | None
            Optional policy text that qualifies how the assistant should respond to this
            contact. When omitted, a safe default policy is automatically applied.
        is_system : bool, default False
            Mark as a system contact (assistant/user/org member). Optional.

        Returns
        -------
        ToolOutcome
            A standard outcome dict: ``{"outcome": "contact created successfully", "details": {"contact_id": <int>}}``.

        Raises
        ------
        AssertionError
            - If all provided fields are ``None`` (at least one field is required).
            - If any uniqueness constraint is violated (duplicate ``email_address``
              or ``phone_number``).

        Behaviour and Edge Cases
        ------------------------
        - New regular contacts receive the next available id. System contacts are
          provisioned separately from resolved session ids.
        - ``response_policy`` defaults to a conservative policy that avoids sharing sensitive
          information when not explicitly provided.
        - Unspecified fields remain ``None`` and can be populated later via ``update_contact``.
        """
        return _op_create(
            self,
            first_name=first_name,
            surname=surname,
            email_address=email_address,
            phone_number=phone_number,
            whatsapp_number=whatsapp_number,
            discord_id=discord_id,
            slack_user_id=slack_user_id,
            bio=bio,
            job_title=job_title,
            timezone=timezone,
            rolling_summary=rolling_summary,
            should_respond=should_respond,
            response_policy=response_policy,
            is_system=is_system,
            contact_id=_contact_id,
        )

    def update_contact(
        self,
        *,
        contact_id: int,
        first_name: Optional[str] = None,
        surname: Optional[str] = None,
        email_address: Optional[str] = None,
        phone_number: Optional[str] = None,
        whatsapp_number: Optional[str] = None,
        discord_id: Optional[str] = None,
        slack_user_id: Optional[str] = None,
        bio: Optional[str] = None,
        job_title: Optional[str] = None,
        timezone: Optional[str] = None,
        rolling_summary: Optional[str] = None,
        should_respond: Optional[bool] = None,
        response_policy: Optional[str] = None,
        is_system: Optional[bool] = None,
        _log_id: Optional[int] = None,
    ) -> ToolOutcome:
        """
        Update one or more fields of an existing contact.

        Parameters
        ----------
        contact_id : int
            The numeric identifier of the contact to modify. Must refer to exactly one
            existing contact.
        first_name : str | None
            New given name. Same validation guidance as in ``_create_contact``. Omit (leave
            as ``None``) to keep unchanged.
        surname : str | None
            New family name (stored as ``surname``). Same guidance as ``first_name``. Omit
            to keep unchanged.
        email_address : str | None
            New email address. Must be unique across all contacts and contain one ``@``.
        phone_number : str | None
            New phone number. Digits only unless explicitly provided with leading ``+``.
            Must be unique.
        whatsapp_number : str | None
            New WhatsApp number. Digits only unless explicitly provided with leading ``+``.
        discord_id : str | None
            Discord snowflake id. Optional.
        slack_user_id : str | None
            Slack user id. Optional.
        bio : str | None
            Free‑form notes/description.
        job_title : str | None
            Free‑text job title / specialization. See ``_create_contact``.
        timezone : str | None
            IANA Timezone identifier.
        rolling_summary : str | None
            Updated rolling activity summary (internal).
        should_respond : bool | None
            Whether the assistant should reply to this contact by default. Omit to leave
            unchanged.
        response_policy : str | None
            Override the contact‑specific response policy. Omit to leave unchanged.
        is_system : bool | None
            System-contact flag. Omit to leave unchanged.

        Returns
        -------
        ToolOutcome
            A standard outcome dict: ``{"outcome": "contact updated", "details": {"contact_id": <int>}}``.

        Raises
        ------
        ValueError
            - If no updatable field is provided (all parameters ``None`` except ``contact_id``).
            - If ``contact_id`` does not exist or resolves to multiple records (data integrity issue).
            - If updating to a value that violates uniqueness constraints (duplicate email/phone).

        Notes
        -----
        - Fields not supplied remain unchanged.
        - This operation overwrites the stored values for the selected fields.
        - ``contact_id`` itself cannot be changed.
        """
        return _op_update(
            self,
            contact_id=contact_id,
            first_name=first_name,
            surname=surname,
            email_address=email_address,
            phone_number=phone_number,
            whatsapp_number=whatsapp_number,
            discord_id=discord_id,
            slack_user_id=slack_user_id,
            bio=bio,
            job_title=job_title,
            timezone=timezone,
            rolling_summary=rolling_summary,
            should_respond=should_respond,
            response_policy=response_policy,
            is_system=is_system,
            _log_id=_log_id,
        )

    def _delete_contact(
        self,
        *,
        contact_id: int,
        _log_id: Optional[int] = None,
    ) -> ToolOutcome:
        """
        Permanently delete a contact.

        Parameters
        ----------
        contact_id : int
            The identifier of the contact to remove. Must refer to a non‑system contact.

        Returns
        -------
        ToolOutcome
            ``{"outcome": "contact deleted", "details": {"contact_id": <int>}}``.

        Raises
        ------
        RuntimeError
            If attempting to delete reserved system contacts: ``0`` (assistant) or ``1`` (default user).
        ValueError
            If the contact does not exist, or if multiple records share the same ``contact_id``
            (indicates data integrity issues).

        Notes
        -----
        - This operation cannot be undone. Consider ``_merge_contacts`` to consolidate records
          without losing history.
        """
        return _op_delete(
            self,
            contact_id=contact_id,
            _log_id=_log_id,
        )

    def _merge_contacts(
        self,
        *,
        contact_id_1: int,
        contact_id_2: int,
        overrides: Optional[Dict[str, int]] = None,
    ) -> ToolOutcome:
        """
        Merge two contacts into a single consolidated record.

        Overview
        --------
        This operation reads both source contacts, computes a per‑column winner, updates
        the kept record with the consolidated values, deletes the other record, and then
        rewrites transcript references so message histories remain consistent.

        Parameters
        ----------
        contact_id_1 : int
            Identifier of the first source contact.
        contact_id_2 : int
            Identifier of the second source contact. Must be different from ``contact_id_1``.
        overrides : Dict[str, int], optional
            A map indicating which source wins for each column. Keys are column names.
            Values must be either ``1`` or ``2`` where:
            - ``1`` → take the value from ``contact_id_1``
            - ``2`` → take the value from ``contact_id_2``

            If not provided, the first non‑``None`` value in the order ``contact_id_1`` → ``contact_id_2`` is used for each column.
            The special key ``"contact_id"`` can be provided to explicitly choose which id to keep; the other contact will be deleted.

        Returns
        -------
        ToolOutcome
            ``{"outcome": "contacts merged successfully", "details": {"kept_contact_id": <int>, "deleted_contact_id": <int>}}``.

        Raises
        ------
        ValueError
            - If the two ids are identical.
            - If either contact cannot be found.
            - If any value in ``overrides`` is not ``1`` or ``2``.
        RuntimeError
            If the merge would delete a protected assistant self or boss contact.

        Notes
        -----
        - After the merge, transcript messages that referenced the deleted contact will have
          their ``contact_id`` updated to the kept id for consistency.
        """
        return _op_merge(
            self,
            contact_id_1=contact_id_1,
            contact_id_2=contact_id_2,
            overrides=overrides,
        )

    def warm_embeddings(self) -> None:
        try:
            ensure_vector_column(
                self._ctx,
                embed_column="_bio_emb",
                source_column="bio",
            )
        except Exception:
            pass

    def _provision_storage(self) -> None:
        """Ensure Contacts context, schema, and local view exist (delegated)."""
        _storage_provision(self)

    def _num_contacts(
        self,
    ) -> int:
        """
        Get the total number of contacts stored in the contacts table.

        Returns
        -------
        int
            The total number of contacts.
        """
        return federated_count(
            [FederatedSearchContext(context=self._ctx, source=self._ctx)],
            key="contact_id",
        )

    def _get_columns(self) -> Dict[str, str]:
        return _storage_get_columns(self)

    # System contact sync
    def _sync_required_contacts(self) -> None:
        from ..session_details import SESSION_DETAILS

        self_contact_id = int(SESSION_DETAILS.self_contact_id)
        boss_contact_id = int(SESSION_DETAILS.boss_contact_id)
        existing_logs = db.get_logs(
            context=self._ctx,
            filter=f"contact_id == {self_contact_id} or contact_id == {boss_contact_id}",
            limit=2,
        )
        logs_by_contact_id = {
            int(lg.entries.get("contact_id")): lg
            for lg in existing_logs
            if lg.entries.get("contact_id") is not None
        }
        assistant_log = logs_by_contact_id.get(self_contact_id)
        user_log = logs_by_contact_id.get(boss_contact_id)
        _sys_provision_assistant_contact(
            self,
            assistant_log,
            contact_id=self_contact_id,
        )
        _sys_provision_user_contact(self, user_log, contact_id=boss_contact_id)

    # Validation / sanitization
    def _allowed_fields(self) -> list[str]:
        """Return the list of columns safe to fetch (exclude private/vector)."""
        cols = self._get_columns()
        # Exclude private (leading underscore) and vector columns ("*_emb")
        allowed = [
            name
            for name in cols.keys()
            if not str(name).startswith("_") and not str(name).endswith("_emb")
        ]
        # Ensure all built-ins are present even if schema drifted
        for b in self._BUILTIN_FIELDS:
            if b not in allowed:
                allowed.append(b)
        return allowed

    # Misc small utilities (kept last)
    # Deprecated: client construction is centralized in unify.common.llm_client.new_llm_client
    @staticmethod
    def _default_ask_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Require search_contacts on the first step (if enabled); auto thereafter."""
        from unify.settings import SETTINGS

        if (
            SETTINGS.FIRST_ASK_TOOL_IS_SEARCH
            and step_index < 1
            and "search_contacts" in current_tools
        ):
            return (
                "required",
                {"search_contacts": current_tools["search_contacts"]},
            )
        return ("auto", current_tools)

    @staticmethod
    def _default_update_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Require ask on the first step (if enabled); auto thereafter."""
        from unify.settings import SETTINGS

        if (
            SETTINGS.FIRST_MUTATION_TOOL_IS_ASK
            and step_index < 1
            and "ask" in current_tools
        ):
            return ("required", {"ask": current_tools["ask"]})
        return ("auto", current_tools)
