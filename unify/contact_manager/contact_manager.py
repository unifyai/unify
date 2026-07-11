import logging
from contextlib import contextmanager
from typing import List, Dict, Optional, Callable, Any, Tuple, Type, Union, Set
from pydantic import BaseModel
import asyncio
import functools
import re
import threading

_log = logging.getLogger(__name__)
logger = _log

CONTACTS_TABLE = "Contacts"
CONTACTS_META_TABLE = "Contacts/Meta"
from .prompt_builders import build_ask_prompt, build_update_prompt
from ..knowledge_manager.types import ColumnType
from ..common.embed_utils import ensure_vector_column
from ..common.tool_outcome import ToolErrorException, ToolOutcome
from ..common.tool_spec import read_only, manager_tool, ToolSpec

import unisdk
from .types.contact import Contact, VOICE_ENROLLMENT_FIELDS
from .types.meta import ContactMeta
from .custom_contacts import compute_custom_contacts_hash
from ..common.log_utils import create_logs as unity_create_logs
from ..common.authorship import strip_authoring_assistant_id
from ..common.embed_utils import list_private_fields
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
from ..blacklist_manager.blacklist_manager import BlackListManager
from ..conversation_manager.cm_types import Medium

# Module delegations (split helpers)
from .storage import (
    provision_storage as _storage_provision,
    get_columns as _storage_get_columns,
)
from .system_contacts import (
    _ensure_columns_exist as _sys_ensure_columns_exist,
    provision_assistant_contact as _sys_provision_assistant_contact,
    provision_user_contact as _sys_provision_user_contact,
    provision_org_member_contacts as _sys_provision_org_member_contacts,
    provision_team_assistant_contacts as _sys_provision_team_assistant_contacts,
)
from .custom_columns import (
    create_custom_column as _cc_create,
    delete_custom_column as _cc_delete,
)
from .voice_enrollment import (
    get_voice_profiles as _voice_get_profiles,
    get_voice_enrollment_info as _voice_get_info,
    set_voice_enrollment as _voice_set_enrollment,
    sync_manual_voice_enrollment as _voice_sync_manual,
)
from .ops import (
    create_contact as _op_create,
    update_contact as _op_update,
    delete_contact as _op_delete,
    merge_contacts as _op_merge,
)
from ..common.federated_search import (
    CONTEXT_FIELD,
    FederatedSearchContext,
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
                fields={**model_to_fields(Contact), **VOICE_ENROLLMENT_FIELDS},
                unique_keys={"contact_id": "int"},
                auto_counting={"contact_id": None},
            ),
            TableContext(
                name=CONTACTS_META_TABLE,
                description="Metadata for source-defined custom contact sync state.",
                fields=model_to_fields(ContactMeta),
                unique_keys={"meta_id": "int"},
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
        "you may inform your boss about this new contact and ask for guidance, or if this appears to be "
        "spam or unwanted contact, you may choose to blacklist them via the Actor. If your boss confirms "
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
        self._meta_ctx = ContextRegistry.get_context(self, CONTACTS_META_TABLE)
        self._custom_contacts_synced = False
        self._custom_contacts_synced_contexts: set[str] = set()
        self._destination_context_lock = threading.RLock()
        self._destination_write_scoped = False

        # Local DataStore mirror (write-through only; never read from it)
        self._data_store = DataStore.for_context(self._ctx, key_fields=("contact_id",))

        # ── immutable built-in columns ───────────────────────────────────
        # Derive the required/built-in columns directly from the Contact model so
        # that there is a single source-of-truth for field names across the
        # code-base.  Any future change to the Contact schema will
        # automatically propagate here.
        self._BUILTIN_FIELDS: Tuple[str, ...] = tuple(Contact.model_fields.keys())
        self._REQUIRED_COLUMNS: set[str] = set(self._BUILTIN_FIELDS)

        # Track observed/created custom columns in-process so immediate reads
        # right after creation include the new columns without requiring a
        # round-trip schema refresh.
        self._known_custom_fields: set[str] = set()

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
                    fn=self._create_custom_column,
                    display_label="Adding a custom contact field",
                ),
                ToolSpec(
                    fn=self._delete_custom_column,
                    display_label="Removing a custom contact field",
                ),
                ToolSpec(
                    fn=self._merge_contacts,
                    display_label="Merging duplicate contacts",
                ),
                ToolSpec(
                    fn=self._move_to_blacklist,
                    display_label="Blocking a contact",
                ),
                include_class_name=False,
            ),
        }
        self.add_tools("update", update_tools)

        # rolling activity inclusion flag
        self._rolling_summary_in_prompts = rolling_summary_in_prompts

        # (No per-instance shorthand registry; dynamic aliases are registered
        #  directly on the Contact class for minimal plumbing.)

        # Ensure context/schema and prefill known custom fields
        self._provision_storage()

        # Ensure assistant self and boss contacts exist and are up to date.
        self._sync_required_contacts()

    def _contact_context_from_root(self, root_context: str) -> str:
        """Return the concrete Contacts context under one registry root."""

        return f"{root_context.strip('/')}/Contacts"

    def _contact_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public write destination into a concrete Contacts context."""

        root_context = ContextRegistry.write_root(
            self,
            "Contacts",
            destination=destination,
        )
        return self._contact_context_from_root(root_context)

    def _read_contact_contexts(self) -> list[str]:
        """Return ordered concrete Contacts contexts visible to this assistant."""

        try:
            root_contexts = ContextRegistry.read_roots(self, "Contacts")
            contexts = [self._contact_context_from_root(root) for root in root_contexts]
        except RuntimeError as exc:
            if "no base context available" not in str(exc):
                raise
            from ..session_details import SESSION_DETAILS

            contexts = [self._ctx]
            contexts.extend(
                f"Teams/{team_id}/Contacts"
                for team_id in sorted(set(SESSION_DETAILS.team_ids))
            )
        return list(dict.fromkeys(contexts))

    def _data_store_for_context(self, context: str):
        """Return the per-root local cache for a concrete Contacts context."""

        if context == self._ctx:
            return self._data_store
        return DataStore.for_context(context, key_fields=("contact_id",))

    def _membership_target_for_destination(
        self,
        destination: str | None,
    ) -> tuple[str, int | None]:
        """Return the ContactMembership target fields for a public destination."""

        if destination is None or destination == "personal":
            return "personal", None
        return "team", int(destination.removeprefix("team:"))

    def _delete_contact_memberships(
        self,
        contact_id: int,
        *,
        destination: str | None,
    ) -> None:
        """Delete assistant relationship overlays for one contact id."""

        from ..session_details import SESSION_DETAILS

        if (
            not SESSION_DETAILS.is_initialized
            or SESSION_DETAILS.assistant.agent_id is None
        ):
            return

        api_key = SESSION_DETAILS.unify_key
        if not api_key:
            _log.warning(
                "UNIFY_KEY is not set; skipping contact membership deletion.",
            )
            return

        from unisdk.utils import http

        target_scope, target_team_id = self._membership_target_for_destination(
            destination,
        )
        assistant_id = int(SESSION_DETAILS.assistant.agent_id)
        url = (
            f"{SETTINGS.ORCHESTRA_URL.rstrip('/')}"
            f"/assistant/{assistant_id}/contact-memberships/{int(contact_id)}"
        )
        response = http.delete(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
            params={
                "target_scope": target_scope,
                "target_team_id": target_team_id,
            },
            timeout=15,
        )
        response.raise_for_status()

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
                ReadOnlyAskGuardHandle if SETTINGS.UNITY_READONLY_ASK_GUARD else None
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
        unisdk.delete_context(self._ctx)

        # Clear local cache and custom-field state so subsequent reads/writes
        # operate against a clean slate
        try:
            self._data_store.clear()
        except Exception:
            pass

        # No per-instance custom field state to reset

        # Ensure the schema exists again via shared provisioning helper
        ContextRegistry.refresh(self, "Contacts")

        self._provision_storage()

        # Verify the context is visible before attempting reads
        try:
            import time as _time  # local import to avoid polluting module namespace

            for _ in range(3):
                try:
                    unisdk.get_fields(context=self._ctx)
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

        for context in self._read_contact_contexts():
            if not remaining:
                break
            store = self._data_store_for_context(context)

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

            found_ids = set(results).intersection(remaining)
            remaining = [cid for cid in remaining if cid not in found_ids]
            if not remaining:
                break

            if misses:
                if len(misses) == 1:
                    filt = f"contact_id == {misses[0]}"
                else:
                    filt = f"contact_id in [{', '.join(str(x) for x in misses)}]"
                rows = unisdk.get_logs(
                    context=context,
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

                found_ids = set(results).intersection(remaining)
                remaining = [cid for cid in remaining if cid not in found_ids]

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
            ``\"contact_id\"`` or numeric custom columns. A single column name
            returns a scalar; a list of column names computes the metric
            independently per key and returns a ``{key -> value}`` mapping.
        filter : str | dict[str, str] | None, default None
            Optional row-level filter expression(s) in the same Python syntax as
            :py:meth:`filter_contacts`. When a string, the expression is applied
            uniformly; when a dict, each key maps to its own filter expression.
        group_by : str | list[str] | None, default None
            Optional contact field(s) to group by, for example ``\"should_respond\"``
            or a segmenting custom column. Use a single column name for one
            grouping level, or a list such as ``[\"should_respond\", \"contact_id\"]``
            to group hierarchically in that order. When provided, the result
            becomes a nested mapping keyed by group values, mirroring
            :func:`unisdk.get_logs_metric` behaviour.

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
                    context=context,
                    source=context,
                    allowed_fields=[key, *group_fields],
                )
                for context in self._read_contact_contexts()
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
            your expression applies a case‑normalisation.
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
        if getattr(self, "_known_custom_fields", None):
            from_fields.extend(sorted(self._known_custom_fields))

        contexts = [
            FederatedSearchContext(
                context=context,
                source=context,
                allowed_fields=from_fields,
            )
            for context in self._read_contact_contexts()
        ]
        annotated_rows = federated_filter(
            contexts,
            filter=normalize_filter_expr(filter),
            offset=offset,
            limit=eff_limit,
        )
        rows: list[dict] = []
        for annotated in annotated_rows:
            row = {
                key: value
                for key, value in annotated.items()
                if not key.startswith("_federated_")
            }
            # Write-through to the local DataStore mirror for the source root.
            try:
                self._data_store_for_context(annotated[CONTEXT_FIELD]).put(row)
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
        if getattr(self, "_known_custom_fields", None):
            allowed_fields.extend(sorted(self._known_custom_fields))

        from ..session_details import SESSION_DETAILS

        system_filter = (
            f"contact_id != {int(SESSION_DETAILS.self_contact_id)} "
            f"and contact_id != {int(SESSION_DETAILS.boss_contact_id)}"
        )
        contexts = [
            FederatedSearchContext(
                context=context,
                source=context,
                row_filter=system_filter,
                allowed_fields=allowed_fields,
            )
            for context in self._read_contact_contexts()
        ]
        rows = federated_ranked_search(
            contexts,
            references,
            limit=k,
            backfill=True,
        )

        visible_contacts: list[Contact] = []
        for row in rows:
            source_context = row.get(CONTEXT_FIELD)
            clean = {
                key: value
                for key, value in row.items()
                if not key.startswith("_federated_")
            }
            if source_context:
                try:
                    self._data_store_for_context(source_context).put(clean)
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
        custom_key: Optional[str] = None,
        custom_hash: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        destination: Optional[str] = None,
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
        custom_key / custom_hash : str | None
            Deployment-defined contact identity fields. Optional.
        custom_fields : dict[str, Any] | None
            Values for existing custom columns (snake_case keys that are not part of
            the built‑in ``Contact`` schema). Create new columns first via
            ``_create_custom_column``. Do not nest a key literally named ``"kwargs"``.
        destination : str | None, default None
            Where to file this contact. Pass ``"personal"`` (the default) for
            contacts that belong only to you — personal acquaintances, family,
            contacts whose interactions are private to your relationship with
            your boss. Pass ``"team:<id>"`` for an operational team contact
            that every member of a shared team should see (operatives,
            customers, suppliers, peers in a shared workspace). The set of
            available ``team:<id>`` values, each with a name and a
            description naming the team / domain it exists for, is rendered in
            the *Accessible shared teams* block of your system prompt — read
            that block before choosing. The privacy floor: when in doubt
            between personal and a team, pick personal. When confidence is
            low and the contact would land in a shared team, call
            ``request_clarification`` instead of guessing toward the wider
            audience.

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
        - New regular contacts receive the next available id in the destination root.
          System contacts are provisioned separately from resolved session ids.
        - ``response_policy`` defaults to a conservative policy that avoids sharing sensitive
          information when not explicitly provided.
        - Unspecified fields remain ``None`` and can be populated later via ``update_contact``.
        - For custom columns, ensure the column exists beforehand via ``_create_custom_column``;
          otherwise the request will fail server‑side.
        """
        try:
            context = self._contact_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
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
            custom_key=custom_key,
            custom_hash=custom_hash,
            custom_fields=custom_fields,
            contact_id=_contact_id,
            context=context,
            data_store=self._data_store_for_context(context),
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
        custom_key: Optional[str] = None,
        custom_hash: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        destination: Optional[str] = None,
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
        custom_key / custom_hash : str | None
            Deployment-defined contact identity fields. Optional.
        custom_fields : dict[str, Any] | None
            Updates for existing custom columns. Keys must be existing column names
            (snake_case) that are not part of the built‑in ``Contact`` schema. Any key
            with a ``None`` value is ignored.
        destination : str | None, default None
            The team whose copy of this contact you are updating. Defaults to
            ``"personal"`` (your private copy). Passing ``"team:<id>"``
            updates the shared copy in that team and is visible to every
            member. See the *Accessible shared teams* block in your system
            prompt for the available teams and their descriptions.

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
        try:
            context = self._contact_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
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
            custom_key=custom_key,
            custom_hash=custom_hash,
            custom_fields=custom_fields,
            _log_id=_log_id,
            context=context,
            data_store=self._data_store_for_context(context),
        )

    def _delete_contact(
        self,
        *,
        contact_id: int,
        destination: Optional[str] = None,
        _log_id: Optional[int] = None,
    ) -> ToolOutcome:
        """
        Permanently delete a contact.

        Parameters
        ----------
        contact_id : int
            The identifier of the contact to remove. Must refer to a non‑system contact.
        destination : str | None, default None
            Which copy of the contact to remove. Defaults to ``"personal"``.
            Passing ``"team:<id>"`` removes the shared copy from that team
            for every member; do not delete a shared contact unless the team
            decision is to remove the relationship entirely. See the
            *Accessible shared teams* block in your system prompt.

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
        try:
            context = self._contact_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        outcome = _op_delete(
            self,
            contact_id=contact_id,
            _log_id=_log_id,
            context=context,
            data_store=self._data_store_for_context(context),
        )
        self._delete_contact_memberships(contact_id, destination=destination)
        return outcome

    def _merge_contacts(
        self,
        *,
        contact_id_1: int,
        contact_id_2: int,
        overrides: Optional[Dict[str, int]] = None,
        destination: Optional[str] = None,
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
            A map indicating which source wins for each column. Keys are column names
            (built‑in or custom). Values must be either ``1`` or ``2`` where:
            - ``1`` → take the value from ``contact_id_1``
            - ``2`` → take the value from ``contact_id_2``

            If not provided, the first non‑``None`` value in the order ``contact_id_1`` → ``contact_id_2`` is used for each column.
            The special key ``"contact_id"`` can be provided to explicitly choose which id to keep; the other contact will be deleted.
        destination : str | None, default None
            Which root the merge operates within. Defaults to ``"personal"``.
            Passing ``"team:<id>"`` merges the two contacts inside that
            team's contact pool; merging across roots is not supported.
            See the *Accessible shared teams* block in your system prompt.

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
        - Custom fields are applied via ``update_contact``; built‑in fields are applied
          directly as arguments.
        """
        try:
            context = self._contact_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        return _op_merge(
            self,
            contact_id_1=contact_id_1,
            contact_id_2=contact_id_2,
            overrides=overrides,
            context=context,
            data_store=self._data_store_for_context(context),
        )

    def _move_to_blacklist(
        self,
        *,
        contact_id: int,
        reason: str,
        destination: Optional[str] = None,
    ) -> ToolOutcome:
        """
        Add all non-empty contact details for the specified contact to the blacklist.

        For each available detail:
        - email_address → one blacklist entry with ``medium == email``.
        - phone_number → two entries with ``medium == sms_message`` and ``phone_call``.

        The blacklist reason is standardised as a concise summary of the contact followed by the cause:
        - ``"{first_name}, {surname}, {bio}, moved to blacklist due to {reason}"`` with missing parts omitted and no stray commas.

        Additionally, this tool deletes the contact from the Contacts table once the blacklist entries
        have been created. When no details exist to blacklist, the contact is still deleted as part of
        the move operation.
        destination : str | None, default None
            Which Contacts root contains the contact. Defaults to ``"personal"``.
            Pass ``"team:<id>"`` when blacklisting a contact from a shared
            team. See the *Accessible shared teams* block in your system prompt.

        Returns
        -------
        ToolOutcome
            ``{"outcome": "contact details moved to blacklist", "details": {"contact_id": <int>, "blacklist_ids": [<int>, ...]}}``.

        Raises
        ------
        ValueError
            If the contact cannot be found.
        """
        try:
            context = self._contact_context_for_destination(destination)
        except ToolErrorException as exc:
            return exc.payload
        store = self._data_store_for_context(context)

        # Fetch the contact row (public fields only)
        rows = unisdk.get_logs(
            context=context,
            filter=f"contact_id == {int(contact_id)}",
            limit=1,
            from_fields=self._allowed_fields(),
        )
        if not rows:
            raise ValueError(
                f"No contact found with contact_id {contact_id} to move to blacklist.",
            )
        ent = rows[0].entries

        first = (ent.get("first_name") or "").strip()
        last = (ent.get("surname") or "").strip()
        bio = (ent.get("bio") or "").strip()
        parts = [p for p in (first, last, bio) if p]
        head = ", ".join(parts)
        suffix = f"moved to blacklist due to {reason}"
        bl_reason = f"{head}, {suffix}" if head else suffix

        # Build detail → media pairs
        detail_media: list[tuple[str, Medium]] = []
        email = (ent.get("email_address") or "").strip()
        if email:
            detail_media.append((email, Medium.EMAIL))
        phone = (ent.get("phone_number") or "").strip()
        if phone:
            detail_media.append((phone, Medium.SMS_MESSAGE))
            detail_media.append((phone, Medium.PHONE_CALL))

        if not detail_media:
            # Even when no details exist, delete the contact as part of the move
            try:
                _op_delete(
                    self,
                    contact_id=contact_id,
                    _log_id=None,
                    context=context,
                    data_store=store,
                )
                self._delete_contact_memberships(
                    contact_id,
                    destination=destination,
                )
            except Exception:
                # Best-effort delete; surface original outcome regardless
                pass
            return {
                "outcome": "no contact details to blacklist",
                "details": {"contact_id": int(contact_id), "blacklist_ids": []},
            }

        blm = BlackListManager()
        blacklist_context = blm._blacklist_context_for_destination(destination)
        created_ids: list[int] = []

        # Best-effort de-duplication per (medium, contact_detail)
        for detail, med in detail_media:
            existing = unisdk.get_logs(
                context=blacklist_context,
                filter=f"medium == '{med.value}' and contact_detail == '{detail}'",
                limit=1,
            )
            if existing:
                # Skip creating duplicates
                try:
                    created_ids.append(int(existing[0].entries["blacklist_id"]))
                except Exception:
                    pass
                continue

            res = blm.create_blacklist_entry(
                medium=med,
                contact_detail=detail,
                reason=bl_reason,
                destination=destination,
            )
            try:
                created_ids.append(int(res["details"]["blacklist_id"]))
            except Exception:
                pass

        # Finally, delete the original contact
        _op_delete(
            self,
            contact_id=contact_id,
            _log_id=None,
            context=context,
            data_store=store,
        )
        self._delete_contact_memberships(contact_id, destination=destination)

        return {
            "outcome": "contact details moved to blacklist",
            "details": {"contact_id": int(contact_id), "blacklist_ids": created_ids},
        }

    def _create_custom_column(
        self,
        *,
        column_name: str,
        column_type: ColumnType | str,
        column_description: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Create a new custom column on the contacts table.

        Parameters
        ----------
        column_name : str
            The name of the new custom column. Must be a valid snake_case name.
        column_type : ColumnType | str
            The type of the new custom column.
        column_description : str | None
            The description of the new custom column.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the name and type of the new custom column.
        """
        return _cc_create(
            self,
            column_name=column_name,
            column_type=column_type,
            column_description=column_description,
        )

    def _delete_custom_column(self, *, column_name: str) -> Dict[str, str]:
        """
        Delete a custom column from the contacts table.

        Parameters
        ----------
        column_name : str
            The name of the custom column to delete.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the name.
        """
        return _cc_delete(self, column_name=column_name)

    # ──────────────────────────────────────────────────────────────────────
    #  Internal helpers (not exposed as tools)
    # ──────────────────────────────────────────────────────────────────────
    # Storage / provisioning
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

    # ── voice enrollment (programmatic only; never exposed as LLM tools) ──
    def get_voice_profiles(self, contact_ids) -> Dict[int, List[float]]:
        """Return {contact_id: voice embedding} for enrolled contacts."""
        return _voice_get_profiles(self, contact_ids)

    def get_voice_enrollment_info(self, contact_id: int) -> Dict[str, Any]:
        """Return enrollment metadata (enrolled, enrolled_at, source)."""
        return _voice_get_info(self, contact_id)

    def set_voice_enrollment(
        self,
        *,
        contact_id: int,
        embedding: List[float],
        wav_bytes: bytes | None = None,
        source: str,
    ) -> None:
        """Persist a voice enrollment (embedding + optional sample) on a contact."""
        _voice_set_enrollment(
            self,
            contact_id=contact_id,
            embedding=embedding,
            wav_bytes=wav_bytes,
            source=source,
        )

    def sync_manual_voice_enrollment(self) -> None:
        """Sync the boss user's manually recorded voice sample onto the boss contact."""
        _voice_sync_manual(self)

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
            [
                FederatedSearchContext(context=context, source=context)
                for context in self._read_contact_contexts()
            ],
            key="contact_id",
        )

    def _get_columns(self) -> Dict[str, str]:
        return _storage_get_columns(self)

    # System contact sync
    def _ensure_columns_exist(self, extra_fields: Dict[str, Any]) -> None:
        _sys_ensure_columns_exist(self, extra_fields)

    def _sync_required_contacts(self) -> None:
        from ..session_details import SESSION_DETAILS

        self_contact_id = int(SESSION_DETAILS.self_contact_id)
        boss_contact_id = int(SESSION_DETAILS.boss_contact_id)
        existing_logs = unisdk.get_logs(
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

        # Sync org members (returns early if not org API key)
        _sys_provision_org_member_contacts(self)

        # Sync teammate assistants (returns early when not on any team)
        _sys_provision_team_assistant_contacts(self)

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
        # Include any custom fields created/observed during this manager's lifetime
        try:
            for k in getattr(self, "_known_custom_fields", set()):
                if k not in allowed:
                    allowed.append(k)
        except Exception:
            pass
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

    def _meta_context_for_destination(self, destination: str | None) -> str:
        """Resolve a public destination into one concrete Contacts/Meta context."""
        root_context = ContextRegistry.write_root(
            self,
            CONTACTS_META_TABLE,
            destination=destination,
        )
        return f"{root_context.strip('/')}/{CONTACTS_META_TABLE}"

    @contextmanager
    def _temporary_contact_context(self, attr_name: str, context: str):
        """Temporarily bind an existing storage method to a resolved context."""
        with self._destination_context_lock:
            original = getattr(self, attr_name)
            was_write_scoped = self._destination_write_scoped
            setattr(self, attr_name, context)
            self._destination_write_scoped = True
            try:
                yield
            finally:
                setattr(self, attr_name, original)
                self._destination_write_scoped = was_write_scoped

    def _sync_destination_contexts(
        self,
        destination: str | None,
    ) -> tuple[str, str, bool]:
        """Return destination-scoped contacts context, meta context, and personal flag."""
        data_context = self._contact_context_for_destination(destination)
        meta_context = self._meta_context_for_destination(destination)
        return data_context, meta_context, destination in (None, "personal")

    def _get_stored_custom_contacts_hash(self) -> str:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get("custom_contacts_hash", "") or ""
        except Exception as exc:
            logger.warning("Failed to read custom contacts hash: %s", exc)
        return ""

    def _store_custom_contacts_hash(self, hash_value: str) -> None:
        try:
            logs = unisdk.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                unisdk.update_logs(
                    context=self._meta_ctx,
                    logs=[logs[0].id],
                    entries={"custom_contacts_hash": hash_value},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[{"meta_id": 1, "custom_contacts_hash": hash_value}],
                    stamp_authoring=True,
                )
        except Exception as exc:
            logger.warning("Failed to store custom contacts hash: %s", exc)

    def _get_custom_contacts_from_db(self) -> Dict[str, Dict[str, Any]]:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter="custom_hash != None",
            exclude_fields=list_private_fields(self._ctx),
        )
        return {
            lg.entries.get("custom_key"): lg.entries
            for lg in logs
            if lg.entries.get("custom_key")
        }

    def _delete_custom_contact_by_key(self, custom_key: str) -> bool:
        logs = unisdk.get_logs(
            context=self._ctx,
            filter=f"custom_key == '{custom_key}' and custom_hash != None",
            limit=1,
        )
        if not logs:
            return False
        unisdk.delete_logs(context=self._ctx, logs=[logs[0].id])
        return True

    def _update_custom_contact(
        self,
        contact_id: int,
        data: Dict[str, Any],
    ) -> None:
        log_ids = unisdk.get_logs(
            context=self._ctx,
            filter=f"contact_id == {int(contact_id)}",
            limit=1,
            return_ids_only=True,
        )
        if not log_ids:
            raise ValueError(
                f"No contact found with contact_id {contact_id} to update.",
            )
        update_data = strip_authoring_assistant_id(
            {k: v for k, v in data.items() if k != "contact_id"},
        )
        unisdk.update_logs(
            context=self._ctx,
            logs=[log_ids[0]],
            entries=update_data,
            overwrite=True,
        )

    def _insert_custom_contact(self, data: Dict[str, Any]) -> int:
        insert_data = {k: v for k, v in data.items() if k != "contact_id"}
        if insert_data.get("response_policy") is None:
            insert_data["response_policy"] = self.DEFAULT_RESPONSE_POLICY
        insert_data.setdefault("is_system", False)
        result = unity_create_logs(
            context=self._ctx,
            entries=[insert_data],
            stamp_authoring=True,
            recompute_derived=True,
        )
        if isinstance(result, list) and len(result) > 0:
            log = result[0]
            if hasattr(log, "entries"):
                return log.entries.get("contact_id", -1)
        elif isinstance(result, dict):
            log_ids = result.get("log_event_ids", [])
            if log_ids:
                logs = unisdk.get_logs(
                    context=self._ctx,
                    filter=f"id == {log_ids[0]}",
                    limit=1,
                )
                if logs and hasattr(logs[0], "entries"):
                    return logs[0].entries.get("contact_id")
        return -1

    def sync_custom_contacts(
        self,
        *,
        source_contacts: Optional[Dict[str, Dict[str, Any]]] = None,
        destination: str | None = None,
    ) -> bool:
        """Ensure custom contact rows match source ``contacts.jsonl`` definitions."""
        try:
            contacts_context, meta_context, is_personal = (
                self._sync_destination_contexts(destination)
            )
        except ToolErrorException as exc:
            return exc.payload  # type: ignore[return-value]

        with (
            self._temporary_contact_context("_ctx", contacts_context),
            self._temporary_contact_context("_meta_ctx", meta_context),
        ):
            if source_contacts is None:
                source_contacts = {}
            expected_hash = compute_custom_contacts_hash(
                source_contacts=source_contacts,
            )
            current_hash = self._get_stored_custom_contacts_hash()
            already_synced = (
                self._custom_contacts_synced
                if is_personal
                else contacts_context in self._custom_contacts_synced_contexts
            )

            if already_synced and current_hash == expected_hash:
                return False

            if current_hash == expected_hash:
                logger.debug("Custom contacts hash matches, skipping sync")
                if is_personal:
                    self._custom_contacts_synced = True
                else:
                    self._custom_contacts_synced_contexts.add(contacts_context)
                return False

            logger.info(
                "Custom contacts hash mismatch "
                "(current=%s, expected=%s), syncing...",
                current_hash,
                expected_hash,
            )

            db_contacts = self._get_custom_contacts_from_db()
            processed_keys: Set[str] = set()

            for custom_key, source_data in source_contacts.items():
                processed_keys.add(custom_key)
                contact_data = {
                    k: v for k, v in source_data.items() if k not in {"destination"}
                }

                if custom_key in db_contacts:
                    db_entry = db_contacts[custom_key]
                    if db_entry.get("custom_hash") != contact_data["custom_hash"]:
                        logger.info("Updating custom contact entry: %s", custom_key)
                        self._update_custom_contact(
                            contact_id=db_entry["contact_id"],
                            data=contact_data,
                        )
                else:
                    existing = unisdk.get_logs(
                        context=self._ctx,
                        filter=f"custom_key == '{custom_key}'",
                        limit=1,
                    )
                    if existing:
                        logger.info(
                            "Overwriting user-added contact entry with custom: %s",
                            custom_key,
                        )
                        unisdk.delete_logs(
                            context=self._ctx,
                            logs=[existing[0].id],
                        )

                    logger.info("Inserting custom contact entry: %s", custom_key)
                    self._insert_custom_contact(contact_data)

            for custom_key in db_contacts:
                if custom_key not in processed_keys:
                    logger.info(
                        "Deleting removed custom contact entry: %s",
                        custom_key,
                    )
                    self._delete_custom_contact_by_key(custom_key)

            self._store_custom_contacts_hash(expected_hash)
            if is_personal:
                self._custom_contacts_synced = True
            else:
                self._custom_contacts_synced_contexts.add(contacts_context)
            return True

    def sync_custom(
        self,
        *,
        source_contacts: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> bool:
        """Sync custom contacts from pre-collected sources across destinations."""
        if source_contacts is None:
            source_contacts = {}

        by_destination: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for custom_key, source_data in source_contacts.items():
            destination = source_data.get("destination") or "personal"
            by_destination.setdefault(destination, {})[custom_key] = source_data

        changed = False
        for destination, group in by_destination.items():
            destination_arg = None if destination == "personal" else destination
            changed |= self.sync_custom_contacts(
                source_contacts=group,
                destination=destination_arg,
            )
        return changed
