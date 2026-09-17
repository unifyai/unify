from __future__ import annotations

import asyncio
import functools
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Type
from pydantic import BaseModel

from unify import db
from unify.common.llm_client import new_llm_client
from unify.common.log_utils import log as unity_log

logger = logging.getLogger(__name__)
from ..common.llm_helpers import methods_to_tool_dict
from ..common.tool_spec import ToolSpec
from ..common.async_tool_loop import (
    start_async_tool_loop,
    SteerableToolHandle,
    TOOL_LOOP_LINEAGE,
)
from ..settings import SETTINGS
from ..common.read_only_ask_guard import ReadOnlyAskGuardHandle
from ..events.event_bus import EVENT_BUS, Event
from ..events.manager_event_logging import log_manager_call
from ..common.tool_outcome import ToolOutcome
from ..common.embed_utils import ensure_vector_column
from ..common.context_store import TableStore
from ..common.model_to_fields import model_to_fields
from .types import Secret
from .base import BaseSecretManager
from .prompt_builders import build_ask_prompt, build_update_prompt
from ..common.filter_utils import normalize_filter_expr
from ..common.search_utils import is_plain_identifier
from ..common.federated_search import (
    FederatedSearchContext,
    federated_filter,
    federated_ranked_search,
)
from ..common.context_registry import ContextRegistry, TableContext

SECRETS_TABLE = "Secrets"  # pragma: allowlist secret


class SecretManager(BaseSecretManager):
    """
    Manage credentials without exposing raw values to LLMs.

    All credentials live in one vault, mirrored into the local ``.env`` file
    so code executed by the assistant can read them from ``os.environ``.
    Runtime credential lookups raise for unknown names rather than falling
    back to any other source.
    """

    class Config:
        required_contexts = [
            TableContext(
                name=SECRETS_TABLE,
                description="Key-value secrets with descriptions and embeddings.",
                fields=model_to_fields(Secret),
                unique_keys={"secret_id": "int", "name": "str"},
                auto_counting={"secret_id": None},
            ),
        ]

    def __init__(self) -> None:
        super().__init__()
        self._ctx = ContextRegistry.get_context(self, SECRETS_TABLE)

        # Ensure storage/schema exists deterministically (idempotent)
        self._provision_storage()

        # Public tools
        ask_tools: Dict[str, Callable] = {
            **methods_to_tool_dict(
                ToolSpec(
                    fn=self._list_columns,
                    display_label="Listing credential fields",
                ),
                ToolSpec(
                    fn=self._filter_secrets,
                    display_label="Filtering credentials",
                ),
                ToolSpec(
                    fn=self._search_secrets,
                    display_label="Searching credentials",
                ),
                ToolSpec(
                    fn=self._list_secret_keys,
                    display_label="Listing credential names",
                ),
                include_class_name=False,
            ),
        }
        self.add_tools("ask", ask_tools)
        update_tools: Dict[str, Callable] = {
            **methods_to_tool_dict(
                ToolSpec(fn=self.ask, display_label="Querying credentials"),
                ToolSpec(
                    fn=self._create_secret,
                    display_label="Storing a new credential",
                ),
                ToolSpec(fn=self._update_secret, display_label="Updating a credential"),
                ToolSpec(fn=self._delete_secret, display_label="Deleting a credential"),
                include_class_name=False,
            ),
        }
        self.add_tools("update", update_tools)

        # .env sync: create file if missing and backfill existing secrets as KEY=VALUE
        try:
            self._ensure_dotenv_synced_on_init()
        except Exception:
            # Best-effort – local file sync must never break construction
            pass

    # --------------------- Storage provisioning helper --------------------- #
    def _provision_storage(self) -> None:
        """Ensure Secrets context and schema exist and required vectors are present."""
        self._store = TableStore(
            self._ctx,
            unique_keys={"secret_id": "int", "name": "str"},
            auto_counting={"secret_id": None},
            description="Key-value secrets with descriptions and embeddings.",
            fields=model_to_fields(Secret),
        )
        self._description_vector_ensured = False

    def warm_embeddings(self) -> None:
        self._ensure_description_vector()

    def _ensure_description_vector(self) -> None:
        """Ensure the description embedding exists on the Secrets table."""
        if self._description_vector_ensured:
            return
        try:
            ensure_vector_column(
                self._ctx,
                embed_column="description_emb",
                source_column="description",
                derived_expr=None,
            )
        except Exception:
            pass
        self._description_vector_ensured = True

    @functools.wraps(BaseSecretManager.clear, updated=())
    def clear(self) -> None:
        db.delete_context(self._ctx)

        # Force re-provisioning even if previously ensured
        self._ctx = ContextRegistry.refresh(self, SECRETS_TABLE)

        # Re-create schema and vectors
        self._provision_storage()

        # Verify the context is visible before attempting reads
        try:
            import time as _time  # local import

            for _ in range(3):
                try:
                    db.get_fields(context=self._ctx)
                    break
                except Exception:
                    _time.sleep(0.05)
        except Exception:
            pass

    # --------------------- Internal helpers (LLM client/policies) --------------------- #

    @staticmethod
    def _default_ask_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Default ask-side tool policy (no-op, retain current tools)."""
        return ("auto", current_tools)

    @staticmethod
    def _default_update_tool_policy(
        step_index: int,
        current_tools: Dict[str, Any],
    ) -> tuple[str, Dict[str, Any]]:
        """Require 'ask' on the first step (if enabled); auto thereafter."""
        from unify.settings import SETTINGS

        if (
            SETTINGS.FIRST_MUTATION_TOOL_IS_ASK
            and step_index < 1
            and "ask" in current_tools
        ):
            return ("required", {"ask": current_tools["ask"]})
        return ("auto", current_tools)

    def _get_secret_value(self, name: str) -> str | None:
        try:
            rows = db.get_logs(
                context=self._ctx,
                filter=f"name == {name!r}",
                limit=1,
                from_fields=["name", "value"],
            )
            if rows:
                value = (rows[0].entries or {}).get("value")
                if isinstance(value, str) and value:
                    return value
        except Exception:
            pass
        value = os.environ.get(name)
        return value if value else None

    # --------------------- Internal helpers (.env sync) --------------------- #
    def _dotenv_path(self) -> str:
        """Return the path to the .env file used for local sync.

        Honors UNIFY_SECRET_DOTENV_PATH from SETTINGS; defaults to ".env" in CWD.
        """
        import os as _os

        if SETTINGS.secret.DOTENV_PATH:
            return SETTINGS.secret.DOTENV_PATH
        from unify.file_manager.settings import get_local_root

        return _os.path.join(get_local_root(), ".env")

    def _sync_dotenv(self) -> None:
        """Fetch all secrets from the store and merge into the local .env file.

        Ensures that secrets written to the store through any other path are
        available as environment variables for code executed via
        ``os.environ``.
        """
        try:
            rows = db.get_logs(context=self._ctx)
        except Exception:
            rows = []
        name_to_value: Dict[str, str] = {}
        for lg in rows:
            try:
                nm = (lg.entries or {}).get("name")
                val = (lg.entries or {}).get("value")
                if isinstance(nm, str) and nm and isinstance(val, str):
                    name_to_value[nm] = val
            except Exception:
                continue

        if name_to_value:
            self._env_merge_and_write(add_or_update=name_to_value, remove_keys=None)

    def _ensure_dotenv_synced_on_init(self) -> None:
        """Create .env if missing, pull assistant OAuth tokens, and merge all secrets."""
        path = self._dotenv_path()
        # Ensure directory exists
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        except Exception:
            pass
        # Ensure file exists
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("")

    @staticmethod
    def _parse_env_lines(lines: List[str]) -> Dict[str, int]:
        """Return mapping of existing KEY -> line index for a simple .env file."""
        import re

        key_to_idx: Dict[str, int] = {}
        for idx, raw in enumerate(lines):
            m = re.match(r"\s*([A-Za-z_][A-Za-z0-9_/]*)\s*=", raw)
            if m:
                key_to_idx[m.group(1)] = idx
        return key_to_idx

    def _env_merge_and_write(
        self,
        add_or_update: Dict[str, str] | None,
        remove_keys: List[str] | None,
    ) -> None:
        """Merge provided updates/removals into the .env file atomically."""
        path = self._dotenv_path()
        try:
            with open(path, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except FileNotFoundError:
            lines = []

        key_to_idx = self._parse_env_lines(lines)

        # Remove keys first
        if remove_keys:
            rm = set(remove_keys)

            def _keep(i: int, s: str) -> bool:
                for k, j in key_to_idx.items():
                    if j == i and k in rm:
                        return False
                return True

            lines = [s for i, s in enumerate(lines) if _keep(i, s)]
            key_to_idx = self._parse_env_lines(lines)

        # Add or update keys
        if add_or_update:
            for key, value in add_or_update.items():
                line = f"{key}={value}"
                if key in key_to_idx:
                    lines[key_to_idx[key]] = line
                else:
                    lines.append(line)
                os.environ[key] = value

        if remove_keys:
            for key in remove_keys:
                os.environ.pop(key, None)

        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + ("\n" if lines else ""))

    def _env_set(self, name: str, value: str) -> None:
        """Set or update one KEY=VALUE line in .env."""
        self._env_merge_and_write({name: value}, remove_keys=None)

    def _env_remove(self, name: str) -> None:
        """Remove one KEY from .env (if present)."""
        self._env_merge_and_write(add_or_update=None, remove_keys=[name])

    # --------------------- Public API --------------------- #
    async def from_placeholder(self, text: str) -> str:
        """Resolve ${name} placeholders in text to raw secret values (no LLM).

        Parameters
        ----------
        text : str
            Input string that may contain placeholders like "${api_key}".

        Returns
        -------
        str
            String with placeholders substituted with their secret values.
        """
        return self._resolve_placeholders(text)

    async def to_placeholder(self, text: str) -> str:
        """Convert secret values in text to placeholders.

        Parameters
        ----------
        text : str
            The text to convert secret values to placeholders.

        Returns
        -------
        str
            The text with secret values converted to placeholders.
        """
        value_to_name: Dict[str, str] = {}
        try:
            rows = db.get_logs(
                context=self._ctx,
                from_fields=["name", "value"],
            )
        except Exception:
            rows = []

        for lg in rows:
            try:
                nm = (lg.entries or {}).get("name")
                val = (lg.entries or {}).get("value")
                if isinstance(nm, str) and nm and isinstance(val, str) and val:
                    if val in value_to_name:
                        if nm < value_to_name[val]:
                            value_to_name[val] = nm
                    else:
                        value_to_name[val] = nm
            except Exception:
                continue

        # Replace longer values first to avoid partial overlaps
        import re

        ordered_values = sorted(value_to_name.keys(), key=len, reverse=True)
        result = text
        for val in ordered_values:
            name = value_to_name[val]
            pattern = re.escape(val)
            placeholder = f"${{{name}}}"
            result = re.sub(pattern, placeholder, result)

        return result

    @functools.wraps(BaseSecretManager.ask, updated=())
    @log_manager_call(
        "SecretManager",
        "ask",
        payload_key="question",
        display_label="Checking credentials",
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
        _call_id: Optional[str] = None,
    ) -> SteerableToolHandle:
        # First, replace any known raw secret values with placeholders
        try:
            text = await self.to_placeholder(text)
        except Exception:
            pass

        client = new_llm_client()

        # Build tools for read-only inspection
        tools = dict(self.get_tools("ask"))
        _clar_queues = None
        _on_clar_req = None
        _on_clar_ans = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            from ..common.llm_helpers import make_request_clarification_tool

            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

            async def _on_clar_req(q: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "SecretManager",
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
                                "manager": "SecretManager",
                                "method": "ask",
                                "action": "clarification_answer",
                                "answer": ans,
                            },
                        ),
                    )
                except Exception:
                    pass

        # System message via prompt builder
        client.set_system_message(
            build_ask_prompt(tools=tools).to_list(),
        )

        handle = start_async_tool_loop(
            client,
            text,
            tools,
            loop_id=f"{self.__class__.__name__}.{self.ask.__name__}",
            parent_lineage=TOOL_LOOP_LINEAGE.get([]),
            parent_chat_context=_parent_chat_context,
            tool_policy=self._default_ask_tool_policy,
            response_format=response_format,
            handle_cls=(
                ReadOnlyAskGuardHandle if SETTINGS.UNIFY_READONLY_ASK_GUARD else None
            ),
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

    @functools.wraps(BaseSecretManager.update, updated=())
    @log_manager_call(
        "SecretManager",
        "update",
        payload_key="request",
        display_label="Updating credentials",
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
        _call_id: Optional[str] = None,
    ) -> SteerableToolHandle:
        # First, replace any known raw secret values with placeholders
        try:
            text = await self.to_placeholder(text)
        except Exception:
            pass

        client = new_llm_client()

        tools = dict(self.get_tools("update"))
        _clar_queues = None
        _on_clar_req = None
        _on_clar_ans = None
        if _clarification_up_q is not None and _clarification_down_q is not None:
            from ..common.llm_helpers import make_request_clarification_tool

            _clar_queues = (_clarification_up_q, _clarification_down_q)
            tools["request_clarification"] = make_request_clarification_tool(None, None)

            async def _on_clar_req(q: str):
                try:
                    await EVENT_BUS.publish(
                        Event(
                            type="ManagerMethod",
                            calling_id=_call_id,
                            payload={
                                "manager": "SecretManager",
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
                                "manager": "SecretManager",
                                "method": "update",
                                "action": "clarification_answer",
                                "answer": ans,
                            },
                        ),
                    )
                except Exception:
                    pass

        client.set_system_message(
            build_update_prompt(tools=tools).to_list(),
        )

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

    # --------------------- Tools (read-only) --------------------- #
    def get_credential(self, integration: str) -> str:
        """Return one raw credential by name.

        Parameters
        ----------
        integration : str
            Secret name to resolve.

        Raises
        ------
        KeyError
            If no credential with that name is stored.
        """
        rows = db.get_logs(
            context=self._ctx,
            filter=f"name == {integration!r}",
            limit=1,
        )
        value = (rows[0].entries or {}).get("value") if rows else None
        if not isinstance(value, str):
            raise KeyError(f"No credential named {integration!r}.")
        return value

    def _resolve_placeholders(self, text: str) -> str:
        """Return a copy of text with ${name} placeholders replaced by values.

        This helper performs direct Unify reads and never emits logs/events.
        Unknown names are left unchanged.
        """
        import re

        def repl(match: "re.Match[str]") -> str:
            name = match.group(1)
            try:
                return self.get_credential(name)
            except KeyError:
                pass
            return match.group(0)  # leave placeholder as-is when missing

        return re.sub(r"\$\{([^}]+)\}", repl, text)

    def _list_columns(
        self,
        *,
        include_types: bool = True,
    ) -> Dict[str, Any] | List[str]:
        """Return available columns for the secrets table.

        Parameters
        ----------
        include_types : bool, default True
            When True, returns a mapping ``{column_name: column_type}``.
            When False, returns a list of column names only.

        Returns
        -------
        Dict[str, Any] | List[str]
            Column map when ``include_types=True``; otherwise a list of names.
        """
        cols = self._store.get_columns()
        return cols if include_types else list(cols)

    def _sanitize_secret_references(
        self,
        references: Optional[Dict[str, str]],
    ) -> Optional[Dict[str, str]]:
        """Return a safe subset of references limited to description-based terms.

        Only allows:
        - Plain identifier "description"; or
        - Derived expressions whose placeholders are exclusively {description}.
        Any other term is dropped to avoid embedding sensitive columns like "value".
        """
        if not references:
            return references

        allowed: Dict[str, str] = {}
        for source_expr, ref_text in references.items():
            try:
                if is_plain_identifier(source_expr):
                    if source_expr == "description":
                        allowed[source_expr] = ref_text
                    continue

                # Derived expression – verify placeholders are only {description}
                import re as _re

                placeholders = _re.findall(
                    r"\{\s*([a-zA-Z_][\w]*)\s*\}",
                    source_expr or "",
                )
                if placeholders and all(ph == "description" for ph in placeholders):
                    allowed[source_expr] = ref_text
            except Exception:
                # Skip malformed expressions defensively
                continue

        return allowed or None

    def _search_secrets(
        self,
        *,
        references: Optional[Dict[str, str]] = None,
        k: int = 10,
    ) -> List[Secret]:
        """Semantic search over secrets using the description embedding.

        Parameters
        ----------
        references : Dict[str, str] | None, default None
            Mapping of source expressions to reference text. For this manager
            use a column name like ``"description"`` to search over secret descriptions.
            When None or empty, returns most-recent rows.
        k : int, default 10
            Maximum number of results to return. Must be <= 1000.

        Returns
        -------
        List[Secret]
            Up to ``k`` redacted Secret models (``value`` is never populated).
        """
        # Sanitize references to avoid embedding sensitive fields like "value"
        safe_refs = self._sanitize_secret_references(references)

        self._ensure_description_vector()
        rows = federated_ranked_search(
            [self._redacted_search_context()],
            safe_refs,
            limit=k,
            backfill=True,
        )
        return [self._redacted_secret(row) for row in rows]

    def _redacted_search_context(self) -> FederatedSearchContext:
        """Return the Secrets table as a search context that never exposes values."""
        return FederatedSearchContext(
            context=self._ctx,
            source=self._ctx,
            allowed_fields=["secret_id", "name", "description"],
        )

    @staticmethod
    def _redacted_secret(row: Dict[str, Any]) -> Secret:
        """Build a Secret model from a search row with the value blanked."""
        return Secret(
            secret_id=(
                int(row.get("secret_id")) if row.get("secret_id") is not None else -1
            ),
            name=row.get("name"),
            value="",
            description=row.get("description") or "",
        )

    def _filter_secrets(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Secret]:
        """Filter secrets using a boolean expression evaluated per row.

        Parameters
        ----------
        filter : str | None, default None
            A Python expression evaluated with column names in scope (e.g.,
            ``"name == 'openai_api_key'"``). When None, returns all rows.
        offset : int, default 0
            Zero-based index of the first result to include.
        limit : int, default 100
            Maximum number of rows to return. Must be <= 1000.

        Returns
        -------
        List[Secret]
            Matching Secret models with ``value`` redacted.
        """
        rows = federated_filter(
            [self._redacted_search_context()],
            filter=normalize_filter_expr(filter),
            offset=offset,
            limit=limit,
        )
        return [self._redacted_secret(row) for row in rows]

    def _list_secret_keys(self) -> List[str]:
        """Return all available secret names (keys) stored in Unify.

        Returns
        -------
        List[str]
            Sorted, unique list of secret names currently present in storage.
        """
        try:
            rows = db.get_logs(context=self._ctx, from_fields=["name"])
        except Exception:
            rows = []
        names = {
            nm
            for lg in rows
            if isinstance((nm := (lg.entries or {}).get("name")), str) and nm
        }
        return sorted(names)

    # --------------------- Tools (mutations) --------------------- #
    def _invalidate_credential_processes(self) -> None:
        """Drop stateful execution pools so subprocesses reopen with fresh env."""
        from unify.function_manager.function_manager import VenvPool

        VenvPool.invalidate_all_pools()

    def _create_secret(
        self,
        *,
        name: str,
        value: str,
        description: Optional[str] = None,
    ) -> ToolOutcome:
        """Create and persist a new secret.

        Parameters
        ----------
        name : str
            Unique identifier for the secret. Used as the placeholder name.
        value : str
            Raw secret value to store (never exposed to LLMs). Stored
            credentials are mirrored into the local ``.env`` file.
        description : str | None, default None
            Optional human-readable description.

        Returns
        -------
        ToolOutcome
            A standard outcome dict naming the created secret.
        """
        assert name and value, "Both name and value are required."

        # Enforce uniqueness of name
        existing = db.get_logs(
            context=self._ctx,
            filter=f"name == {name!r}",
            limit=1,
            return_ids_only=True,
        )
        assert not existing, f"Secret with name '{name}' already exists."

        # Write secret (store raw value in backend, but never surface to LLM)
        entries = {
            "name": name,
            "value": value,
            "description": description or "",
        }
        unity_log(
            context=self._ctx,
            **entries,
            new=True,
            mutable=True,
            stamp_authoring=True,
        )

        try:
            self._env_set(name, value)
        except Exception:
            pass
        finally:
            self._invalidate_credential_processes()

        return {"outcome": "secret created", "details": {"name": name}}

    def _update_secret(
        self,
        *,
        name: str,
        value: Optional[str] = None,
        description: Optional[str] = None,
    ) -> ToolOutcome:
        """Update fields of an existing secret.

        Parameters
        ----------
        name : str
            Secret name to update.
        value : str | None, default None
            New raw value (optional). When provided it overwrites the existing
            value; pooled subprocesses pick up the new value on next invocation.
        description : str | None, default None
            New description (optional).

        Returns
        -------
        ToolOutcome
            A standard outcome dict naming the updated secret.
        """
        # Find target log id
        ids = db.get_logs(
            context=self._ctx,
            filter=f"name == {name!r}",
            limit=2,
            return_ids_only=True,
        )
        if not ids:
            raise ValueError(f"No secret found with name '{name}'.")
        if len(ids) > 1:
            raise RuntimeError(f"Multiple secrets found with name '{name}'.")
        log_id = ids[0]

        updates: Dict[str, Any] = {}
        if description is not None:
            updates["description"] = description
        if value is not None:
            updates["value"] = value

        if not updates:
            raise ValueError("No updates provided.")

        db.update_logs(
            logs=[log_id],
            context=self._ctx,
            entries=updates,
            overwrite=True,
        )

        try:
            if value is not None:
                self._env_set(name, value)
        except Exception:
            pass
        finally:
            self._invalidate_credential_processes()

        return {"outcome": "secret updated", "details": {"name": name}}

    def _delete_secret(self, *, name: str) -> ToolOutcome:
        """Delete a secret by name.

        Parameters
        ----------
        name : str
            The secret name to remove. Removing a credential breaks any
            function or integration that depends on it; do not delete one
            unless the decision is to rotate or retire it.

        Returns
        -------
        ToolOutcome
            A standard outcome dict naming the deleted secret.
        """
        ids = db.get_logs(
            context=self._ctx,
            filter=f"name == {name!r}",
            limit=2,
            return_ids_only=True,
        )
        if not ids:
            raise ValueError(f"No secret found with name '{name}'.")
        if len(ids) > 1:
            raise RuntimeError(f"Multiple secrets found with name '{name}'.")
        db.delete_logs(context=self._ctx, logs=ids[0])
        try:
            self._env_remove(name)
        except Exception:
            pass
        finally:
            self._invalidate_credential_processes()
        return {"outcome": "secret deleted", "details": {"name": name}}
