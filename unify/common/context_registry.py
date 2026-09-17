import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Final, List, Optional, Type, Union

from pydantic import BaseModel

from unify import db
from unify.common.authorship import SHARED_SCOPED_TABLES, fields_with_authoring
from unify.common.context_store import create_context_checked
from unify.common.state_managers import BaseStateManager

_log = logging.getLogger(__name__)

_SHARED_SCOPED_TABLES: Final[frozenset[str]] = SHARED_SCOPED_TABLES


class TableContext(BaseModel):
    name: str
    description: str
    fields: Optional[Any] = None
    unique_keys: Optional[Dict[str, str]] = None
    auto_counting: Optional[Dict[str, Optional[str]]] = None
    foreign_keys: Optional[List[Dict[str, Any]]] = None


class ContextRegistry:
    """Resolves and provisions each manager's tables under the session root.

    Every manager declares its tables in ``Config.required_contexts``. The
    registry maps ``(manager, table)`` to the fully-qualified context path
    under the active root (``{user}/{assistant}``), creating the context and
    its declared fields on first use.
    """

    _setup_complete = False
    _registry: Dict[tuple[str, str], str] = {}
    _base_context: Optional[str] = None

    @staticmethod
    def _get_active_context() -> str:
        active_context = db.get_active_context()
        assert (
            active_context["read"] == active_context["write"]
        ), "Read and write contexts must be the same"
        return active_context["read"]

    @staticmethod
    def _get_manager_name(
        manager: Union[BaseStateManager, Type[BaseStateManager]],
    ) -> str:
        try:
            return manager.__name__
        except AttributeError:
            return type(manager).__name__

    @classmethod
    def _is_shared_scoped(cls, table_name: str) -> bool:
        """Return whether a table carries authorship columns."""
        if table_name in _SHARED_SCOPED_TABLES:
            return True
        parent = table_name
        while "/" in parent:
            parent = parent.rsplit("/", 1)[0]
            if parent in _SHARED_SCOPED_TABLES:
                return True
        return False

    @classmethod
    def _session_root(cls, manager_name: str, table_name: str) -> str:
        base = cls._base_context
        if not base:
            try:
                base = cls._get_active_context()
            except Exception:
                base = None
        if not base:
            raise RuntimeError(
                f"Cannot resolve context for {manager_name}.{table_name}: "
                "no base context available (ContextRegistry.setup() has not "
                "run or the active context is empty)",
            )
        cls._base_context = base
        return base

    @staticmethod
    def is_missing_base_context_error(exc: BaseException) -> bool:
        """Return whether *exc* indicates missing root-context setup."""
        return isinstance(exc, RuntimeError) and (
            "no base context available" in str(exc)
        )

    @classmethod
    def set_base_context(cls, base_context: str) -> None:
        """Cache an already-resolved base context root for worker tasks."""
        if not base_context:
            return
        cls._base_context = base_context

    @classmethod
    def _get_contexts_for_manager(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        current_context: str,
    ) -> Dict[str, Dict]:
        """Extract the contexts for a manager, resolving context names to fully qualified names."""
        assert hasattr(
            manager,
            "Config",
        ), f"Manager {cls._get_manager_name(manager)} must have a Config class attribute"
        assert hasattr(
            manager.Config,
            "required_contexts",
        ), "Config must have a required_contexts class attribute"

        out = {}

        for context in manager.Config.required_contexts:
            # Copy foreign keys so the class-level config never accumulates
            # a prefix per resolution.
            resolved_foreign_keys = None
            if context.foreign_keys:
                resolved_foreign_keys = []
                for foreign_key in context.foreign_keys:
                    fk_copy = foreign_key.copy()
                    fk_copy["references"] = (
                        f"{current_context}/{foreign_key['references']}"
                    )
                    resolved_foreign_keys.append(fk_copy)
            context_fields = context.fields
            if cls._is_shared_scoped(context.name):
                context_fields = fields_with_authoring(context_fields)
                context = context.model_copy(update={"fields": context_fields})

            out[context.name] = {
                "resolved_name": f"{current_context}/{context.name}",
                "table_context": context,
                "resolved_foreign_keys": resolved_foreign_keys,
                "root_context": current_context,
            }
        return out

    @classmethod
    def declared_tables(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
    ) -> frozenset[str]:
        """Table names a manager declares in ``Config.required_contexts``."""
        required = getattr(getattr(manager, "Config", None), "required_contexts", None)
        return frozenset(context.name for context in required or ())

    @classmethod
    def _get_managers(cls) -> List[Union[BaseStateManager, Type[BaseStateManager]]]:
        """Get the list of managers that have required contexts."""
        from unify.contact_manager.contact_manager import ContactManager
        from unify.data_manager.data_manager import DataManager
        from unify.file_manager.managers.file_manager import FileManager
        from unify.function_manager.function_manager import FunctionManager
        from unify.guidance_manager.guidance_manager import GuidanceManager
        from unify.image_manager.image_manager import ImageManager
        from unify.knowledge_manager.knowledge_manager import KnowledgeManager
        from unify.secret_manager.secret_manager import SecretManager
        from unify.transcript_manager.transcript_manager import TranscriptManager

        return [
            ContactManager,
            KnowledgeManager,
            TranscriptManager,
            ImageManager,
            GuidanceManager,
            SecretManager,
            FunctionManager,
            DataManager,
            FileManager,
        ]

    @classmethod
    def _create_context_wrapper(
        cls,
        manager_name: str,
        entry: Dict,
    ) -> str:
        """Create the context, ensure its fields exist and record it."""
        table = entry["table_context"]
        target_name = entry["resolved_name"]
        create_context_checked(
            target_name,
            unique_keys=table.unique_keys,
            auto_counting=table.auto_counting,
            description=table.description,
            foreign_keys=entry.get("resolved_foreign_keys"),
        )
        if table.fields:
            db.create_fields(fields=table.fields, context=target_name)

        cls._registry[(manager_name, table.name)] = target_name
        return target_name

    @classmethod
    def refresh(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        ctx_name: str,
    ) -> Optional[str]:
        """Refresh the context by forgetting it and then getting it again."""
        cls.forget(manager, ctx_name)
        return cls.get_context(manager, ctx_name)

    @classmethod
    def forget(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        ctx_name: str,
    ) -> None:
        """Remove the context from the registry."""
        manager_name = cls._get_manager_name(manager)
        for key in list(cls._registry):
            if key[0] == manager_name and key[1] == ctx_name:
                cls._registry.pop(key, None)

    @classmethod
    def clear(cls) -> None:
        """Remove all cached contexts from the registry, primarily for test isolation."""
        cls._registry.clear()
        cls._setup_complete = False
        cls._base_context = None

    @classmethod
    def _ensure_context(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        table_name: str,
        root_context: str,
    ) -> str:
        manager_name = cls._get_manager_name(manager)
        key = (manager_name, table_name)
        target_name = cls._registry.get(key)
        if target_name is not None:
            return target_name

        contexts = cls._get_contexts_for_manager(manager, root_context)
        return cls._create_context_wrapper(manager_name, contexts[table_name])

    @classmethod
    def root(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        table_name: str,
    ) -> str:
        """Provision *table_name* for *manager* and return the session root.

        Every manager's tables live under the one active root
        (``{user}/{assistant}``); callers that address sub-tables compose
        ``f"{root}/{table}"`` from it.
        """
        manager_name = cls._get_manager_name(manager)
        root = cls._session_root(manager_name, table_name)
        cls._ensure_context(manager, table_name, root)
        return root

    @classmethod
    def get_context(
        cls,
        manager: Union[BaseStateManager, Type[BaseStateManager]],
        ctx_name: str,
    ) -> Optional[str]:
        """Get the manager's context, creating it if it doesn't exist."""
        manager_name = cls._get_manager_name(manager)
        return cls._ensure_context(
            manager,
            ctx_name,
            cls._session_root(manager_name, ctx_name),
        )

    @classmethod
    def _provision_managers(
        cls,
        managers: List[Union[Type[BaseStateManager], BaseStateManager]],
        base: str,
    ) -> None:
        """Provision every required context of ``managers`` against *base*."""
        cls._base_context = base

        with ThreadPoolExecutor() as executor:
            futures = []
            for manager in managers:
                manager_name = cls._get_manager_name(manager)
                for entry in cls._get_contexts_for_manager(manager, base).values():
                    futures.append(
                        executor.submit(
                            cls._create_context_wrapper,
                            manager_name,
                            entry,
                        ),
                    )

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    _log.warning("Context creation failed (will retry lazily): %s", e)

    @classmethod
    def setup(cls):
        """Setup the context handler by creating the contexts for all managers."""
        if cls._setup_complete:
            return

        cls._provision_managers(cls._get_managers(), cls._get_active_context())
        cls._setup_complete = True

    @classmethod
    def setup_for_managers(
        cls,
        managers: List[Union[Type[BaseStateManager], BaseStateManager]],
        *,
        base_context: Optional[str] = None,
    ) -> None:
        """Provision contexts for a specific subset of managers.

        Unlike :meth:`setup` this does not set ``_setup_complete`` so that a
        later full ``setup()`` call still runs normally.
        """
        cls._provision_managers(
            managers,
            base_context or cls._get_active_context(),
        )

    @classmethod
    def get_known_base_contexts(cls) -> List[str]:
        """Return the unresolved table names declared across all managers."""
        base_contexts = set()
        for manager in cls._get_managers():
            for table_ctx in manager.Config.required_contexts:
                base_contexts.add(table_ctx.name)
        return sorted(base_contexts)
