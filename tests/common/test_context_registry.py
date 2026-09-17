from unittest.mock import patch

import pytest
from unify.db import CONTEXT_READ, CONTEXT_WRITE

from unify.common.context_registry import (
    PERSONAL_ROOT_IDENTITY,
    ContextRegistry,
    TableContext,
)
from unify.common.tool_outcome import ToolErrorException
from unify.session_details import SESSION_DETAILS


class RegistryExampleManager:
    class Config:
        required_contexts = [
            TableContext(
                name="Contacts",
                description="People and organizations the assistant knows.",
            ),
            TableContext(name="Secrets", description="Private credentials."),
            TableContext(name="Knowledge", description="Structured knowledge tables."),
            TableContext(name="Guidance", description="Assistant guidance rules."),
            TableContext(
                name="Functions/Compositional",
                description="Compositional functions.",
            ),
            TableContext(name="Functions/Meta", description="Function metadata."),
            TableContext(
                name="Functions/Primitives",
                description="Primitive functions.",
            ),
            TableContext(
                name="Functions/VirtualEnvs",
                description="Function virtual environments.",
            ),
            TableContext(name="FileRecords", description="File metadata records."),
            TableContext(name="Files", description="File payload rows."),
            TableContext(name="Data", description="User data tables."),
            TableContext(name="BlackList", description="Blocked contact details."),
            TableContext(name="Transcripts", description="Conversation messages."),
            TableContext(name="Exchanges", description="Conversation exchanges."),
            TableContext(name="Images", description="Stored images."),
            TableContext(name="SearchCache", description="Non-shared runtime cache."),
        ]


_ALL_TABLES = [ctx.name for ctx in RegistryExampleManager.Config.required_contexts]


@pytest.fixture(autouse=True)
def reset_context_registry():
    ContextRegistry.clear()
    SESSION_DETAILS.reset()
    CONTEXT_READ.set("user123/42")
    CONTEXT_WRITE.set("user123/42")
    yield
    ContextRegistry.clear()
    SESSION_DETAILS.reset()


@pytest.fixture
def provision():
    """Stub the store calls provisioning makes; yields the context-create mock."""
    with (
        patch("unify.common.context_registry.create_context_checked") as create_context,
        patch("unify.common.context_registry.db.create_fields"),
    ):
        yield create_context


@pytest.mark.parametrize("destination", [None, "personal", ""])
def test_write_root_resolves_to_the_session_root(destination, provision):
    assert (
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Contacts",
            destination=destination,
        )
        == "user123/42"
    )


@pytest.mark.parametrize("destination", ["team:7", "shared", 7])
def test_invalid_destination_raises_structured_error(destination):
    with pytest.raises(ToolErrorException) as exc_info:
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Contacts",
            destination=destination,
        )

    assert exc_info.value.payload["error_kind"] == "invalid_destination"
    assert exc_info.value.payload["details"]["destination"] == destination
    assert ContextRegistry._registry == {}


@pytest.mark.parametrize("table_name", _ALL_TABLES)
def test_read_roots_is_the_single_session_root(table_name: str, provision):
    assert ContextRegistry.read_roots(RegistryExampleManager, table_name) == [
        "user123/42",
    ]


def test_resolve_root_does_not_provision(provision):
    manager_name, root_identity, root_context = ContextRegistry.resolve_root(
        RegistryExampleManager,
        "Knowledge",
        destination=None,
    )

    assert manager_name == "RegistryExampleManager"
    assert root_identity == PERSONAL_ROOT_IDENTITY
    assert root_context == "user123/42"
    provision.assert_not_called()
    assert ContextRegistry._registry == {}


def test_get_context_returns_the_fully_qualified_table(provision):
    assert (
        ContextRegistry.get_context(RegistryExampleManager, "Contacts")
        == "user123/42/Contacts"
    )
    assert (
        ContextRegistry.get_context(RegistryExampleManager, "SearchCache")
        == "user123/42/SearchCache"
    )


def test_lazy_provisioning_is_cached_per_table(provision):
    ContextRegistry.write_root(RegistryExampleManager, "Contacts")
    ContextRegistry.write_root(RegistryExampleManager, "Contacts")
    ContextRegistry.get_context(RegistryExampleManager, "Contacts")

    provision.assert_called_once()
    assert provision.call_args.args[0] == "user123/42/Contacts"
    assert (
        ContextRegistry._registry[
            ("RegistryExampleManager", "Contacts", PERSONAL_ROOT_IDENTITY)
        ]
        == "user123/42/Contacts"
    )


def test_shared_scoped_tables_gain_the_authoring_field():
    from unify.common.authorship import AUTHORING_ASSISTANT_ID_FIELD

    contexts = ContextRegistry._get_contexts_for_manager(
        RegistryExampleManager,
        "user123/42",
    )
    shared_fields = contexts["Contacts"]["table_context"].fields
    assert AUTHORING_ASSISTANT_ID_FIELD in shared_fields
    assert contexts["SearchCache"]["table_context"].fields is None


def test_forget_and_refresh_reprovision_the_table(provision):
    ContextRegistry.get_context(RegistryExampleManager, "Contacts")
    ContextRegistry.forget(RegistryExampleManager, "Contacts")
    assert ContextRegistry._registry == {}
    assert (
        ContextRegistry.refresh(RegistryExampleManager, "Contacts")
        == "user123/42/Contacts"
    )

    assert provision.call_count == 2


def test_missing_base_context_is_a_recognisable_error():
    CONTEXT_READ.set("")
    CONTEXT_WRITE.set("")

    with pytest.raises(RuntimeError) as exc_info:
        ContextRegistry.get_context(RegistryExampleManager, "Contacts")

    assert ContextRegistry.is_missing_base_context_error(exc_info.value)
    assert not ContextRegistry.is_missing_base_context_error(ValueError("x"))
