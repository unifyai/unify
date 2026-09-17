from unittest.mock import patch

import pytest
from unify.db import CONTEXT_READ, CONTEXT_WRITE

from unify.common.context_registry import ContextRegistry, TableContext
from unify.session_details import SESSION_DETAILS


class RegistryExampleManager:
    class Config:
        required_contexts = [
            TableContext(name="Guidance", description="Assistant procedures."),
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
            TableContext(name="Chat/Messages", description="Chat history."),
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


@pytest.mark.parametrize("table_name", _ALL_TABLES)
def test_root_is_the_session_root(table_name: str, provision):
    assert ContextRegistry.root(RegistryExampleManager, table_name) == "user123/42"


def test_get_context_returns_the_fully_qualified_table(provision):
    assert (
        ContextRegistry.get_context(RegistryExampleManager, "Guidance")
        == "user123/42/Guidance"
    )
    assert (
        ContextRegistry.get_context(RegistryExampleManager, "Chat/Messages")
        == "user123/42/Chat/Messages"
    )


def test_lazy_provisioning_is_cached_per_table(provision):
    ContextRegistry.root(RegistryExampleManager, "Guidance")
    ContextRegistry.root(RegistryExampleManager, "Guidance")
    ContextRegistry.get_context(RegistryExampleManager, "Guidance")

    provision.assert_called_once()
    assert provision.call_args.args[0] == "user123/42/Guidance"
    assert (
        ContextRegistry._registry[("RegistryExampleManager", "Guidance")]
        == "user123/42/Guidance"
    )


def test_forget_and_refresh_reprovision_the_table(provision):
    ContextRegistry.get_context(RegistryExampleManager, "Guidance")
    ContextRegistry.forget(RegistryExampleManager, "Guidance")
    assert ContextRegistry._registry == {}
    assert (
        ContextRegistry.refresh(RegistryExampleManager, "Guidance")
        == "user123/42/Guidance"
    )

    assert provision.call_count == 2


def test_missing_base_context_is_a_recognisable_error():
    CONTEXT_READ.set("")
    CONTEXT_WRITE.set("")

    with pytest.raises(RuntimeError, match="no base context available"):
        ContextRegistry.get_context(RegistryExampleManager, "Guidance")
