from unittest.mock import patch

import pytest
from unisdk.logs import CONTEXT_READ, CONTEXT_WRITE

from unity.common.context_registry import ContextRegistry, TableContext
from unity.common.tool_outcome import ToolErrorException
from unity.session_details import SESSION_DETAILS


class RegistryExampleManager:
    class Config:
        required_contexts = [
            TableContext(name="Tasks", description="Scheduled work items."),
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
            TableContext(name="Dashboards/Tiles", description="Dashboard tile rows."),
            TableContext(
                name="Dashboards/Layouts",
                description="Dashboard layout rows.",
            ),
            TableContext(name="Transcripts", description="Conversation messages."),
            TableContext(name="Exchanges", description="Conversation exchanges."),
            TableContext(name="Images", description="Stored images."),
        ]


@pytest.fixture(autouse=True)
def reset_context_registry():
    ContextRegistry.clear()
    SESSION_DETAILS.reset()
    CONTEXT_READ.set("user123/42")
    CONTEXT_WRITE.set("user123/42")
    yield
    ContextRegistry.clear()
    SESSION_DETAILS.reset()


def test_write_root_resolves_personal_and_space_destinations():
    SESSION_DETAILS.team_ids = [3, 7]

    with patch("unity.common.context_registry._create_context_with_retry"):
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                "Tasks",
                destination=None,
            )
            == "user123/42"
        )
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                "Tasks",
                destination="personal",
            )
            == "user123/42"
        )
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                "Contacts",
                destination="team:7",
            )
            == "Teams/7"
        )


def test_owner_for_root_maps_personal_and_team_roots():
    SESSION_DETAILS.assistant.agent_id = 99
    assert ContextRegistry._owner_for_root("Teams/7") == ("team", 7)
    assert ContextRegistry._owner_for_root("Personal") == ("assistant", 99)
    # No assigned assistant -> defer to backend name-inference.
    SESSION_DETAILS.assistant.agent_id = None
    assert ContextRegistry._owner_for_root("Personal") == (None, None)


def test_provisioning_passes_explicit_owner_scope():
    """Context provisioning forwards explicit owner scope per root type."""
    SESSION_DETAILS.team_ids = [7]
    SESSION_DETAILS.assistant.agent_id = 42

    with patch(
        "unity.common.context_registry._create_context_with_retry",
    ) as mock_create:
        ContextRegistry.write_root(RegistryExampleManager, "Tasks", destination=None)
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Contacts",
            destination="team:7",
        )

    owners = {
        (call.kwargs.get("owner_scope"), call.kwargs.get("owner_id"))
        for call in mock_create.call_args_list
    }
    assert ("assistant", 42) in owners
    assert ("team", 7) in owners


def test_write_root_resolves_all_manager_destination_tables_to_shared_teams():
    class DestinationAwareManager:
        class Config:
            required_contexts = [
                TableContext(
                    name="Knowledge",
                    description="Structured assistant memory.",
                ),
                TableContext(
                    name="Guidance",
                    description="Assistant behavior guidance.",
                ),
                TableContext(
                    name="Functions/Compositional",
                    description="Assistant-authored functions.",
                ),
                TableContext(
                    name="Functions/VirtualEnvs",
                    description="Custom function environments.",
                ),
                TableContext(
                    name="Functions/Primitives",
                    description="Runtime-provided primitive functions.",
                ),
                TableContext(
                    name="Functions/Meta",
                    description="Function synchronization metadata.",
                ),
            ]

    SESSION_DETAILS.team_ids = [37]

    with patch("unity.common.context_registry._create_context_with_retry"):
        for table_name in (
            "Knowledge",
            "Guidance",
            "Functions/Compositional",
            "Functions/VirtualEnvs",
            "Functions/Primitives",
            "Functions/Meta",
        ):
            assert (
                ContextRegistry.write_root(
                    DestinationAwareManager,
                    table_name,
                    destination="team:37",
                )
                == "Teams/37"
            )


def test_invalid_destination_raises_structured_error():
    SESSION_DETAILS.team_ids = [3, 7]

    with pytest.raises(ToolErrorException) as exc_info:
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Tasks",
            destination="team:999",
        )

    assert exc_info.value.payload["error_kind"] == "invalid_destination"
    assert exc_info.value.payload["details"]["destination"] == "team:999"
    assert exc_info.value.payload["details"]["team_ids"] == [3, 7]


def test_read_roots_returns_personal_then_sorted_teams():
    SESSION_DETAILS.team_ids = [7, 3]

    with patch("unity.common.context_registry._create_context_with_retry"):
        task_roots = ContextRegistry.read_roots(RegistryExampleManager, "Tasks")
        contact_roots = ContextRegistry.read_roots(RegistryExampleManager, "Contacts")

    assert task_roots == ["user123/42", "Teams/3", "Teams/7"]
    assert contact_roots == ["user123/42", "Teams/3", "Teams/7"]


def test_files_data_and_blacklist_are_shared_scoped():
    SESSION_DETAILS.team_ids = [7]

    with patch("unity.common.context_registry._create_context_with_retry"):
        assert ContextRegistry.read_roots(RegistryExampleManager, "Secrets") == [
            "user123/42",
            "Teams/7",
        ]
        assert ContextRegistry.read_roots(RegistryExampleManager, "FileRecords") == [
            "user123/42",
            "Teams/7",
        ]
        assert ContextRegistry.read_roots(RegistryExampleManager, "Files") == [
            "user123/42",
            "Teams/7",
        ]
        assert ContextRegistry.read_roots(RegistryExampleManager, "Data") == [
            "user123/42",
            "Teams/7",
        ]
        assert ContextRegistry.read_roots(RegistryExampleManager, "BlackList") == [
            "user123/42",
            "Teams/7",
        ]


def test_resolve_root_supports_dashboard_tables_without_provisioning():
    SESSION_DETAILS.team_ids = [7]

    with patch(
        "unity.common.context_registry._create_context_with_retry",
    ) as create_context:
        manager_name, root_identity, root_context = ContextRegistry.resolve_root(
            RegistryExampleManager,
            "Dashboards/Tiles",
            destination="team:7",
        )

    assert manager_name == "RegistryExampleManager"
    assert root_identity == "Teams/7"
    assert root_context == "Teams/7"
    create_context.assert_not_called()
    assert ContextRegistry._registry == {}


@pytest.mark.parametrize("table_name", ["Transcripts", "Exchanges", "Images"])
def test_media_tables_are_shared_scoped(table_name: str):
    SESSION_DETAILS.team_ids = [7, 3]

    with patch("unity.common.context_registry._create_context_with_retry"):
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                table_name,
                destination=None,
            )
            == "user123/42"
        )
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                table_name,
                destination="team:7",
            )
            == "Teams/7"
        )
        assert ContextRegistry.read_roots(RegistryExampleManager, table_name) == [
            "user123/42",
            "Teams/3",
            "Teams/7",
        ]


def test_lazy_provisioning_is_cached_per_root():
    SESSION_DETAILS.team_ids = [7]

    with patch(
        "unity.common.context_registry._create_context_with_retry",
    ) as create_context:
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Tasks",
            destination="team:7",
        )
        ContextRegistry.write_root(
            RegistryExampleManager,
            "Tasks",
            destination="team:7",
        )

    create_context.assert_called_once()
    assert create_context.call_args.args[0] == "Teams/7/Tasks"
    assert (
        ContextRegistry._registry[("RegistryExampleManager", "Tasks", "Teams/7")]
        == "Teams/7/Tasks"
    )


@pytest.mark.parametrize(
    "table_name",
    [
        "Tasks",
        "Contacts",
        "Secrets",
        "Knowledge",
        "Guidance",
        "Functions/Compositional",
        "Functions/Meta",
        "Functions/Primitives",
        "Functions/VirtualEnvs",
        "FileRecords",
        "Files",
        "Data",
        "BlackList",
        "Dashboards/Tiles",
        "Dashboards/Layouts",
        "Transcripts",
        "Exchanges",
        "Images",
    ],
)
def test_landed_shared_tables_accept_space_destinations(table_name: str):
    SESSION_DETAILS.team_ids = [7]

    with patch("unity.common.context_registry._create_context_with_retry"):
        assert (
            ContextRegistry.write_root(
                RegistryExampleManager,
                table_name,
                destination="team:7",
            )
            == "Teams/7"
        )
