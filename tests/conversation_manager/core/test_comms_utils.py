"""
tests/conversation_manager/core/test_comms_utils.py
=============================================================

Unit tests for comms_utils functions, particularly attachment handling.

Marked ``real_comms_functions`` to opt out of the universal comms stub
in the parent conftest.  These tests call the real ``comms_utils``
implementations with mocked dependencies (aiohttp, SESSION_DETAILS, etc.).

Every test that mocks aiohttp asserts the **URL and HTTP method** used for
the request, not just the response.  This catches wrong-service bugs (like
posting to COMMS_URL when the endpoint lives on the adapters service).
"""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from unify.conversation_manager.domains import comms_utils

pytestmark = pytest.mark.real_comms_functions

COMMS_URL = "http://comms.test:8080"
ADAPTERS_URL = "http://adapters.test:8081"


def _mock_aiohttp_session(response_json=None, raise_on_status=None, status=200):
    """Build a mock aiohttp session + response pair."""
    import json

    mock_response = MagicMock()
    mock_response.status = status
    if raise_on_status:
        mock_response.raise_for_status = MagicMock(side_effect=raise_on_status)
    else:
        mock_response.raise_for_status = MagicMock()
    mock_response.json = AsyncMock(return_value=response_json or {})
    mock_response.text = AsyncMock(
        return_value=json.dumps(response_json or {}),
    )

    mock_session = MagicMock()
    mock_session.post = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)),
    )
    mock_session.get = MagicMock(
        return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)),
    )
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    return mock_session


class TestUploadUnifyAttachment:
    """Tests for upload_unify_attachment function."""

    @pytest.mark.asyncio
    async def test_upload_success(self):
        """Successfully uploads file and returns attachment details."""
        mock_session = _mock_aiohttp_session(
            response_json={
                "id": "test-uuid-123",
                "filename": "document.pdf",
                "url": "https://storage.googleapis.com/signed-url",
            },
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.agent_id = 42
            mock_settings.conversation.ADAPTERS_URL = ADAPTERS_URL

            result = await comms_utils.upload_unify_attachment(
                file_content=b"test file content",
                filename="document.pdf",
            )

            assert result["id"] == "test-uuid-123"
            assert result["filename"] == "document.pdf"
            assert "storage.googleapis.com" in result["url"]

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{ADAPTERS_URL}/unify/attachment"

    @pytest.mark.asyncio
    async def test_upload_int_agent_id_serializes_form_data(self):
        """Integer agent_id must be string-coerced for multipart form serialization.

        aiohttp.FormData cannot serialize int values — calling form_data()
        raises LookupError("Can not serialize value type: <class 'int'>").
        This catches the regression from the str→int agent_id migration.
        """
        mock_session = _mock_aiohttp_session(
            response_json={"id": "uuid", "filename": "f.txt", "url": "https://url"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.agent_id = 42
            mock_settings.conversation.ADAPTERS_URL = ADAPTERS_URL

            await comms_utils.upload_unify_attachment(
                file_content=b"test content",
                filename="file.txt",
            )

            form_data = mock_session.post.call_args.kwargs["data"]
            form_data()

    @pytest.mark.asyncio
    async def test_upload_with_custom_assistant_id(self):
        """Uses provided assistant_id instead of session default."""
        mock_session = _mock_aiohttp_session(
            response_json={"id": "uuid", "filename": "file.txt", "url": "https://url"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.conversation.ADAPTERS_URL = ADAPTERS_URL

            result = await comms_utils.upload_unify_attachment(
                file_content=b"content",
                filename="file.txt",
                assistant_id="custom-assistant-id",
            )

            assert result["id"] == "uuid"
            mock_session.post.assert_called_once()
            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{ADAPTERS_URL}/unify/attachment"

    @pytest.mark.asyncio
    async def test_upload_failure_returns_error(self):
        """Returns error dict when upload fails."""
        mock_session = _mock_aiohttp_session(
            response_json={"error": "Connection refused"},
            status=503,
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.agent_id = 42
            mock_settings.conversation.ADAPTERS_URL = ADAPTERS_URL

            result = await comms_utils.upload_unify_attachment(
                file_content=b"content",
                filename="file.txt",
            )

            assert result.get("success") is False
            assert "error" in result

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{ADAPTERS_URL}/unify/attachment"

    @pytest.mark.asyncio
    async def test_upload_posts_to_adapters_not_comms_app(self):
        """The /unify/attachment endpoint is on the adapters service.

        COMMS_URL points to the comms-app (phone, gmail, infra routes).
        The attachment upload endpoint lives on the separate adapters service.
        Using COMMS_URL results in a 404 because the comms-app has no /unify/* routes.
        """
        mock_session = _mock_aiohttp_session(
            response_json={"id": "uuid", "filename": "file.txt", "url": "https://url"},
        )

        comms_app_url = "http://comms.test:8080"
        adapters_url = "http://adapters.test:8081"

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.agent_id = 42
            mock_settings.conversation.COMMS_URL = comms_app_url
            mock_settings.conversation.ADAPTERS_URL = adapters_url

            await comms_utils.upload_unify_attachment(
                file_content=b"content",
                filename="file.txt",
            )

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url.startswith(adapters_url), (
                f"upload_unify_attachment must post to ADAPTERS_URL "
                f"({adapters_url}), not COMMS_URL ({comms_app_url}). "
                f"Got: {posted_url}"
            )
            assert posted_url == f"{adapters_url}/unify/attachment"


class TestSendSms:
    """Tests for send_sms_message_via_number."""

    @pytest.mark.asyncio
    async def test_send_sms_url_and_body(self):
        """Posts to /phone/send-text on COMMS_URL with correct JSON body."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "sid": "SM123"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.number = "+15551234567"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            result = await comms_utils.send_sms_message_via_number(
                to_number="+15559876543",
                content="Hello from tests",
            )

            assert result["success"] is True

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{COMMS_URL}/phone/send-text"

            payload = mock_session.post.call_args.kwargs["json"]
            assert payload["From"] == "+15551234567"
            assert payload["To"] == "+15559876543"
            assert payload["Body"] == "Hello from tests"

    @pytest.mark.asyncio
    async def test_send_sms_collapses_hard_wrapped_body(self):
        """Hard-wrapped SMS bodies are normalized before transport."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "sid": "SM123"},
        )
        wrapped = "Line one of the clue\nLine two of the clue"

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.number = "+15551234567"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            await comms_utils.send_sms_message_via_number(
                to_number="+15559876543",
                content=wrapped,
            )

            payload = mock_session.post.call_args.kwargs["json"]
            assert payload["Body"] == "Line one of the clue Line two of the clue"

    @pytest.mark.asyncio
    async def test_send_sms_no_from_number(self):
        """Returns failure when assistant has no phone number."""
        with patch(
            "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
        ) as mock_session_details:
            mock_session_details.assistant.number = None

            result = await comms_utils.send_sms_message_via_number(
                to_number="+15559876543",
                content="Hello",
            )

            assert result["success"] is False


class TestStartCall:
    """Tests for start_call."""

    @pytest.mark.asyncio
    async def test_start_call_url_and_body(self):
        """Posts to /phone/send-call on COMMS_URL with correct JSON body."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "call_sid": "CA123"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.number = "+15551234567"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            result = await comms_utils.start_call(to_number="+15559876543")

            assert result["success"] is True

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{COMMS_URL}/phone/send-call"

            payload = mock_session.post.call_args.kwargs["json"]
            assert payload["From"] == "+15551234567"
            assert payload["To"] == "+15559876543"

    @pytest.mark.asyncio
    async def test_start_call_no_from_number(self):
        """Returns failure when assistant has no phone number."""
        with patch(
            "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
        ) as mock_session_details:
            mock_session_details.assistant.number = None

            result = await comms_utils.start_call(to_number="+15559876543")

            assert result["success"] is False


class TestSendUnifyMessage:
    """Tests for send_unify_message function with attachments."""

    @pytest.mark.asyncio
    async def test_send_without_attachment(self):
        """Sends message without attachment via Pub/Sub."""
        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._get_publisher",
            ) as mock_get_publisher,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_dm_destination",
                return_value=({"to_user_id": "user-1"}, None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.DEPLOY_ENV = "production"
            mock_settings.ENV_SUFFIX = ""
            mock_settings.ORCHESTRA_URL = ""
            mock_session.assistant.agent_id = 42
            mock_session.unify_key = ""
            mock_session.boss_contact_id = 1
            mock_session.user.id = "user-1"

            mock_publisher = MagicMock()
            mock_future = MagicMock()
            mock_future.result.return_value = "message-id-123"
            mock_publisher.publish.return_value = mock_future
            mock_publisher.topic_path.return_value = "projects/test/topics/unity-test"
            mock_get_publisher.return_value = mock_publisher

            result = await comms_utils.send_unify_message(
                content="Hello world",
                contact_id=1,
            )

            assert result["success"] is True
            mock_publisher.publish.assert_called_once()

            call_args = mock_publisher.publish.call_args
            assert call_args[0][0] == "projects/test/topics/unity-test"

    @pytest.mark.asyncio
    async def test_send_with_attachment(self):
        """Sends message with attachment included in Pub/Sub event."""
        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._get_publisher",
            ) as mock_get_publisher,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_dm_destination",
                return_value=({"to_user_id": "user-1"}, None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.DEPLOY_ENV = "production"
            mock_settings.ENV_SUFFIX = ""
            mock_settings.ORCHESTRA_URL = ""
            mock_session.assistant.agent_id = 42
            mock_session.unify_key = ""
            mock_session.boss_contact_id = 1
            mock_session.user.id = "user-1"

            mock_publisher = MagicMock()
            mock_future = MagicMock()
            mock_future.result.return_value = "message-id-123"
            mock_publisher.publish.return_value = mock_future
            mock_publisher.topic_path.return_value = "projects/test/topics/unity-test"
            mock_get_publisher.return_value = mock_publisher

            attachment = {
                "id": "att-uuid",
                "filename": "report.pdf",
                "url": "https://storage.googleapis.com/signed-url",
            }

            result = await comms_utils.send_unify_message(
                content="Here's the report",
                contact_id=1,
                attachment=attachment,
            )

            assert result["success"] is True

            call_args = mock_publisher.publish.call_args
            import json

            published_data = json.loads(call_args[0][1].decode("utf-8"))
            assert published_data["event"]["attachments"] == [attachment]

    @pytest.mark.asyncio
    async def test_send_with_team_id_posts_to_orchestra_chat_messages(self):
        """Team chat replies post team_id to Orchestra's unified chat path."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": 99, "thread_id": 5}
        mock_response.text = ""

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_room_member_contact_ids",
                return_value=([1, 4, 6], None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = await comms_utils.send_unify_message(
                content="Hello team",
                team_id=7,
            )

            assert result["success"] is True
            assert result["participant_contact_ids"] == [1, 4, 6]
            mock_post.assert_called_once()
            assert (
                mock_post.call_args[0][0]
                == "http://orchestra.test/v0/assistant/42/chat/messages"
            )
            assert mock_post.call_args[1]["json"] == {
                "content": "Hello team",
                "team_id": 7,
            }

    @pytest.mark.asyncio
    async def test_send_with_group_id_posts_to_orchestra_chat_messages(self):
        """Org chat-group replies post group_id to Orchestra's unified chat path."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": 99, "thread_id": 5}
        mock_response.text = ""

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_room_member_contact_ids",
                return_value=([1, 4], None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = await comms_utils.send_unify_message(
                content="Hello group",
                group_id=9,
            )

            assert result["success"] is True
            mock_post.assert_called_once()
            assert (
                mock_post.call_args[0][0]
                == "http://orchestra.test/v0/assistant/42/chat/messages"
            )
            assert mock_post.call_args[1]["json"] == {
                "content": "Hello group",
                "group_id": 9,
            }

    @pytest.mark.asyncio
    async def test_rejects_combined_contact_and_team_destination(self):
        """contact_id and team_id together are invalid."""
        result = await comms_utils.send_unify_message(
            content="Nope",
            contact_id=1,
            team_id=7,
        )
        assert result["success"] is False
        assert "exactly one" in result["error"]

    @pytest.mark.asyncio
    async def test_contact_id_posts_to_user_id_on_orchestra(self):
        """1:1 sends map contact_id to Orchestra to_user_id."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": 11, "thread_id": 3}
        mock_response.text = ""

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_dm_destination",
                return_value=({"to_user_id": "user-abc"}, None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = await comms_utils.send_unify_message(
                content="Hello human",
                contact_id=4,
            )

            assert result["success"] is True
            assert mock_post.call_args[1]["json"] == {
                "content": "Hello human",
                "to_user_id": "user-abc",
            }

    @pytest.mark.asyncio
    async def test_assistant_peer_contact_posts_to_assistant_id(self):
        """Peer-assistant contacts map to Orchestra to_assistant_id."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "id": 12,
            "thread_id": 9,
            "kind": "assistant_peer_dm",
        }
        mock_response.text = ""

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_dm_destination",
                return_value=({"to_assistant_id": 777}, None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = await comms_utils.send_unify_message(
                content="Hello peer",
                contact_id=9,
            )

            assert result["success"] is True
            assert mock_post.call_args[1]["json"] == {
                "content": "Hello peer",
                "to_assistant_id": 777,
            }


class TestRoomMemberResolutionIsBestEffort:
    """Resolving a room's members enriches a send; it does not authorise one.

    These contact ids only widen the local Transcripts mirror's receiver ids —
    Orchestra already knows the recipients from the room id. Treating them as
    required meant one unsynced contact row stopped the assistant posting to a
    room at all, reported to the user as "a member isn't synced to Contacts yet"
    with nothing in the log to say which member or why.
    """

    def _roster(self, **overrides):
        roster = {
            "teams": [
                {
                    "team_id": 7,
                    "member_user_ids": ["u-known", "u-unsynced"],
                    "assistant_member_ids": [],
                },
            ],
            "groups": [],
        }
        roster.update(overrides)
        return roster

    def _contact_manager(self, known: dict[str, int]):
        """A ContactManager that only knows the ids in *known*."""
        manager = MagicMock()

        def _filter(*, filter: str, limit: int = 1):  # noqa: A002
            for key, contact_id in known.items():
                if f"'{key}'" in filter:
                    return {"contacts": [{"contact_id": contact_id}]}
            return {"contacts": []}

        manager.filter_contacts.side_effect = _filter
        return manager

    def test_an_unsynced_member_no_longer_blocks_the_others(self):
        """The regression: one missing row used to discard every resolved id."""
        with (
            patch.object(
                comms_utils,
                "_fetch_org_roster",
                return_value=self._roster(),
            ),
            patch(
                "unify.manager_registry.ManagerRegistry.get_contact_manager",
                return_value=self._contact_manager({"u-known": 4}),
            ),
            patch.object(comms_utils, "SESSION_DETAILS") as mock_session,
        ):
            mock_session.self_contact_id = 0

            ids, error = comms_utils.resolve_unify_room_member_contact_ids(team_id=7)

        assert ids == [4]
        assert error is None

    def test_an_unresolvable_roster_yields_no_ids_and_no_error(self):
        """A misconfigured runtime must not present as a send failure."""
        with patch.object(comms_utils, "_fetch_org_roster", return_value=None):
            ids, error = comms_utils.resolve_unify_room_member_contact_ids(team_id=7)

        assert ids == []
        assert error is None

    def test_a_room_absent_from_the_roster_yields_no_ids_and_no_error(self):
        """The roster lists only rooms the caller belongs to, so absence is normal."""
        with patch.object(
            comms_utils,
            "_fetch_org_roster",
            return_value=self._roster(teams=[], groups=[]),
        ):
            team_ids, team_error = comms_utils.resolve_unify_room_member_contact_ids(
                team_id=7,
            )
            group_ids, group_error = comms_utils.resolve_unify_room_member_contact_ids(
                group_id=9,
            )

        assert (team_ids, team_error) == ([], None)
        assert (group_ids, group_error) == ([], None)

    def test_asking_without_a_room_is_still_an_error(self):
        """Caller misuse stays an error; only environment problems are softened."""
        _, neither = comms_utils.resolve_unify_room_member_contact_ids()
        _, both = comms_utils.resolve_unify_room_member_contact_ids(
            team_id=7,
            group_id=9,
        )

        assert neither and both

    @staticmethod
    def _warnings(mock_logger) -> str:
        """All warning text as one blob. ``LOGGER.propagate`` is False in this
        runtime, so pytest's ``caplog`` never sees these records — the logger
        itself is the only place to read them from."""
        return " ".join(str(call) for call in mock_logger.warning.call_args_list)

    def test_the_missing_member_is_named_in_the_log(self):
        """Skipping quietly would trade one invisible failure for another."""
        with (
            patch.object(
                comms_utils,
                "_fetch_org_roster",
                return_value=self._roster(),
            ),
            patch(
                "unify.manager_registry.ManagerRegistry.get_contact_manager",
                return_value=self._contact_manager({"u-known": 4}),
            ),
            patch.object(comms_utils, "SESSION_DETAILS") as mock_session,
            patch.object(comms_utils, "LOGGER") as mock_logger,
        ):
            mock_session.self_contact_id = 0
            comms_utils.resolve_unify_room_member_contact_ids(team_id=7)

        assert "u-unsynced" in self._warnings(mock_logger)

    def test_a_roster_fetch_failure_names_what_was_missing(self):
        """The silent return that cost a debugging session."""
        with (
            patch.object(comms_utils, "SESSION_DETAILS") as mock_session,
            patch.object(comms_utils, "SETTINGS") as mock_settings,
            patch.object(comms_utils, "LOGGER") as mock_logger,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.org_id = None

            assert comms_utils._fetch_org_roster() is None

        assert "ORG_ID" in self._warnings(mock_logger)

    def test_a_non_2xx_roster_response_is_logged(self):
        """Only exceptions used to log; a 403 fell through in silence."""
        response = MagicMock()
        response.status_code = 403

        with (
            patch("unisdk.utils.http.get", return_value=response),
            patch.object(comms_utils, "SESSION_DETAILS") as mock_session,
            patch.object(comms_utils, "SETTINGS") as mock_settings,
            patch.object(comms_utils, "LOGGER") as mock_logger,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.org_id = 219

            assert comms_utils._fetch_org_roster() is None

        assert "403" in self._warnings(mock_logger)


class TestSendEmailViaAddress:
    """Tests for send_email_via_address function with attachments."""

    @pytest.mark.asyncio
    async def test_send_with_attachment(self):
        """Sends email with attachment to /email/send on COMMS_URL."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "id": "email-123"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.email = "assistant@test.com"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            attachment = {
                "filename": "report.pdf",
                "content_base64": "UERGIGNvbnRlbnQ=",
            }

            result = await comms_utils.send_email_via_address(
                to=["user@example.com"],
                subject="Report",
                body="Please see attached.",
                attachment=attachment,
            )

            assert result["success"] is True

            call_args = mock_session.post.call_args
            assert call_args[0][0] == f"{COMMS_URL}/email/send"
            assert call_args.kwargs["json"]["attachment"] == attachment

    @pytest.mark.asyncio
    async def test_send_includes_agent_id_when_configured(self):
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "id": "email-123"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.email = "assistant@test.com"
            mock_session_details.assistant.agent_id = 7316
            mock_settings.conversation.COMMS_URL = COMMS_URL

            result = await comms_utils.send_email_via_address(
                to=["user@example.com"],
                subject="Report",
                body="Please see attached.",
            )

            assert result["success"] is True
            assert mock_session.post.call_args.kwargs["json"]["agent_id"] == 7316

    @pytest.mark.asyncio
    async def test_send_with_cc_and_bcc(self):
        """Sends email with cc and bcc to /email/send on COMMS_URL."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "id": "email-123"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.email = "assistant@test.com"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            result = await comms_utils.send_email_via_address(
                to=["user@example.com"],
                cc=["cc1@example.com", "cc2@example.com"],
                bcc=["bcc@example.com"],
                subject="Team Update",
                body="Here's the update.",
                thread_id="gmail-thread-123",
            )

            assert result["success"] is True

            call_args = mock_session.post.call_args
            assert call_args[0][0] == f"{COMMS_URL}/email/send"

            payload = call_args.kwargs["json"]
            assert payload["to"] == ["user@example.com"]
            assert payload["cc"] == ["cc1@example.com", "cc2@example.com"]
            assert payload["bcc"] == ["bcc@example.com"]
            assert payload["thread_id"] == "gmail-thread-123"

    @pytest.mark.asyncio
    async def test_send_uses_provider_agnostic_endpoint(self):
        """All providers route to the provider-agnostic /email/send endpoint."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "id": "email-456"},
        )

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.email = "assistant@outlook.unify.ai"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            result = await comms_utils.send_email_via_address(
                to=["user@example.com"],
                subject="Hello",
                body="Test email.",
            )

            assert result["success"] is True

            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{COMMS_URL}/email/send"

    @pytest.mark.asyncio
    async def test_send_email_collapses_hard_wrapped_body(self):
        """Hard-wrapped email bodies are normalized before transport."""
        mock_session = _mock_aiohttp_session(
            response_json={"success": True, "id": "email-789"},
        )
        wrapped = "First sentence of the quiz.\nSecond sentence here.\n\nClue line."

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session_details,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_session_details.assistant.email = "assistant@outlook.unify.ai"
            mock_settings.conversation.COMMS_URL = COMMS_URL

            await comms_utils.send_email_via_address(
                to=["user@example.com"],
                subject="Quiz",
                body=wrapped,
            )

            payload = mock_session.post.call_args.kwargs["json"]
            assert (
                payload["body"]
                == "First sentence of the quiz. Second sentence here.\n\nClue line."
            )


class TestLocalCommsBackends:
    @pytest.mark.asyncio
    async def test_send_unify_message_mirrors_to_local_outbox_and_pubsub(self):
        """Local-comms mode mirrors the message into the local outbox but
        still publishes to Pub/Sub so the Console SSE bridge can deliver
        it.  Pub/Sub is the single source of truth for the Unify chat
        surface, even when ``LOCAL_COMMS_MODE=local``.
        """
        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = "projects/p/topics/unity-1"
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-id-1"
        mock_publisher.publish.return_value = mock_future

        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._use_local_comms",
                return_value=True,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_local_outbox_async",
                new=AsyncMock(return_value=True),
            ) as mock_outbox,
            patch(
                "unify.conversation_manager.domains.comms_utils._get_publisher",
                return_value=mock_publisher,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.resolve_unify_dm_destination",
                return_value=({"to_user_id": "user-7"}, None),
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
        ):
            mock_settings.ORCHESTRA_URL = ""
            mock_session.unify_key = ""
            mock_session.assistant.agent_id = 42
            result = await comms_utils.send_unify_message(
                content="Hello world",
                contact_id=7,
            )

            assert result["success"] is True
            mock_outbox.assert_awaited_once()
            outbox_payload = mock_outbox.await_args.args[0]
            assert outbox_payload["thread"] == "unify_message_outbound"
            assert outbox_payload["event"]["content"] == "Hello world"
            assert outbox_payload["event"]["contact_id"] == 7
            mock_publisher.publish.assert_called_once()
            _, publish_kwargs = mock_publisher.publish.call_args
            assert publish_kwargs.get("thread") == "unify_message_outbound"

    @pytest.mark.asyncio
    async def test_publish_assistant_desktop_ready_mirrors_to_local_outbox_and_pubsub(
        self,
    ):
        """Local-comms mode mirrors desktop-ready into the local outbox but
        still publishes to Pub/Sub so Console SSE can unlock liveview.
        """
        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = "projects/p/topics/unity-1"
        mock_future = MagicMock()
        mock_future.result.return_value = "desktop-ready-1"
        mock_publisher.publish.return_value = mock_future

        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._use_local_comms",
                return_value=True,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_local_outbox_async",
                new=AsyncMock(return_value=True),
            ) as mock_outbox,
            patch(
                "unify.conversation_manager.domains.comms_utils._get_publisher",
                return_value=mock_publisher,
            ),
        ):
            await comms_utils.publish_assistant_desktop_ready(
                "binding-1",
                "http://127.0.0.1:8090",
                "http://127.0.0.1:8090/desktop/custom.html",
                "ubuntu",
            )

            mock_outbox.assert_awaited_once()
            outbox_payload = mock_outbox.await_args.args[0]
            assert outbox_payload["thread"] == "assistant_desktop_ready"
            assert outbox_payload["event"]["binding_id"] == "binding-1"
            assert outbox_payload["event"]["desktop_url"] == "http://127.0.0.1:8090"
            assert (
                outbox_payload["event"]["liveview_url"]
                == "http://127.0.0.1:8090/desktop/custom.html"
            )
            assert outbox_payload["event"]["vm_type"] == "ubuntu"
            mock_publisher.publish.assert_called_once()
            _, publish_kwargs = mock_publisher.publish.call_args
            assert publish_kwargs.get("thread") == "assistant_desktop_ready"

    @pytest.mark.asyncio
    async def test_publish_assistant_desktop_ready_includes_liveview_password_when_set(
        self,
    ):
        """Console needs the per-binding secret to frame the liveview; older
        bindings without one must not send a stray empty field."""
        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._use_local_comms",
                return_value=True,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_local_outbox_async",
                new=AsyncMock(return_value=True),
            ) as mock_outbox,
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_to_assistant_topic",
            ),
        ):
            await comms_utils.publish_assistant_desktop_ready(
                "binding-1",
                "http://127.0.0.1:8090",
                "http://127.0.0.1:8090/desktop/custom.html",
                "ubuntu",
                liveview_password="binding-secret",  # pragma: allowlist secret
            )

            outbox_payload = mock_outbox.await_args.args[0]
            assert (
                outbox_payload["event"]["liveview_password"]
                == "binding-secret"  # pragma: allowlist secret
            )

    @pytest.mark.asyncio
    async def test_publish_assistant_desktop_ready_omits_liveview_password_when_absent(
        self,
    ):
        """No secret minted yet (pre-rebind) -- don't send an empty field."""
        with (
            patch(
                "unify.conversation_manager.domains.comms_utils._use_local_comms",
                return_value=True,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_local_outbox_async",
                new=AsyncMock(return_value=True),
            ) as mock_outbox,
            patch(
                "unify.conversation_manager.domains.comms_utils._publish_to_assistant_topic",
            ),
        ):
            await comms_utils.publish_assistant_desktop_ready(
                "binding-1",
                "http://127.0.0.1:8090",
                "http://127.0.0.1:8090/desktop/custom.html",
                "ubuntu",
            )

            outbox_payload = mock_outbox.await_args.args[0]
            assert "liveview_password" not in outbox_payload["event"]

    def test_create_assistant_call_posts_ring_session(self):
        """The Meet ring creates an Orchestra ``assistant_dm`` call session;
        Orchestra owns the incoming frame and the answer-triggered dispatch."""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "call_id": "sess-ring-1",
            "room_name": "unity_call_sess-ring-1",
            "status": "ringing",
        }

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = comms_utils.create_assistant_call(
                opening_config={
                    "mode": "opener",
                    "opener_text": "Continuing onboarding on the live call.",
                    "source": "unify_meet_ring",
                },
            )

        assert result["success"] is True
        assert result["call_id"] == "sess-ring-1"
        assert result["room_name"] == "unity_call_sess-ring-1"
        url = mock_post.call_args.args[0]
        assert url.endswith("/calls")
        body = mock_post.call_args.kwargs["json"]
        assert body["opening_config"]["mode"] == "opener"
        assert body["opening_config"]["source"] == "unify_meet_ring"

    def test_end_assistant_call_posts_end(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        with (
            patch(
                "unisdk.utils.http.post",
                return_value=mock_response,
            ) as mock_post,
            patch(
                "unify.conversation_manager.domains.comms_utils.SESSION_DETAILS",
            ) as mock_session,
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.ORCHESTRA_URL = "http://orchestra.test/v0"
            mock_session.unify_key = "test-key"
            mock_session.assistant.agent_id = 42

            result = comms_utils.end_assistant_call("sess-ring-1")

        assert result["success"] is True
        url = mock_post.call_args.args[0]
        assert url.endswith("/calls/sess-ring-1/end")

    @pytest.mark.asyncio
    async def test_upload_unify_attachment_uses_adapters_url_even_in_local_comms_mode(
        self,
    ):
        """Local-comms mode must still upload via adapters/gateway.

        Self-host enables LOCAL_COMMS_MODE=local for Twilio/email callbacks, but
        multipart attachment uploads use the same ``/unify/attachment`` contract
        as staging (gateway/adapters), not the local-ingress metadata registry.
        """
        mock_session = _mock_aiohttp_session(
            response_json={
                "id": "local-uuid",
                "filename": "document.pdf",
                "url": "http://gateway:8001/v0/storage/local/document.pdf",
            },
        )
        adapters_url = "http://gateway:8001"

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils._use_local_comms",
                return_value=True,
            ),
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
        ):
            mock_settings.conversation.ADAPTERS_URL = adapters_url
            mock_settings.conversation.COMMS_URL = adapters_url

            result = await comms_utils.upload_unify_attachment(
                file_content=b"local file content",
                filename="document.pdf",
            )

            assert result["id"] == "local-uuid"
            assert result["filename"] == "document.pdf"
            posted_url = mock_session.post.call_args[0][0]
            assert posted_url == f"{adapters_url}/unify/attachment"
            assert "/local/comms/attachments" not in posted_url

    @pytest.mark.asyncio
    async def test_add_email_attachments_supports_inline_content(self):
        mock_session = _mock_aiohttp_session(response_json={"success": True})
        mock_file_manager = MagicMock()

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.manager_registry.ManagerRegistry.get_file_manager",
                return_value=mock_file_manager,
            ),
        ):
            await comms_utils.add_email_attachments(
                [
                    {
                        "id": "att-1",
                        "filename": "note.txt",
                        "content_base64": base64.b64encode(b"hello").decode("ascii"),
                    },
                ],
                receiver_email="assistant@example.com",
                message_id="",
            )

            mock_session.get.assert_not_called()
            mock_file_manager.save_attachment.assert_called_once_with(
                "att-1",
                "note.txt",
                b"hello",
                auto_ingest=False,
            )


class TestAddEmailAttachmentsRouting:
    """Tests for provider-agnostic routing in add_email_attachments."""

    @pytest.mark.asyncio
    async def test_fetches_via_provider_agnostic_endpoint(self):
        """Attachments are fetched from /email/attachment with unified params."""
        mock_response = MagicMock()
        mock_response.read = AsyncMock(return_value=b"file-bytes")

        mock_session = MagicMock()
        mock_session.get = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
            ),
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_file_manager = MagicMock()

        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            patch(
                "unify.conversation_manager.domains.comms_utils.SETTINGS",
            ) as mock_settings,
            patch(
                "unify.manager_registry.ManagerRegistry.get_file_manager",
                return_value=mock_file_manager,
            ),
        ):
            mock_settings.conversation.COMMS_URL = COMMS_URL

            await comms_utils.add_email_attachments(
                [{"id": "att-1", "filename": "doc.pdf"}],
                receiver_email="assistant@unify.ai",
                message_id="msg-123",
            )

            get_url = mock_session.get.call_args[0][0]
            assert get_url == f"{COMMS_URL}/email/attachment"

            params = mock_session.get.call_args.kwargs["params"]
            assert params["receiver_email"] == "assistant@unify.ai"
            assert params["message_id"] == "msg-123"
            assert params["attachment_id"] == "att-1"


@pytest.mark.asyncio
async def test_request_deferred_desktop_binding_posts_to_comms_runtime():
    mock_session = _mock_aiohttp_session(response_json={"accepted": True})

    with (
        patch("aiohttp.ClientSession", return_value=mock_session),
        patch(
            "unify.conversation_manager.domains.comms_utils.SETTINGS",
        ) as mock_settings,
    ):
        mock_settings.conversation.COMMS_URL = COMMS_URL
        await comms_utils.request_deferred_desktop_binding(7315)

    post_url = mock_session.post.call_args[0][0]
    assert post_url == f"{COMMS_URL}/infra/runtime/7315/request-desktop"
