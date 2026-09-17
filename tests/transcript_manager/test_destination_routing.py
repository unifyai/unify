from __future__ import annotations

from datetime import UTC, datetime

import pytest
from unify import db
from tests.helpers import _handle_project
from unify.contact_manager.types.contact import Contact
from unify.transcript_manager.transcript_manager import TranscriptManager
from unify.transcript_manager.types.message import Message


def _logs(context: str, filter_expr: str):
    return db.get_logs(context=context, filter=filter_expr, return_ids_only=False)


def _message_payload(
    content: str,
    *,
    exchange_id: int | None = None,
    sender_id=0,
    receiver_ids=None,
) -> dict:
    payload = {
        "medium": "unify_message",
        "sender_id": sender_id,
        "receiver_ids": receiver_ids if receiver_ids is not None else [1],
        "timestamp": datetime.now(UTC),
        "content": content,
    }
    if exchange_id is not None:
        payload["exchange_id"] = exchange_id
    return payload


@_handle_project
def test_personal_destination_writes_to_the_home_root():
    tm = TranscriptManager()

    routed_content = "personal launch transcript"
    created = tm.log_messages(
        _message_payload(routed_content, exchange_id=81001),
        synchronous=True,
        destination="personal",
    )
    assert isinstance(created, list)
    first_exchange_id, first_message_id = tm.log_first_message_in_new_exchange(
        _message_payload("personal kickoff transcript"),
        exchange_initial_metadata={"topic": "kickoff"},
        destination="personal",
    )

    assert _logs(tm._transcripts_ctx, f"content == '{routed_content}'")
    assert _logs(tm._exchanges_ctx, "exchange_id == 81001")
    assert _logs(tm._exchanges_ctx, f"exchange_id == {first_exchange_id}")

    messages = tm._filter_messages(filter=f"content == '{routed_content}'")["messages"]
    assert [message.content for message in messages] == [routed_content]
    assert tm.get_exchange_metadata(first_exchange_id).exchange_id == first_exchange_id

    image_refs = [{"raw_image_ref": {"image_id": 123}, "annotation": "diagram"}]
    tm.update_message_images(first_message_id, image_refs, destination="personal")
    [updated_log] = _logs(tm._transcripts_ctx, f"message_id == {first_message_id}")
    assert updated_log.entries["images"] == image_refs


@_handle_project
def test_metadata_lookup_reports_the_root_it_matched_in():
    """A session artifact that lands later has to find its exchange by metadata.

    The lookup hands back the exchange id together with the destination that
    addresses the root it matched in, so the follow-up write can name that
    root explicitly rather than assume it.
    """
    tm = TranscriptManager()

    room_name = "unity_meet_room"
    exchange_id, _ = tm.log_first_message_in_new_exchange(
        _message_payload("call transcript"),
        exchange_initial_metadata={"room_name": room_name},
    )

    located = tm.resolve_exchange_id_by_metadata("room_name", room_name)
    assert located is not None
    assert located[0] == exchange_id
    assert tm.resolve_exchange_id_by_metadata("room_name", "no such room") is None

    recording_url = f"https://example.com/{room_name}.mp4"
    tm.update_exchange_metadata(
        located[0],
        {"recording_url": recording_url},
        destination=located[1],
    )

    [row] = _logs(tm._exchanges_ctx, f"exchange_id == {exchange_id}")
    assert row.entries["metadata"]["recording_url"] == recording_url


@_handle_project
def test_move_helpers_surface_destination_errors():
    tm = TranscriptManager()

    exchange_id, message_id = tm.log_first_message_in_new_exchange(
        _message_payload("personal source transcript"),
        exchange_initial_metadata={"owner": "personal"},
    )

    invalid = tm.move_message(
        message_id,
        from_root="personal",
        to_destination="team:999999999",
    )
    assert invalid["error_kind"] == "invalid_destination"
    assert _logs(tm._transcripts_ctx, f"message_id == {message_id}")

    invalid_exchange = tm.move_exchange(
        exchange_id,
        from_root="personal",
        to_destination="team:999999999",
    )
    assert invalid_exchange["error_kind"] == "invalid_destination"
    assert _logs(tm._exchanges_ctx, f"exchange_id == {exchange_id}")

    with pytest.raises(ValueError):
        tm.move_message(987654321, from_root="personal", to_destination="personal")


@_handle_project
def test_invalid_transcript_destination_returns_tool_error():
    tm = TranscriptManager()

    result = tm.log_messages(
        Message(**_message_payload("invalid destination transcript", exchange_id=99)),
        synchronous=True,
        destination="team:42",
    )

    assert result["error_kind"] == "invalid_destination"
    assert result["details"]["destination"] == "team:42"

    metadata_result = tm.update_exchange_metadata(
        99,
        {"owner": "team"},
        destination="team:42",
    )
    assert metadata_result["error_kind"] == "invalid_destination"


@_handle_project
def test_transcript_contact_search_and_reductions():
    tm = TranscriptManager()

    decoy_marker = "personal decoy transcript"
    sender_marker = "sender transcript"
    receiver_marker = "receiver transcript"

    tm.log_messages(
        _message_payload(
            decoy_marker,
            exchange_id=12001,
            sender_id=Contact(
                first_name="Nina",
                surname="Desk",
                bio="Private planning contact for personal browser notes.",
            ),
        ),
        synchronous=True,
    )
    tm.log_messages(
        _message_payload(
            sender_marker,
            exchange_id=98001,
            sender_id=Contact(
                first_name="Mara",
                surname="Field",
                bio="Heliotrope relay supervisor for compressor callback bundles.",
            ),
        ),
        synchronous=True,
    )
    tm.log_messages(
        _message_payload(
            receiver_marker,
            exchange_id=99001,
            sender_id=Contact(
                first_name="Iris",
                surname="Research",
                bio="Market pricing analyst for quarterly customer interview synthesis.",
            ),
            receiver_ids=[
                Contact(
                    first_name="Omar",
                    surname="Dispatch",
                    bio="Zephyr dispatch owner for overnight compressor incidents.",
                ),
            ],
        ),
        synchronous=True,
    )

    sender_results = tm._search_messages(
        references={"sender_bio": "heliotrope relay supervisor"},
        k=1,
    )["messages"]
    assert [message.content for message in sender_results] == [sender_marker]

    receiver_results = tm._search_messages(
        references={"receiver_bio": "zephyr dispatch owner"},
        k=1,
    )["messages"]
    assert [message.content for message in receiver_results] == [receiver_marker]

    counts_by_medium = tm._reduce(
        metric="count",
        keys="message_id",
        group_by="medium",
    )
    assert counts_by_medium["unify_message"] == 3
    assert tm._reduce(metric="max", keys="exchange_id") == 99001.0
