"""
Foreign Key Tests for TranscriptManager

Coverage
========
✓ sender_id → Contacts.contact_id (direct FK with SET NULL delete)
  - sender_id becomes null when the sender contact is deleted
  - messages with a null sender still load into the Message model

✓ receiver_ids[*] → Contacts.contact_id (array FK)
  - a deleted contact's slot in receiver_ids becomes None
  - messages with a shortened receiver list still load into the Message model

✓ exchange_id → Exchanges.exchange_id (direct FK with CASCADE delete)
  - messages are deleted when their exchange is deleted

✓ images[*].raw_image_ref.image_id → Images.image_id (deeply nested FK)
  - the nested image_id is cleared in place when the image is deleted
  - messages with a cleared image id still load into the Message model
"""

from __future__ import annotations

import pytest
from unify import db
from datetime import datetime
from tests.helpers import _handle_project
from unify.contact_manager.contact_manager import ContactManager
from unify.image_manager.image_manager import ImageManager
from unify.image_manager.utils import make_solid_png_base64
from unify.transcript_manager.transcript_manager import TranscriptManager


# Helper function for creating valid test images
def _make_test_image_b64(
    size: int = 32,
    color: tuple[int, int, int] = (255, 0, 0),
) -> str:
    """Create a minimal valid base64-encoded PNG for testing."""
    return make_solid_png_base64(size, size, color)


# --------------------------------------------------------------------------- #
#  Unit Tests: sender_id → Contacts.contact_id (SET NULL)                    #
# --------------------------------------------------------------------------- #


@_handle_project
def test_fk_message_sender_id_valid_reference():
    """Test that messages can reference valid contact IDs as sender."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1234567890",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="0987654321",
    )

    # Get contact IDs
    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Log message with valid sender
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Hello Bob!",
            "timestamp": datetime.now(),
        },
    )

    # Verify message was created
    messages = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "sender_id"],
    )
    assert len(messages) == 1
    assert messages[0].entries["sender_id"] == alice_id


@_handle_project
def test_fk_message_sender_id_set_null_on_delete():
    """Test SET NULL: sender_id becomes null when sender contact is deleted."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Alice sends messages to Bob
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Message 1",
            "timestamp": datetime.now(),
        },
    )
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Message 2",
            "timestamp": datetime.now(),
        },
    )

    # Verify messages exist with Alice as sender
    messages = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "sender_id"],
    )
    assert len(messages) == 2
    assert all(m.entries["sender_id"] == alice_id for m in messages)

    # Delete Alice (SET NULL should null sender_id but preserve messages)
    cm._delete_contact(contact_id=alice_id)

    # Verify Alice's messages still exist but sender_id is null (SET NULL behavior)
    messages_after = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "sender_id", "content"],
    )
    assert len(messages_after) == 2  # Messages still exist
    # Verify all sender_ids are null
    assert all(m.entries.get("sender_id") is None for m in messages_after)
    # Verify content is preserved
    contents = {m.entries["content"] for m in messages_after}
    assert contents == {"Message 1", "Message 2"}


@_handle_project
def test_fk_message_sender_id_null_does_not_break_manager_init():
    """Test that loading messages with null sender_id doesn't break TranscriptManager initialization."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Alice sends message to Bob
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Hello from Alice",
            "timestamp": datetime.now(),
        },
    )

    # Delete Alice (SET NULL will null sender_id)
    cm._delete_contact(contact_id=alice_id)

    # Reinitialize TranscriptManager to force loading messages from DB
    tm_new = TranscriptManager()

    # Verify the new manager can successfully read messages with null sender_id
    messages = db.get_logs(
        context=tm_new._transcripts_ctx,
    )
    assert len(messages) == 1
    assert messages[0].entries.get("sender_id") is None  # Null sender
    assert messages[0].entries["receiver_ids"] == [bob_id]
    assert messages[0].entries["content"] == "Hello from Alice"

    # Verify we can construct Message objects from the DB data (no ValidationError)
    from unify.transcript_manager.types.message import Message

    msg = Message(**messages[0].entries)
    assert msg.sender_id is None
    assert msg.receiver_ids == [bob_id]
    assert msg.content == "Hello from Alice"


@_handle_project
def test_fk_message_receiver_ids_shortened_does_not_break_manager_init():
    """Messages whose receiver list lost a deleted contact still load."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )
    cm._create_contact(
        first_name="Charlie",
        email_address="charlie@test.com",
        phone_number="3333333333",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )
    charlie_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Charlie"
    )

    # Log message with multiple receivers
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id, charlie_id],
            "content": "Hello Bob and Charlie!",
            "timestamp": datetime.now(),
        },
    )

    # Verify message exists with both receivers
    messages = db.get_logs(context=tm._transcripts_ctx)
    assert len(messages) == 1
    assert set(messages[0].entries["receiver_ids"]) == {bob_id, charlie_id}

    # Delete Charlie: the array FK nulls Charlie's slot in receiver_ids
    cm._delete_contact(contact_id=charlie_id)

    messages = db.get_logs(context=tm._transcripts_ctx)
    assert len(messages) == 1
    assert messages[0].entries["receiver_ids"] == [bob_id, None]

    # Create a new TranscriptManager instance and verify it loads successfully
    tm_new = TranscriptManager()

    messages = db.get_logs(context=tm_new._transcripts_ctx)
    assert len(messages) == 1
    assert messages[0].entries["receiver_ids"] == [bob_id, None]

    # Verify we can construct Message objects from the DB data (no ValidationError)
    from unify.transcript_manager.types.message import Message

    msg = Message(**messages[0].entries)
    assert msg.receiver_ids == [bob_id, None]


@_handle_project
def test_fk_message_images_null_does_not_break_manager_init():
    """Test that messages with null image_ids in nested images can be loaded without errors."""
    cm = ContactManager()
    tm = TranscriptManager()
    im = ImageManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Create two test images
    img_ids = im.add_images(
        [
            {"data": _make_test_image_b64(color=(255, 0, 0)), "caption": "Image 1"},
            {"data": _make_test_image_b64(color=(0, 0, 255)), "caption": "Image 2"},
        ],
        synchronous=True,
    )
    assert img_ids[0] is not None and img_ids[1] is not None, "Image creation failed"
    img1_id = img_ids[0]
    img2_id = img_ids[1]

    # Log message with multiple images
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Check out these images!",
            "timestamp": datetime.now(),
            "images": [
                {
                    "annotation": "First image",
                    "raw_image_ref": {"image_id": img1_id},
                },
                {
                    "annotation": "Second image",
                    "raw_image_ref": {"image_id": img2_id},
                },
            ],
        },
    )

    # Verify message exists with both images
    messages = db.get_logs(context=tm._transcripts_ctx)
    assert len(messages) == 1
    image_ids_in_msg = [
        img["raw_image_ref"]["image_id"]
        for img in messages[0].entries.get("images", [])
    ]
    assert img1_id in image_ids_in_msg
    assert img2_id in image_ids_in_msg

    # Delete img2 (should trigger SET NULL on nested image_id)
    img2_logs = db.get_logs(
        context=im._ctx,
        filter=f"image_id == {img2_id}",
        return_ids_only=True,
    )
    assert img2_logs, "Image not found"
    db.delete_logs(context=im._ctx, logs=img2_logs[0])

    # Verify message now has one valid image_id and one None
    messages = db.get_logs(context=tm._transcripts_ctx)
    assert len(messages) == 1
    images_list = messages[0].entries.get("images", [])
    assert len(images_list) == 2

    image_ids_after_delete = [img["raw_image_ref"]["image_id"] for img in images_list]
    assert img1_id in image_ids_after_delete
    assert None in image_ids_after_delete

    # Create a new TranscriptManager instance and verify it loads successfully
    tm_new = TranscriptManager()

    # Verify the new manager can successfully read messages with null image_ids
    messages = db.get_logs(context=tm_new._transcripts_ctx)
    assert len(messages) == 1
    images_list = messages[0].entries.get("images", [])
    assert len(images_list) == 2

    # Verify we can construct Message objects from the DB data (no ValidationError)
    from unify.transcript_manager.types.message import Message

    msg = Message(**messages[0].entries)
    assert len(msg.images.root) == 2
    # One image should have a valid ID, one should have None
    image_ids_in_model = [ref.raw_image_ref.image_id for ref in msg.images.root]
    assert img1_id in image_ids_in_model
    assert None in image_ids_in_model


# --------------------------------------------------------------------------- #
#  Unit Tests: receiver_ids[*] → Contacts.contact_id (element nulled)        #
# --------------------------------------------------------------------------- #


@_handle_project
def test_fk_message_receiver_ids_valid_reference():
    """Test that messages can reference valid contact IDs in receiver_ids array."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )
    cm._create_contact(
        first_name="Carol",
        email_address="carol@test.com",
        phone_number="3333333333",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    contact_map = {
        c.entries["first_name"]: int(c.entries["contact_id"]) for c in contacts
    }

    # Log message with multiple receivers
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": contact_map["Alice"],
            "receiver_ids": [contact_map["Bob"], contact_map["Carol"]],
            "content": "Group message",
            "timestamp": datetime.now(),
        },
    )

    # Verify message was created with all receivers
    messages = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "receiver_ids"],
    )
    assert len(messages) == 1
    assert sorted(messages[0].entries["receiver_ids"]) == sorted(
        [contact_map["Bob"], contact_map["Carol"]],
    )


@_handle_project
def test_fk_message_receiver_ids_null_deleted_contact():
    """Deleting a contact nulls its slot in every receiver_ids array."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )
    cm._create_contact(
        first_name="Carol",
        email_address="carol@test.com",
        phone_number="3333333333",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    contact_map = {
        c.entries["first_name"]: int(c.entries["contact_id"]) for c in contacts
    }

    # Alice sends to Bob and Carol
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": contact_map["Alice"],
            "receiver_ids": [contact_map["Bob"], contact_map["Carol"]],
            "content": "Group message",
            "timestamp": datetime.now(),
        },
    )

    # Verify both receivers
    messages = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "receiver_ids"],
    )
    assert sorted(messages[0].entries["receiver_ids"]) == sorted(
        [contact_map["Bob"], contact_map["Carol"]],
    )

    cm._delete_contact(contact_id=contact_map["Bob"])

    messages_after = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "receiver_ids"],
    )
    assert len(messages_after) == 1  # Message still exists
    assert messages_after[0].entries["receiver_ids"] == [None, contact_map["Carol"]]


# --------------------------------------------------------------------------- #
#  Unit Tests: exchange_id → Exchanges.exchange_id (CASCADE)                 #
# --------------------------------------------------------------------------- #


@_handle_project
def test_fk_message_exchange_id_cascade_delete():
    """Test CASCADE: Messages deleted when exchange is deleted."""
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Log messages in same exchange
    exchange_id, _ = tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Message 1",
            "timestamp": datetime.now(),
        },
    )

    # Log more messages in same exchange (synchronous=True to ensure persisted before query)
    tm.log_messages(
        {
            "medium": "unify_message",
            "sender_id": bob_id,
            "receiver_ids": [alice_id],
            "content": "Reply",
            "timestamp": datetime.now(),
            "exchange_id": exchange_id,
        },
        synchronous=True,
    )

    # Verify 2 messages in exchange
    messages_in_exchange = db.get_logs(
        context=tm._transcripts_ctx,
        filter=f"exchange_id == {exchange_id}",
        from_fields=["message_id"],
    )
    assert len(messages_in_exchange) == 2

    # Delete the exchange (get log ID first, then delete)
    exchange_logs = db.get_logs(
        context=tm._exchanges_ctx,
        filter=f"exchange_id == {exchange_id}",
        return_ids_only=True,
    )
    assert exchange_logs, "Exchange not found"
    db.delete_logs(context=tm._exchanges_ctx, logs=exchange_logs[0])

    # Verify all messages in exchange were cascade deleted
    messages_after = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "exchange_id"],
    )
    assert len(messages_after) == 0  # All messages deleted


# --------------------------------------------------------------------------- #
#  Unit Tests: images[*].raw_image_ref.image_id → Images.image_id (nested)  #
# --------------------------------------------------------------------------- #


@_handle_project
def test_fk_message_images_valid_reference():
    """Test that messages can reference valid image IDs in nested structure."""
    cm = ContactManager()
    im = ImageManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Store images with valid base64 PNG data
    img_ids = im.add_images(
        [
            {"data": _make_test_image_b64(color=(255, 0, 0)), "caption": "Image 1"},
            {"data": _make_test_image_b64(color=(0, 0, 255)), "caption": "Image 2"},
        ],
        synchronous=True,
    )
    assert img_ids[0] is not None and img_ids[1] is not None, "Image creation failed"
    img1_id = img_ids[0]
    img2_id = img_ids[1]

    # Log message with images
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Check out these images!",
            "timestamp": datetime.now(),
            "images": [
                {"raw_image_ref": {"image_id": img1_id}, "annotation": "First image"},
                {"raw_image_ref": {"image_id": img2_id}, "annotation": "Second image"},
            ],
        },
    )

    # Verify message created with nested image references
    messages = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "images"],
    )
    assert len(messages) == 1
    images = messages[0].entries["images"]
    assert len(images) == 2
    assert images[0]["raw_image_ref"]["image_id"] == img1_id
    assert images[1]["raw_image_ref"]["image_id"] == img2_id


@_handle_project
def test_fk_message_images_set_null_on_delete():
    """Test SET NULL: Deleting image replaces nested image_id with None (in-place)."""
    cm = ContactManager()
    im = ImageManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice",
        email_address="alice@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="2222222222",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    alice_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Alice"
    )
    bob_id = next(
        int(c.entries["contact_id"])
        for c in contacts
        if c.entries["first_name"] == "Bob"
    )

    # Store images with valid base64 PNG data
    img_ids = im.add_images(
        [
            {"data": _make_test_image_b64(color=(255, 0, 0)), "caption": "Image 1"},
            {"data": _make_test_image_b64(color=(0, 255, 0)), "caption": "Image 2"},
            {"data": _make_test_image_b64(color=(0, 0, 255)), "caption": "Image 3"},
        ],
        synchronous=True,
    )
    assert all(iid is not None for iid in img_ids), "Image creation failed"
    img1_id = img_ids[0]
    img2_id = img_ids[1]
    img3_id = img_ids[2]

    # Log message with 3 images
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice_id,
            "receiver_ids": [bob_id],
            "content": "Three images",
            "timestamp": datetime.now(),
            "images": [
                {"raw_image_ref": {"image_id": img1_id}, "annotation": "First"},
                {"raw_image_ref": {"image_id": img2_id}, "annotation": "Second"},
                {"raw_image_ref": {"image_id": img3_id}, "annotation": "Third"},
            ],
        },
    )

    # Verify 3 images
    messages = db.get_logs(context=tm._transcripts_ctx, from_fields=["images"])
    assert len(messages[0].entries["images"]) == 3

    # Delete middle image (img2) - get log ID first, then delete
    img2_logs = db.get_logs(
        context=im._ctx,
        filter=f"image_id == {img2_id}",
        return_ids_only=True,
    )
    assert img2_logs, "Image not found"
    db.delete_logs(context=im._ctx, logs=img2_logs[0])

    # Verify img2 replaced with None in nested structure (SET NULL = in-place replacement)
    messages_after = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["images"],
    )
    images_after = messages_after[0].entries.get("images", [])
    assert len(images_after) == 3  # Array length unchanged (in-place replacement)

    # First and third images preserved
    assert images_after[0]["raw_image_ref"]["image_id"] == img1_id
    assert images_after[2]["raw_image_ref"]["image_id"] == img3_id

    # Middle image has None for image_id (in-place SET NULL)
    assert images_after[1]["raw_image_ref"]["image_id"] is None
    assert images_after[1]["annotation"] == "Second"  # Annotation preserved


# --------------------------------------------------------------------------- #
#  Integration Tests: Contact Merge                                          #
# --------------------------------------------------------------------------- #


@_handle_project
@pytest.mark.integration
def test_contact_merge_with_transcripts():
    """
    Test contact merge behavior with transcripts.

    Flow:
    1. Create two contacts with messages
    2. Merge contacts (manual update_contact_id before delete)
    3. Verify all transcript references point to kept contact
    4. Verify FK SET NULL doesn't interfere (no references to deleted contact remain)
    """
    cm = ContactManager()
    tm = TranscriptManager()

    # Create contacts
    cm._create_contact(
        first_name="Alice Original",
        email_address="alice1@test.com",
        phone_number="1111111111",
    )
    cm._create_contact(
        first_name="Alice Duplicate",
        email_address="alice2@test.com",
        phone_number="2222222222",
    )
    cm._create_contact(
        first_name="Bob",
        email_address="bob@test.com",
        phone_number="3333333333",
    )

    contacts = db.get_logs(
        context=cm._ctx,
        from_fields=["contact_id", "first_name"],
    )
    contact_map = {
        c.entries["first_name"]: int(c.entries["contact_id"]) for c in contacts
    }
    alice1_id = contact_map["Alice Original"]
    alice2_id = contact_map["Alice Duplicate"]
    bob_id = contact_map["Bob"]

    # Alice1 sends message to Bob
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice1_id,
            "receiver_ids": [bob_id],
            "content": "From Alice1",
            "timestamp": datetime.now(),
        },
    )

    # Alice2 sends message to Bob
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": alice2_id,
            "receiver_ids": [bob_id],
            "content": "From Alice2",
            "timestamp": datetime.now(),
        },
    )

    # Bob sends to both Alices
    tm.log_first_message_in_new_exchange(
        {
            "medium": "unify_message",
            "sender_id": bob_id,
            "receiver_ids": [alice1_id, alice2_id],
            "content": "To both Alices",
            "timestamp": datetime.now(),
        },
    )

    # Verify 3 messages before merge
    messages_before = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id"],
    )
    assert len(messages_before) == 3

    # Merge Alice2 into Alice1 (keeps alice1_id, deletes alice2_id)
    cm._merge_contacts(contact_id_1=alice1_id, contact_id_2=alice2_id)

    # Verify Alice2 contact deleted (exclude system contacts: 0=assistant, 1=user)
    contacts_after = db.get_logs(
        context=cm._ctx,
        filter="contact_id > 1",
        from_fields=["contact_id", "first_name"],
    )
    assert len(contacts_after) == 2  # Only Alice1 and Bob remain
    remaining_ids = [int(c.entries["contact_id"]) for c in contacts_after]
    assert alice1_id in remaining_ids
    assert alice2_id not in remaining_ids

    # Verify all 3 messages still exist
    messages_after = db.get_logs(
        context=tm._transcripts_ctx,
        from_fields=["message_id", "sender_id", "receiver_ids", "content"],
    )
    assert len(messages_after) == 3

    # Verify all alice2_id references changed to alice1_id (manual update before delete)
    for msg in messages_after:
        sender = msg.entries.get("sender_id")
        receivers = msg.entries.get("receiver_ids", [])

        # No null senders (manual update happened before delete)
        assert sender is not None

        # No alice2_id references anywhere
        assert sender != alice2_id
        assert alice2_id not in receivers

        # No None in receiver arrays (manual update worked)
        assert None not in receivers

    # Verify specific message outcomes
    alice1_msgs = [m for m in messages_after if m.entries.get("sender_id") == alice1_id]
    assert len(alice1_msgs) == 2  # Both Alice messages now from alice1_id

    bob_msg = next(
        m for m in messages_after if m.entries["content"] == "To both Alices"
    )
    assert bob_msg.entries["sender_id"] == bob_id
    assert sorted(bob_msg.entries["receiver_ids"]) == [
        alice1_id,
    ]  # Only one contact_id now
