from __future__ import annotations

from datetime import datetime, UTC
from unify import db
from unify.transcript_manager.transcript_manager import TranscriptManager
from unify.transcript_manager.types.message import Message
from unify.image_manager.utils import make_solid_png_base64
from tests.helpers import _handle_project
from unify.image_manager.types import AnnotatedImageRefs, RawImageRef, AnnotatedImageRef

PNG_BLUE = make_solid_png_base64(32, 32, (0, 0, 255))


@_handle_project
def test_schema_roundtrip():
    tm = TranscriptManager()

    refs = AnnotatedImageRefs.model_validate(
        [
            AnnotatedImageRef(
                raw_image_ref=RawImageRef(image_id=101),
                annotation="first test image",
            ),
            AnnotatedImageRef(
                raw_image_ref=RawImageRef(image_id=202),
                annotation="Screenshot of the modal open state",
            ),
        ],
    )

    msg = Message(
        medium="unify_message",
        sender_id=0,
        receiver_ids=[1],
        timestamp=datetime.now(UTC),
        content="click this button to open the modal",
        exchange_id=880011,
        images=refs,
    )

    tm.log_messages(msg)
    tm.join_published()

    # 1) Column exists in Transcripts context
    fields = db.get_fields(context=tm._transcripts_ctx)
    assert "images" in fields, "images column should exist in Transcripts"

    # 2) Round-trip retrieval preserves references
    stored = tm._filter_messages(filter=f"exchange_id == {msg.exchange_id}")["messages"]
    assert len(stored) == 1
    got = stored[0].images
    assert isinstance(got, AnnotatedImageRefs)
    # Compare by image_ids and presence of annotations
    got_items = getattr(got, "root", [])
    assert len(got_items) == 2
    got_ids = [it.raw_image_ref.image_id for it in got_items]
    assert got_ids == [101, 202]
    ann = getattr(got_items[1], "annotation", None)
    assert isinstance(ann, str) and "modal" in ann.lower()


@_handle_project
def test_accepts_annotated_refs_only():
    refs = AnnotatedImageRefs.model_validate(
        [
            AnnotatedImageRef(
                raw_image_ref=RawImageRef(image_id=1),
                annotation="Relevant to the settings section",
            ),
            AnnotatedImageRef(
                raw_image_ref=RawImageRef(image_id=2),
                annotation="Another relevant screenshot",
            ),
        ],
    )
    assert isinstance(refs, AnnotatedImageRefs)
    root = getattr(refs, "root", [])
    assert len(root) == 2 and hasattr(root[1], "annotation")


@_handle_project
def test_roundtrip_annotated_only():
    m = Message(
        medium="unify_message",
        sender_id=1,
        receiver_ids=[2],
        timestamp=datetime.now(UTC),
        content="coercion test",
        exchange_id=99001,
        images=AnnotatedImageRefs.model_validate(
            [
                AnnotatedImageRef(
                    raw_image_ref=RawImageRef(image_id=101),
                    annotation="single reference",
                ),
            ],
        ),
    )
    assert isinstance(m.images, AnnotatedImageRefs)


@_handle_project
def test_images_field_schema_shape():
    tm = TranscriptManager()

    # 1) The Transcripts context should expose a nested JSON schema for the images field
    fields = db.get_fields(context=tm._transcripts_ctx)
    assert "images" in fields
    dtype = str(fields["images"].get("data_type"))
    # Expect array/list with object items including raw_image_ref + annotation and nested image_id
    assert "raw_image_ref" in dtype and "annotation" in dtype and "image_id" in dtype

    # 2) A nested payload matching that schema is accepted
    common = {
        "medium": "unify_message",
        "sender_id": 1,
        "receiver_ids": [2],
        "timestamp": datetime.now(UTC).isoformat(),
        "content": "hello",
    }

    valid_payload = {
        **common,
        "images": [
            {"raw_image_ref": {"image_id": 101}, "annotation": "blue square"},
        ],
    }
    _ = db.log(context=tm._transcripts_ctx, **valid_payload, new=True, mutable=True)
