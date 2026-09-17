from __future__ import annotations

import base64
from datetime import UTC, datetime

import pytest
from unify import db
from tests.helpers import _handle_project
from unify.image_manager.image_manager import ImageManager
from unify.image_manager.utils import make_solid_png_base64


def _image_logs(context: str, image_id: int):
    return db.get_logs(
        context=context,
        filter=f"image_id == {int(image_id)}",
        return_ids_only=False,
    )


def _image_payload(caption: str, data: str | None = None) -> dict:
    return {
        "timestamp": datetime.now(UTC),
        "caption": caption,
        "data": data or make_solid_png_base64(16, 16, (255, 0, 0)),
    }


@_handle_project
def test_image_personal_destination_writes_to_the_home_root():
    im = ImageManager()

    implicit_caption = "personal lunch receipt"
    explicit_caption = "compressor callback diagram"
    [implicit_id] = im.add_images([_image_payload(implicit_caption)], synchronous=True)
    [explicit_id] = im.add_images(
        [_image_payload(explicit_caption, make_solid_png_base64(16, 16, (0, 0, 255)))],
        synchronous=True,
        destination="personal",
    )

    assert _image_logs(im._ctx, implicit_id)
    assert _image_logs(im._ctx, explicit_id)

    all_captions = {image.caption for image in im.filter_images(limit=10)}
    assert {implicit_caption, explicit_caption} <= all_captions
    personal_only = im.filter_images(destination="personal", limit=10)
    assert {image.caption for image in personal_only} == all_captions
    semantic = im.search_images(reference_text="compressor callback diagram", k=1)
    assert [image.caption for image in semantic] == [explicit_caption]

    [handle] = im.get_images([explicit_id], destination="personal")
    assert handle.caption == explicit_caption


@_handle_project
def test_image_updates_resolve_filepath_and_move_surface_errors(tmp_path):
    im = ImageManager()

    [image_id] = im.add_images([_image_payload("original")], synchronous=True)

    im.update_images(
        [{"image_id": image_id, "caption": "updated"}],
        destination="personal",
    )
    assert (
        im.filter_images(filter=f"image_id == {image_id}", destination="personal")[
            0
        ].caption
        == "updated"
    )

    raw_path = tmp_path / "routed.png"
    raw_path.write_bytes(base64.b64decode(make_solid_png_base64(8, 8, (0, 255, 0))))
    routed_id = im.resolve_filepath(str(raw_path), destination="personal")
    assert _image_logs(im._ctx, routed_id)

    invalid = im.move_image(
        routed_id,
        from_root="personal",
        to_destination="team:999999999",
    )
    assert invalid["error_kind"] == "invalid_destination"
    assert _image_logs(im._ctx, routed_id)

    with pytest.raises(ValueError):
        im.move_image(987654321, from_root="personal", to_destination="personal")


@_handle_project
def test_image_handle_updates_persist_to_the_home_root():
    im = ImageManager()

    [handle] = im.add_images(
        [_image_payload("handle original")],
        synchronous=True,
        return_handles=True,
        destination="personal",
    )

    handle.update_metadata(caption="handle updated")

    [row] = _image_logs(im._ctx, handle.image_id)
    assert row.entries["caption"] == "handle updated"
