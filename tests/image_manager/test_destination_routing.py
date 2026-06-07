from __future__ import annotations

import base64
import time
from datetime import UTC, datetime

import pytest
import unify

from tests.helpers import _handle_project
from unity.image_manager.image_manager import ImageManager
from unity.image_manager.utils import make_solid_png_base64
from unity.session_details import SESSION_DETAILS


def _team_id() -> int:
    return int(time.time_ns() % 1_000_000_000)


def _image_logs(context: str, image_id: int):
    return unify.get_logs(
        context=context,
        filter=f"image_id == {int(image_id)}",
        return_ids_only=False,
    )


def _delete_context_tree(root: str) -> None:
    try:
        children = list(unify.get_contexts(prefix=f"{root}/").keys())
    except Exception:
        children = []
    for context in sorted(children, key=len, reverse=True):
        try:
            unify.delete_context(context)
        except Exception:
            pass
    try:
        unify.delete_context(root)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def reset_team_membership_state():
    yield
    for team_id in SESSION_DETAILS.team_ids:
        _delete_context_tree(f"Teams/{team_id}")
    SESSION_DETAILS.team_ids = []
    SESSION_DETAILS.team_summaries = []


def _image_payload(caption: str, data: str | None = None) -> dict:
    return {
        "timestamp": datetime.now(UTC),
        "caption": caption,
        "data": data or make_solid_png_base64(16, 16, (255, 0, 0)),
    }


@_handle_project
def test_image_writes_route_to_space_and_reads_fan_out():
    team_id = _team_id()
    SESSION_DETAILS.team_ids = [team_id]
    im = ImageManager()

    personal_caption = f"personal lunch receipt {team_id}"
    space_caption = f"shared compressor callback diagram {team_id}"
    [personal_id] = im.add_images([_image_payload(personal_caption)], synchronous=True)
    [team_id_value] = im.add_images(
        [_image_payload(space_caption, make_solid_png_base64(16, 16, (0, 0, 255)))],
        synchronous=True,
        destination=f"team:{team_id}",
    )

    space_context = f"Teams/{team_id}/Images"
    assert _image_logs(im._ctx, personal_id)
    assert not unify.get_logs(
        context=im._ctx,
        filter=f"caption == '{space_caption}'",
    )
    assert _image_logs(space_context, team_id_value)

    all_captions = {image.caption for image in im.filter_images(limit=10)}
    assert {personal_caption, space_caption} <= all_captions
    space_only = im.filter_images(destination=f"team:{team_id}", limit=10)
    assert {image.caption for image in space_only} == {space_caption}
    semantic = im.search_images(reference_text="compressor callback diagram", k=1)
    assert [image.caption for image in semantic] == [space_caption]

    [space_handle] = im.get_images([team_id_value], destination=f"team:{team_id}")
    assert space_handle.caption == space_caption


@_handle_project
def test_image_updates_resolve_filepath_and_move_are_root_aware(tmp_path):
    team_id = _team_id()
    SESSION_DETAILS.team_ids = [team_id]
    im = ImageManager()

    [personal_id] = im.add_images(
        [_image_payload("personal duplicate id")],
        synchronous=True,
    )
    [space_image_id] = im.add_images(
        [_image_payload("space original")],
        synchronous=True,
        destination=f"team:{team_id}",
    )
    assert personal_id == space_image_id

    im.update_images(
        [{"image_id": space_image_id, "caption": "space updated"}],
        destination=f"team:{team_id}",
    )
    assert (
        im.filter_images(filter=f"image_id == {personal_id}", destination="personal")[
            0
        ].caption
        == "personal duplicate id"
    )
    assert (
        im.filter_images(
            filter=f"image_id == {space_image_id}",
            destination=f"team:{team_id}",
        )[0].caption
        == "space updated"
    )

    raw_path = tmp_path / "routed.png"
    raw_path.write_bytes(base64.b64decode(make_solid_png_base64(8, 8, (0, 255, 0))))
    routed_id = im.resolve_filepath(str(raw_path), destination=f"team:{team_id}")
    assert _image_logs(f"Teams/{team_id}/Images", routed_id)
    assert not _image_logs(im._ctx, routed_id)

    moved = im.move_image(
        routed_id,
        from_root=f"team:{team_id}",
        to_destination="personal",
    )
    assert moved["details"]["from_context"] == f"Teams/{team_id}/Images"
    assert moved["details"]["to_context"] == im._ctx
    assert not _image_logs(f"Teams/{team_id}/Images", routed_id)
    assert _image_logs(im._ctx, routed_id)

    invalid = im.move_image(
        routed_id,
        from_root="personal",
        to_destination="team:999999999",
    )
    assert invalid["error_kind"] == "invalid_destination"

    with pytest.raises(ValueError):
        im.move_image(
            987654321,
            from_root="personal",
            to_destination=f"team:{team_id}",
        )


@_handle_project
def test_image_handle_updates_persist_to_original_root():
    team_id = _team_id()
    SESSION_DETAILS.team_ids = [team_id]
    im = ImageManager()

    [handle] = im.add_images(
        [_image_payload("handle original")],
        synchronous=True,
        return_handles=True,
        destination=f"team:{team_id}",
    )

    handle.update_metadata(caption="handle updated in space")

    assert not unify.get_logs(
        context=im._ctx,
        filter="caption == 'handle updated in space'",
    )
    [space_row] = _image_logs(f"Teams/{team_id}/Images", handle.image_id)
    assert space_row.entries["caption"] == "handle updated in space"
