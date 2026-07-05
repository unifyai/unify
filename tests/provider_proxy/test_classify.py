from __future__ import annotations

import pytest

from unify.provider_proxy.classify import (
    KIND_BATCH,
    KIND_FILE_READ,
    KIND_FILE_WRITE,
    KIND_NON_FILE,
    KIND_UNKNOWN,
    classify,
)

# ── Microsoft Graph ──────────────────────────────────────────────────────────


def test_ms_root_children_is_listing_with_parent():
    c = classify("microsoft", "GET", "v1.0/me/drive/root/children", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent is not None and c.parent.item_id == "root"


def test_ms_item_children_uses_explicit_drive_and_item():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1/children", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert (c.parent.drive_id, c.parent.item_id) == ("D1", "I1")


def test_ms_get_item_targets_item():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is False
    assert (c.target.drive_id, c.target.item_id) == ("D1", "I1")


def test_ms_content_is_content_read():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1/content", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_content is True
    assert c.target.item_id == "I1"


def test_ms_search_is_listing():
    c = classify("microsoft", "GET", "v1.0/me/drive/root/search(q='x')", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.is_search is True


@pytest.mark.parametrize("method", ["PATCH", "DELETE"])
def test_ms_item_write_targets_item(method):
    c = classify("microsoft", method, "v1.0/drives/D1/items/I1", {})
    assert c.kind == KIND_FILE_WRITE
    assert (c.target.drive_id, c.target.item_id) == ("D1", "I1")


def test_ms_create_child_is_write_with_parent():
    c = classify("microsoft", "POST", "v1.0/drives/D1/items/I1/children", {})
    assert c.kind == KIND_FILE_WRITE
    assert (c.parent.drive_id, c.parent.item_id) == ("D1", "I1")


def test_ms_batch():
    assert classify("microsoft", "POST", "v1.0/$batch", {}).kind == KIND_BATCH


def test_ms_non_file_passthrough():
    assert classify("microsoft", "GET", "v1.0/me/events", {}).kind == KIND_NON_FILE
    assert classify("microsoft", "GET", "v1.0/me/messages", {}).kind == KIND_NON_FILE


def test_ms_path_get_item_is_read_with_path_target():
    c = classify("microsoft", "GET", "v1.0/me/drive/root:/Finance/report.xlsx", {})
    assert c.kind == KIND_FILE_READ
    assert c.target is not None and c.target.is_path
    assert c.target.path == "Finance/report.xlsx"
    assert c.target.anchor_item_id == "root"


def test_ms_path_children_listing():
    c = classify("microsoft", "GET", "v1.0/me/drive/root:/Finance:/children", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent is not None and c.parent.path == "Finance"


def test_ms_path_content_download():
    c = classify("microsoft", "GET", "v1.0/drives/D1/root:/a/b.txt:/content", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_content is True
    assert (c.target.drive_id, c.target.path) == ("D1", "a/b.txt")


def test_ms_path_content_upload_is_write():
    c = classify("microsoft", "PUT", "v1.0/me/drive/root:/Finance/new.txt:/content", {})
    assert c.kind == KIND_FILE_WRITE
    assert c.target is not None and c.target.path == "Finance/new.txt"


def test_ms_path_upload_session_is_write():
    c = classify(
        "microsoft",
        "POST",
        "v1.0/me/drive/root:/Finance/big.zip:/createUploadSession",
        {},
    )
    assert c.kind == KIND_FILE_WRITE
    assert c.target is not None and c.target.path == "Finance/big.zip"


def test_ms_path_relative_to_item():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1:/sub/file.txt", {})
    assert c.kind == KIND_FILE_READ
    assert c.target.anchor_item_id == "I1"
    assert c.target.path == "sub/file.txt"


def test_ms_site_path_get_item_is_read():
    c = classify("microsoft", "GET", "v1.0/sites/S1/drive/root:/HR/report.xlsx", {})
    assert c.kind == KIND_FILE_READ
    assert c.target is not None and c.target.drive_id == "site:S1"
    assert c.target.path == "HR/report.xlsx"


def test_ms_unparseable_colon_shape_denied():
    c = classify("microsoft", "GET", "v1.0/me/drive/root:/Finance:/notSupported", {})
    assert c.kind == KIND_UNKNOWN


def test_ms_list_drives_is_root_listing_unfiltered():
    c = classify("microsoft", "GET", "v1.0/me/drives", {})
    assert c.kind == KIND_FILE_READ
    assert c.root_listing is True
    assert c.is_listing is False


def test_ms_top_level_drives_is_root_listing():
    c = classify("microsoft", "GET", "v1.0/drives", {})
    assert c.kind == KIND_FILE_READ
    assert c.root_listing is True


def test_ms_recent_is_per_item_listing():
    c = classify("microsoft", "GET", "v1.0/me/drive/recent", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent is None


def test_ms_root_delta_is_listing():
    c = classify("microsoft", "GET", "v1.0/me/drive/root/delta", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent is not None and c.parent.item_id == "root"


def test_ms_item_delta_is_listing():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1/delta", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert (c.parent.drive_id, c.parent.item_id) == ("D1", "I1")


def test_ms_special_list_is_listing():
    c = classify("microsoft", "GET", "v1.0/me/drive/special", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True


def test_ms_item_permissions_read():
    c = classify("microsoft", "GET", "v1.0/drives/D1/items/I1/permissions", {})
    assert c.kind == KIND_FILE_READ
    assert c.target.item_id == "I1"


def test_ms_item_create_link_write():
    c = classify("microsoft", "POST", "v1.0/drives/D1/items/I1/createLink", {})
    assert c.kind == KIND_FILE_WRITE
    assert c.target.item_id == "I1"


def test_ms_share_drive_item_children():
    c = classify("microsoft", "GET", "v1.0/shares/SH1/driveItem/children", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True


# Production regression paths (Dan staging session, 2026-07-05)
_DAN_REGRESSION_PATHS = [
    ("GET", "v1.0/sites/{siteId}/drive"),
    ("GET", "v1.0/sites/{siteId}/drive/root/children"),
    ("GET", "v1.0/sites/{siteId}/drive/root/delta"),
    ("GET", "v1.0/drives"),
    ("GET", "v1.0/groups/{groupId}/drive"),
    ("GET", "v1.0/groups/{groupId}/drive/root/children"),
    ("GET", "v1.0/users/{userId}/drive"),
    ("GET", "v1.0/users/{userId}/drive/root/children"),
    ("GET", "v1.0/sites/{siteId}/drives"),
    ("GET", "v1.0/sites/{siteId}/drives/{driveId}/root/children"),
]


@pytest.mark.parametrize(
    "method,path",
    _DAN_REGRESSION_PATHS,
    ids=[p[1] for p in _DAN_REGRESSION_PATHS],
)
def test_dan_regression_paths_are_file_read_not_unknown(method, path):
    c = classify("microsoft", method, path, {})
    assert c.kind == KIND_FILE_READ, f"{path} classified as {c.kind}"


_MS_ADDRESSING_TABLE = [
    ("GET", "v1.0/me/drive/root/children", True, False),
    ("GET", "v1.0/me/drive/items/{id}", False, False),
    ("GET", "v1.0/me/drive/items/{id}/content", False, True),
    ("GET", "v1.0/me/drive/root/delta", True, False),
    ("GET", "v1.0/me/drive/root/search(q='x')", True, False),
    ("GET", "v1.0/drives/{driveId}/root/children", True, False),
    ("GET", "v1.0/drives/{driveId}/items/{id}", False, False),
    ("GET", "v1.0/drives/{driveId}/items/{id}/content", False, True),
    ("GET", "v1.0/drives/{driveId}/items/{id}/delta", True, False),
    ("GET", "v1.0/sites/{siteId}/drive/root/children", True, False),
    ("GET", "v1.0/sites/{siteId}/drive/items/{id}/content", False, True),
    ("GET", "v1.0/sites/{siteId}/drive/root/delta", True, False),
    ("GET", "v1.0/groups/{groupId}/drive/root/children", True, False),
    ("GET", "v1.0/users/{userId}/drive/root/children", True, False),
    ("GET", "v1.0/shares/{shareId}/driveItem", False, False),
    ("GET", "v1.0/shares/{shareId}/driveItem/children", True, False),
    ("POST", "v1.0/drives/{driveId}/items/{id}/createLink", False, False),
    ("GET", "v1.0/drives/{driveId}/items/{id}/permissions", False, False),
    ("GET", "v1.0/drives/{driveId}/items/{id}/versions", False, False),
    ("GET", "v1.0/drives/{driveId}/items/{id}/thumbnails", False, False),
]


@pytest.mark.parametrize(
    "method,path,is_listing,is_content",
    _MS_ADDRESSING_TABLE,
    ids=[p[1] for p in _MS_ADDRESSING_TABLE],
)
def test_ms_addressing_table(method, path, is_listing, is_content):
    c = classify("microsoft", method, path, {})
    assert c.kind == (KIND_FILE_WRITE if method != "GET" else KIND_FILE_READ)
    assert c.is_listing is is_listing
    assert c.is_content is is_content


_MS_UNKNOWN_PATHS = [
    "v1.0/me/drive/items/{id}/notARealGraphOp",
    "v1.0/me/drive/bundles/{id}/extra",
]


@pytest.mark.parametrize("path", _MS_UNKNOWN_PATHS)
def test_ms_unknown_suffixes_denied(path):
    assert classify("microsoft", "GET", path, {}).kind == KIND_UNKNOWN


# ── Google Drive ─────────────────────────────────────────────────────────────


def test_google_files_list_is_listing():
    c = classify("google", "GET", "drive/v3/files", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent is None


def test_google_get_file_targets_item_with_drive_from_query():
    c = classify("google", "GET", "drive/v3/files/F1", {"driveId": "SD1"})
    assert c.kind == KIND_FILE_READ
    assert (c.target.drive_id, c.target.item_id) == ("SD1", "F1")


def test_google_media_download_is_content():
    c = classify("google", "GET", "drive/v3/files/F1", {"alt": "media"})
    assert c.kind == KIND_FILE_READ
    assert c.is_content is True


def test_google_create_is_write():
    assert classify("google", "POST", "drive/v3/files", {}).kind == KIND_FILE_WRITE


def test_google_update_is_write_targeting_item():
    c = classify("google", "PATCH", "drive/v3/files/F1", {})
    assert c.kind == KIND_FILE_WRITE
    assert c.target.item_id == "F1"


def test_google_non_drive_passthrough():
    c = classify("google", "GET", "calendar/v3/calendars/primary/events", {})
    assert c.kind == KIND_NON_FILE


def test_google_changes_list_is_listing():
    c = classify("google", "GET", "drive/v3/changes", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.changes_list is True


def test_google_changes_start_token():
    c = classify("google", "GET", "drive/v3/changes/startPageToken", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is False


def test_google_file_permissions():
    c = classify("google", "GET", "drive/v3/files/F1/permissions", {})
    assert c.kind == KIND_FILE_READ
    assert c.target.item_id == "F1"


def test_google_file_revisions():
    c = classify("google", "GET", "drive/v3/files/F1/revisions", {})
    assert c.kind == KIND_FILE_READ
    assert c.target.item_id == "F1"


def test_google_file_trash_write():
    c = classify("google", "POST", "drive/v3/files/F1/trash", {})
    assert c.kind == KIND_FILE_WRITE
    assert c.target.item_id == "F1"


def test_google_get_shared_drive():
    c = classify("google", "GET", "drive/v3/drives/SD1", {})
    assert c.kind == KIND_FILE_READ


def test_google_unknown_drive_endpoint_denied():
    c = classify("google", "GET", "drive/v3/files/F1/notReal", {})
    assert c.kind == KIND_UNKNOWN
