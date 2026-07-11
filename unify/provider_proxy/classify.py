"""Classify a proxied Drive/Graph request into a policy decision shape.

The proxy fronts the *full* Microsoft Graph and Google Drive REST surface. Only
file/drive endpoints are subject to the allowlist; everything else (calendar,
mail, contacts, ...) is passed straight through. This module inspects the method
and path and returns a :class:`Classification` describing how the proxy should
enforce the allowlist. Anything that looks like a file/drive endpoint but is not
explicitly recognized is classified ``unknown_file`` and denied by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

KIND_NON_FILE = "non_file"
KIND_FILE_READ = "file_read"
KIND_FILE_WRITE = "file_write"
KIND_UNKNOWN = "unknown_file"
KIND_BATCH = "batch"

_MY_DRIVE = "my-drive"
_MS_DEFAULT_DRIVE = "me"
_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

_MS_CONTAINER_PREFIX = {"sites": "site", "groups": "group", "users": "user"}

_MS_ITEM_WRITE_OPS = frozenset(
    {
        "copy",
        "createUploadSession",
        "createLink",
        "invite",
        "restore",
        "permanentDelete",
        "checkin",
        "checkout",
        "discardCheckout",
        "follow",
        "assignSensitivityLabel",
        "removeRetentionLabel",
    },
)

_MS_ITEM_READ_OPS = frozenset(
    {
        "thumbnails",
        "preview",
        "analytics",
        "activities",
        "listItem",
        "extractSensitivityLabels",
        "retentionLabel",
    },
)


@dataclass
class Locator:
    """A unified handle for an item.

    Either id-addressed (``item_id`` set, ``path is None``) or, for Microsoft
    Graph, path-addressed relative to an anchor (``path`` set,
    ``anchor_item_id`` = ``"root"`` or a concrete item id). Path-addressed
    locators are resolved to concrete ids by the proxy before allow-checks.
    """

    drive_id: str
    item_id: str = ""
    path: str | None = None
    anchor_item_id: str | None = None

    @property
    def is_path(self) -> bool:
        return self.path is not None


@dataclass
class DriveBase:
    """Parsed Microsoft Graph drive container + remainder path segments."""

    drive_id: str
    rest: list[str]


@dataclass
class Classification:
    provider: str
    kind: str
    operation: str = ""
    target: Optional[Locator] = None
    parent: Optional[Locator] = None
    is_listing: bool = False
    is_content: bool = False
    is_search: bool = False
    root_listing: bool = False
    changes_list: bool = False


# ── Microsoft Graph ─────────────────────────────────────────────────────────


def _strip_ms_version(segs: list[str]) -> list[str]:
    if segs and segs[0] in ("v1.0", "beta"):
        return segs[1:]
    return segs


def _ms_split_drive_base(rest: str) -> tuple[str, str] | None:
    """Return ``(drive_id, remainder)`` after the drive base, or None.

    ``remainder`` is the string after the drive base and may contain Graph
    path-addressing colons.
    """
    if rest == "me/drive" or rest.startswith("me/drive/"):
        return (_MS_DEFAULT_DRIVE, rest[len("me/drive") :].lstrip("/"))
    if rest.startswith("drives/"):
        remainder = rest[len("drives/") :]
        drive_id, _, after = remainder.partition("/")
        if not drive_id:
            return None
        return (drive_id, after)
    for container, sentinel_key in _MS_CONTAINER_PREFIX.items():
        prefix = f"{container}/"
        if not rest.startswith(prefix):
            continue
        body = rest[len(prefix) :]
        container_id, _, after = body.partition("/")
        if not container_id:
            return None
        if after == "drive" or after.startswith("drive/"):
            remainder = after[len("drive") :].lstrip("/")
            return (f"{sentinel_key}:{container_id}", remainder)
    if rest.startswith("shares/"):
        body = rest[len("shares/") :]
        share_id, _, after = body.partition("/")
        if not share_id:
            return None
        if after == "driveItem" or after.startswith("driveItem/"):
            remainder = after[len("driveItem") :].lstrip("/")
            return (f"share:{share_id}", remainder)
    return None


def _classify_ms_path(
    method: str,
    drive_id: str,
    after: str,
    query: dict[str, str],
) -> Classification | None:
    """Classify a Graph path-addressed request (``root:/path:`` forms)."""
    if after.startswith("root:"):
        anchor_id = "root"
        remainder = after[len("root:") :]
    elif after.startswith("items/"):
        body = after[len("items/") :]
        anchor_id, colon, remainder = body.partition(":")
        if not colon or not anchor_id:
            return None
    else:
        return None

    path_part, _, suffix = remainder.partition(":")
    path = path_part.strip("/")
    loc = Locator(drive_id=drive_id, path=path, anchor_item_id=anchor_id)
    write = method in _WRITE_METHODS

    if suffix == "":
        kind = KIND_FILE_WRITE if write else KIND_FILE_READ
        return Classification("microsoft", kind, "path_item", target=loc)
    if suffix == "/children":
        if write:
            return Classification(
                "microsoft",
                KIND_FILE_WRITE,
                "path_children",
                parent=loc,
            )
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "path_children",
            parent=loc,
            is_listing=True,
        )
    if suffix == "/content":
        kind = KIND_FILE_WRITE if write else KIND_FILE_READ
        return Classification(
            "microsoft",
            kind,
            "path_content",
            target=loc,
            is_content=not write,
        )
    if suffix == "/createUploadSession":
        return Classification("microsoft", KIND_FILE_WRITE, "path_upload", target=loc)
    if suffix == "/copy":
        return Classification("microsoft", KIND_FILE_WRITE, "path_copy", target=loc)
    if suffix.startswith("/search("):
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "path_search",
            parent=loc,
            is_listing=True,
            is_search=True,
        )
    return None


def _ms_is_file_domain(segs: list[str]) -> bool:
    if segs[:2] == ["me", "drive"]:
        return True
    if segs[:2] == ["me", "drives"]:
        return True
    if segs[:1] == ["drives"]:
        return True
    if segs[:1] == ["shares"]:
        return True
    if segs and segs[0] in _MS_CONTAINER_PREFIX:
        return "drive" in segs or "drives" in segs
    return False


def _ms_is_drives_collection_list(segs: list[str]) -> bool:
    if segs == ["drives"]:
        return True
    if segs[:2] == ["me", "drives"] and len(segs) == 2:
        return True
    if len(segs) == 3 and segs[0] in _MS_CONTAINER_PREFIX and segs[2] == "drives":
        return True
    return False


def _ms_parse_drive_base(segs: list[str]) -> DriveBase | None:
    if segs[:2] == ["me", "drive"]:
        return DriveBase(_MS_DEFAULT_DRIVE, segs[2:])
    if segs[:1] == ["drives"] and len(segs) >= 2:
        return DriveBase(segs[1], segs[2:])
    if segs[:1] == ["shares"] and len(segs) >= 2:
        if len(segs) == 2:
            return DriveBase(f"share:{segs[1]}", [])
        if segs[2] == "driveItem":
            return DriveBase(f"share:{segs[1]}", segs[3:])
        return None
    if segs[0] in _MS_CONTAINER_PREFIX and len(segs) >= 3:
        container_id = segs[1]
        sentinel_key = _MS_CONTAINER_PREFIX[segs[0]]
        if segs[2] == "drive":
            return DriveBase(f"{sentinel_key}:{container_id}", segs[3:])
        if segs[2] == "drives" and len(segs) >= 4:
            return DriveBase(segs[3], segs[4:])
    return None


def _classify_ms_item_subresource(
    method: str,
    drive_id: str,
    item_id: str,
    sub: list[str],
    segs_path: str,
) -> Classification | None:
    write = method in _WRITE_METHODS
    if not sub:
        return Classification(
            "microsoft",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "item",
            target=Locator(drive_id, item_id),
        )
    if sub == ["children"]:
        kind = KIND_FILE_WRITE if write else KIND_FILE_READ
        return Classification(
            "microsoft",
            kind,
            "children",
            parent=Locator(drive_id, item_id),
            is_listing=not write,
        )
    if sub == ["content"]:
        return Classification(
            "microsoft",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "content",
            target=Locator(drive_id, item_id),
            is_content=not write,
        )
    if sub == ["delta"] and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "delta",
            parent=Locator(drive_id, item_id),
            is_listing=True,
        )
    if sub and sub[0].startswith("search(") and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "search",
            parent=Locator(drive_id, item_id),
            is_listing=True,
            is_search=True,
        )
    if sub[0] in _MS_ITEM_WRITE_OPS:
        return Classification(
            "microsoft",
            KIND_FILE_WRITE,
            sub[0],
            target=Locator(drive_id, item_id),
        )
    if sub[0] in _MS_ITEM_READ_OPS and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            sub[0],
            target=Locator(drive_id, item_id),
        )
    if sub[0] == "permissions":
        if write:
            return Classification(
                "microsoft",
                KIND_FILE_WRITE,
                "permissions",
                target=Locator(drive_id, item_id),
            )
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "permissions",
            target=Locator(drive_id, item_id),
        )
    if sub[0] == "versions":
        if len(sub) == 1 and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "versions",
                target=Locator(drive_id, item_id),
            )
        if len(sub) == 2 and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "version",
                target=Locator(drive_id, item_id),
            )
        if len(sub) == 3 and sub[2] == "content" and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "version_content",
                target=Locator(drive_id, item_id),
                is_content=True,
            )
    if sub[0] == "unfollow" and write:
        return Classification(
            "microsoft",
            KIND_FILE_WRITE,
            "unfollow",
            target=Locator(drive_id, item_id),
        )
    return None


def _classify_ms_drive_tail(
    method: str,
    drive_id: str,
    rest: list[str],
    segs_path: str,
    query: dict[str, str],
) -> Classification:
    write = method in _WRITE_METHODS

    if not rest:
        return Classification("microsoft", KIND_FILE_READ, "get_drive")

    head = rest[0]
    tail = rest[1:]

    if head in ("recent", "sharedWithMe") and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            head,
            is_listing=True,
        )
    if head in ("following", "bundles") and not tail and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            head,
            is_listing=True,
        )

    if head == "special":
        if not tail and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "special_list",
                is_listing=True,
            )
        if len(tail) == 1:
            return Classification(
                "microsoft",
                KIND_FILE_WRITE if write else KIND_FILE_READ,
                "special_item",
                target=Locator(drive_id, tail[0]),
            )
        return Classification("microsoft", KIND_UNKNOWN, segs_path)

    if head == "children" and not tail and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "children",
            parent=Locator(drive_id, "root"),
            is_listing=True,
        )

    if head == "root":
        if tail == ["children"]:
            kind = KIND_FILE_WRITE if write else KIND_FILE_READ
            return Classification(
                "microsoft",
                kind,
                "root_children",
                parent=Locator(drive_id, "root"),
                is_listing=not write,
            )
        if tail == ["delta"] and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "root_delta",
                parent=Locator(drive_id, "root"),
                is_listing=True,
            )
        if tail and tail[0].startswith("search(") and not write:
            return Classification(
                "microsoft",
                KIND_FILE_READ,
                "search",
                parent=Locator(drive_id, "root"),
                is_listing=True,
                is_search=True,
            )
        if not tail:
            return Classification(
                "microsoft",
                KIND_FILE_WRITE if write else KIND_FILE_READ,
                "root_item",
                target=Locator(drive_id, "root"),
            )
        return Classification("microsoft", KIND_UNKNOWN, segs_path)

    if head == "items" and len(rest) >= 2:
        item_id = rest[1]
        sub = rest[2:]
        classified = _classify_ms_item_subresource(
            method,
            drive_id,
            item_id,
            sub,
            segs_path,
        )
        if classified is not None:
            return classified
        return Classification("microsoft", KIND_UNKNOWN, segs_path)

    if head == "delta" and not tail and not write:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "drive_delta",
            is_listing=True,
        )

    return Classification("microsoft", KIND_UNKNOWN, segs_path)


def _classify_microsoft(
    method: str,
    segs: list[str],
    query: dict[str, str],
) -> Classification:
    segs = _strip_ms_version(segs)
    if not segs:
        return Classification("microsoft", KIND_NON_FILE, "root")

    if segs == ["$batch"] and method == "POST":
        return Classification("microsoft", KIND_BATCH, "batch")

    if not _ms_is_file_domain(segs):
        return Classification("microsoft", KIND_NON_FILE, "/".join(segs))

    rest_str = "/".join(segs)

    if ":" in rest_str:
        parsed = _ms_split_drive_base(rest_str)
        if parsed is not None:
            drive_id, after = parsed
            path_class = _classify_ms_path(method, drive_id, after, query)
            if path_class is not None:
                return path_class
        return Classification("microsoft", KIND_UNKNOWN, rest_str)

    if _ms_is_drives_collection_list(segs):
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "list_drives",
            root_listing=True,
        )

    base = _ms_parse_drive_base(segs)
    if base is None:
        return Classification("microsoft", KIND_UNKNOWN, rest_str)

    if base.drive_id.startswith("share:") and not base.rest:
        return Classification("microsoft", KIND_FILE_READ, "get_share")

    return _classify_ms_drive_tail(method, base.drive_id, base.rest, rest_str, query)


# ── Google Drive ────────────────────────────────────────────────────────────


@dataclass
class GoogleDriveRest:
    """Segments after ``drive/v3`` plus whether the path uses the upload host prefix."""

    rest: list[str]
    upload: bool = False


def _google_normalize_segs(segs: list[str]) -> GoogleDriveRest | None:
    if segs[:2] == ["drive", "v3"]:
        return GoogleDriveRest(segs[2:])
    if segs[:3] == ["upload", "drive", "v3"]:
        return GoogleDriveRest(segs[3:], upload=True)
    return None


def _google_is_file_domain(segs: list[str]) -> bool:
    return _google_normalize_segs(segs) is not None


def _google_colon_action(segment: str) -> tuple[str, str] | None:
    if ":" not in segment:
        return None
    name, _, action = segment.partition(":")
    if not name or not action:
        return None
    return (name, action)


def _classify_google_comments_subresource(
    method: str,
    drive_id: str,
    file_id: str,
    sub: list[str],
) -> Classification | None:
    write = method in _WRITE_METHODS
    loc = Locator(drive_id, file_id)
    if sub == ["comments"]:
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "comments",
            target=loc,
        )
    if len(sub) == 2 and sub[0] == "comments":
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "comment",
            target=loc,
        )
    if len(sub) == 3 and sub[0] == "comments" and sub[2] == "replies":
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "replies",
            target=loc,
        )
    if len(sub) == 4 and sub[0] == "comments" and sub[2] == "replies":
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "reply",
            target=loc,
        )
    return None


def _classify_google_accessproposals_subresource(
    method: str,
    drive_id: str,
    file_id: str,
    sub: list[str],
) -> Classification | None:
    write = method in _WRITE_METHODS
    loc = Locator(drive_id, file_id)
    if sub == ["accessproposals"]:
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "accessproposals",
            target=loc,
        )
    if sub[:1] == ["accessproposals"] and len(sub) == 2:
        action = _google_colon_action(sub[1])
        if action is not None and action[1] == "resolve" and write:
            return Classification(
                "google",
                KIND_FILE_WRITE,
                "accessproposal_resolve",
                target=loc,
            )
        if action is None and not write:
            return Classification(
                "google",
                KIND_FILE_READ,
                "accessproposal",
                target=loc,
            )
    return None


def _classify_google_approvals_subresource(
    method: str,
    drive_id: str,
    file_id: str,
    sub: list[str],
) -> Classification | None:
    write = method in _WRITE_METHODS
    loc = Locator(drive_id, file_id)
    if not sub:
        return None
    head = sub[0]
    head_action = _google_colon_action(head)
    if head == "approvals":
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "approvals",
            target=loc,
        )
    if head_action is not None and head_action[0] == "approvals" and write:
        return Classification(
            "google",
            KIND_FILE_WRITE,
            f"approval_{head_action[1]}",
            target=loc,
        )
    if len(sub) == 2 and sub[0] == "approvals":
        tail_action = _google_colon_action(sub[1])
        if tail_action is not None and write:
            return Classification(
                "google",
                KIND_FILE_WRITE,
                f"approval_{tail_action[1]}",
                target=loc,
            )
        if tail_action is None and not write:
            return Classification(
                "google",
                KIND_FILE_READ,
                "approval",
                target=loc,
            )
    return None


def _classify_google_file_subresource(
    method: str,
    drive_id: str,
    file_id: str,
    sub: list[str],
    query: dict[str, str],
) -> Classification | None:
    write = method in _WRITE_METHODS
    loc = Locator(drive_id, file_id)
    if sub == ["export"]:
        return Classification(
            "google",
            KIND_FILE_READ,
            "export",
            target=loc,
            is_content=True,
        )
    if sub == ["copy"]:
        return Classification("google", KIND_FILE_WRITE, "copy", target=loc)
    if sub == ["download"] and write:
        return Classification("google", KIND_FILE_WRITE, "download", target=loc)
    if sub == ["listLabels"] and not write:
        return Classification("google", KIND_FILE_READ, "listLabels", target=loc)
    if sub == ["modifyLabels"] and write:
        return Classification("google", KIND_FILE_WRITE, "modifyLabels", target=loc)
    if sub == ["permissions"]:
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "permissions",
            target=loc,
        )
    if sub[:1] == ["permissions"] and len(sub) == 2:
        return Classification(
            "google",
            KIND_FILE_WRITE if write else KIND_FILE_READ,
            "permission",
            target=loc,
        )
    if sub == ["revisions"] and not write:
        return Classification(
            "google",
            KIND_FILE_READ,
            "revisions",
            target=loc,
        )
    if sub[:1] == ["revisions"] and len(sub) == 2:
        if write:
            return Classification("google", KIND_FILE_WRITE, "revision", target=loc)
        if (query.get("alt") or "").lower() == "media":
            return Classification(
                "google",
                KIND_FILE_READ,
                "revision_content",
                target=loc,
                is_content=True,
            )
        return Classification("google", KIND_FILE_READ, "revision", target=loc)
    if sub == ["watch"] and write:
        return Classification("google", KIND_FILE_WRITE, "watch", target=loc)
    if sub == ["trash"] and write:
        return Classification("google", KIND_FILE_WRITE, "trash", target=loc)
    for classifier in (
        _classify_google_comments_subresource,
        _classify_google_accessproposals_subresource,
        _classify_google_approvals_subresource,
    ):
        result = classifier(method, drive_id, file_id, sub)
        if result is not None:
            return result
    return None


def _classify_google_drives_tail(
    method: str,
    rest: list[str],
    segs_path: str,
) -> Classification | None:
    write = method in _WRITE_METHODS
    if rest == ["drives"]:
        if write:
            return Classification("google", KIND_FILE_WRITE, "create_drive")
        return Classification(
            "google",
            KIND_FILE_READ,
            "list_drives",
            root_listing=True,
        )
    if rest[:1] == ["drives"] and len(rest) >= 2:
        if len(rest) == 2:
            return Classification(
                "google",
                KIND_FILE_WRITE if write else KIND_FILE_READ,
                "get_drive",
            )
        if rest[2:] == ["emptyTrash"] and write:
            return Classification("google", KIND_FILE_WRITE, "empty_trash")
        if rest[2:] == ["hide"] and write:
            return Classification("google", KIND_FILE_WRITE, "hide_drive")
        if rest[2:] == ["unhide"] and write:
            return Classification("google", KIND_FILE_WRITE, "unhide_drive")
    return None


def _classify_google_top_level(
    method: str,
    rest: list[str],
    query: dict[str, str],
    segs_path: str,
) -> Classification | None:
    write = method in _WRITE_METHODS

    drives = _classify_google_drives_tail(method, rest, segs_path)
    if drives is not None:
        return drives

    if rest == ["about"]:
        return Classification("google", KIND_NON_FILE, "about")

    if rest == ["apps"] and not write:
        return Classification("google", KIND_NON_FILE, "list_apps")
    if rest[:1] == ["apps"] and len(rest) == 2 and not write:
        return Classification("google", KIND_NON_FILE, "get_app")

    if rest == ["channels", "stop"] and write:
        return Classification("google", KIND_NON_FILE, "channels_stop")

    if rest[:1] == ["operations"] and len(rest) == 2 and not write:
        return Classification("google", KIND_NON_FILE, "operation")

    if rest == ["changes"]:
        if write:
            return Classification("google", KIND_FILE_WRITE, "changes_watch")
        return Classification(
            "google",
            KIND_FILE_READ,
            "changes_list",
            is_listing=True,
            changes_list=True,
        )
    if rest == ["changes", "startPageToken"] and not write:
        return Classification("google", KIND_FILE_READ, "changes_start_token")
    if rest == ["changes", "watch"] and write:
        return Classification("google", KIND_FILE_WRITE, "changes_watch")

    if rest == ["files", "generateIds"] and not write:
        return Classification("google", KIND_NON_FILE, "generate_ids")
    if rest == ["files", "generateCseToken"] and not write:
        return Classification("google", KIND_NON_FILE, "generate_cse_token")
    if rest == ["files", "trash"] and write:
        return Classification("google", KIND_FILE_WRITE, "empty_trash")

    if rest == ["files"]:
        if write:
            return Classification("google", KIND_FILE_WRITE, "create")
        return Classification("google", KIND_FILE_READ, "list", is_listing=True)

    return None


def _classify_google(
    method: str,
    segs: list[str],
    query: dict[str, str],
) -> Classification:
    normalized = _google_normalize_segs(segs)
    if normalized is None:
        return Classification("google", KIND_NON_FILE, "/".join(segs))

    rest = normalized.rest
    segs_path = "/".join(segs)
    if not rest:
        return Classification("google", KIND_NON_FILE, "drive_root")

    write = method in _WRITE_METHODS
    drive_id = query.get("driveId") or _MY_DRIVE

    top = _classify_google_top_level(method, rest, query, segs_path)
    if top is not None:
        return top

    if rest[:1] == ["files"] and len(rest) >= 2:
        file_id = rest[1]
        sub = rest[2:]
        classified = _classify_google_file_subresource(
            method,
            drive_id,
            file_id,
            sub,
            query,
        )
        if classified is not None:
            return classified
        if not sub:
            if write:
                return Classification(
                    "google",
                    KIND_FILE_WRITE,
                    "update",
                    target=Locator(drive_id, file_id),
                )
            if (query.get("alt") or "").lower() == "media":
                return Classification(
                    "google",
                    KIND_FILE_READ,
                    "content",
                    target=Locator(drive_id, file_id),
                    is_content=True,
                )
            return Classification(
                "google",
                KIND_FILE_READ,
                "get",
                target=Locator(drive_id, file_id),
            )

    return Classification("google", KIND_UNKNOWN, segs_path)


def classify(
    provider: str,
    method: str,
    rest_path: str,
    query: dict[str, str],
) -> Classification:
    """Classify a proxied request. ``rest_path`` excludes the ``/{provider}`` prefix."""
    segs = [s for s in rest_path.split("/") if s]
    method = method.upper()
    if provider == "microsoft":
        return _classify_microsoft(method, segs, query)
    if provider == "google":
        return _classify_google(method, segs, query)
    return Classification(provider, KIND_UNKNOWN, rest_path)
