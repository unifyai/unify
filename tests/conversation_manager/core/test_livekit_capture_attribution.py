"""Several people can share into one LiveKit room, and each frame is somebody's.

The Recall path has named its presenter for a while (see
``test_meet_screenshare_attribution``). The LiveKit path — Unify Meet, where org
calls actually run — read one frame per source and filed it with no owner, so a
group call with two screens up showed the brains one of them, unlabelled, and a
room of people with cameras on became a single anonymous face.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from unify.conversation_manager.cm_types.screenshot import (
    ScreenshotEntry,
    generate_screenshot_path,
)
from unify.conversation_manager.medium_scripts.common import UserTrackCaptureManager

FIXED_TIME = datetime(2026, 7, 30, 10, 0, tzinfo=timezone.utc)


def _entry(attribution: str | None = None) -> ScreenshotEntry:
    return ScreenshotEntry(
        b64="x",
        utterance="",
        timestamp=FIXED_TIME,
        source="user",
        attribution=attribution,
    )


class _FakeRoom:
    """Just enough LiveKit room to register and drop event handlers."""

    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}

    def on(self, event: str):
        def register(fn):
            self.handlers[event] = fn
            return fn

        return register

    def off(self, event: str, _fn) -> None:
        self.handlers.pop(event, None)


def _rgba(colour: int) -> tuple[bytes, int, int]:
    """A 2x2 RGBA frame of one solid colour."""
    pixel = bytes((colour, colour, colour, 255))
    return (pixel * 4, 2, 2)


def _manager(track_source: str = "screenshare") -> UserTrackCaptureManager:
    return UserTrackCaptureManager(_FakeRoom(), track_source=track_source)


def _seed(manager: UserTrackCaptureManager, *publishers: tuple[str, str, int]) -> None:
    """Install captures directly, bypassing live track subscription."""
    for sid, identity, colour in publishers:
        manager._captures[sid] = {
            "stream": None,
            "task": None,
            "identity": identity,
            "frame": _rgba(colour),
        }
        manager._capture_order.append(sid)


def test_every_publisher_is_read_not_just_the_focused_one():
    manager = _manager()
    _seed(manager, ("sid-a", "user-alice-x1", 10), ("sid-b", "user-bob-x2", 20))

    shots = manager.capture_screenshots()

    assert [identity for _, identity in shots] == ["user-alice-x1", "user-bob-x2"]
    assert len({b64 for b64, _ in shots}) == 2


def test_publishers_are_read_oldest_first():
    """A caller that has to truncate should keep the established share."""
    manager = _manager()
    _seed(manager, ("sid-a", "first", 10), ("sid-b", "second", 20))

    assert [identity for _, identity in manager.capture_screenshots()] == [
        "first",
        "second",
    ]


def test_a_publisher_with_no_frame_yet_is_skipped():
    """Subscribed is not the same as sending; an empty capture has nothing to say."""
    manager = _manager()
    _seed(manager, ("sid-a", "has-frame", 10))
    manager._captures["sid-b"] = {
        "stream": None,
        "task": None,
        "identity": "no-frame",
        "frame": None,
    }
    manager._capture_order.append("sid-b")

    assert [identity for _, identity in manager.capture_screenshots()] == ["has-frame"]


def test_no_publishers_reads_as_empty_not_as_a_blank_frame():
    assert _manager().capture_screenshots() == []


def test_a_limit_keeps_the_longest_standing_shares():
    """Cost scales with sharers, so callers bound it — oldest-first.

    Every frame read here is encoded, written to disk and published over IPC once
    per turn for as long as the share is up.
    """
    manager = _manager()
    _seed(
        manager,
        ("sid-a", "first", 10),
        ("sid-b", "second", 20),
        ("sid-c", "third", 30),
    )

    shots = manager.capture_screenshots(limit=2)

    assert [identity for _, identity in shots] == ["first", "second"]


def test_a_limit_of_zero_reads_nothing():
    manager = _manager()
    _seed(manager, ("sid-a", "first", 10))

    assert manager.capture_screenshots(limit=0) == []


def test_no_limit_reads_every_publisher():
    manager = _manager()
    _seed(manager, ("sid-a", "first", 10), ("sid-b", "second", 20))

    assert len(manager.capture_screenshots()) == 2


def test_focused_read_still_returns_the_newest_publisher():
    """``capture_screenshot`` keeps its latest-presenter-wins contract."""
    manager = _manager()
    _seed(manager, ("sid-a", "older", 10), ("sid-b", "newer", 20))

    focused = manager.capture_screenshot()

    newest = dict((identity, b64) for b64, identity in manager.capture_screenshots())[
        "newer"
    ]
    assert focused == newest


def test_subscription_records_which_participant_published():
    """The publisher is captured at subscribe time; nothing else knows it later.

    The room hands ``(track, publication, participant)`` to the handler and the
    third argument used to be dropped, which is why every frame arrived ownerless.
    """
    manager = _manager()
    track = SimpleNamespace(kind="video-kind")
    publication = SimpleNamespace(sid="sid-a", source="screenshare-source")
    participant = SimpleNamespace(identity="user-carol-x9", name="Carol")

    with (
        patch(
            "livekit.rtc.TrackKind",
            SimpleNamespace(KIND_VIDEO="video-kind"),
        ),
        patch("livekit.rtc.VideoStream", lambda *a, **k: None),
        patch(
            "asyncio.create_task",
            lambda coro: coro.close(),
        ),
    ):
        manager._rtc_source = "screenshare-source"
        manager._handle_track_subscribed(track, publication, participant)

    assert manager._captures["sid-a"]["identity"] == "user-carol-x9"


def test_two_sharers_do_not_overwrite_each_others_files():
    """One folder, one timestamp, two screens — the loser used to vanish."""
    alice = generate_screenshot_path(_entry("Alice"))

    assert alice != generate_screenshot_path(_entry("Bob"))
    assert "Alice" in alice


def test_an_unattributed_frame_keeps_its_plain_filename():
    """The single-user surfaces have no sharer, and gain no suffix for one."""
    path = generate_screenshot_path(_entry())

    assert path.endswith("2026-07-30T10-00-00.000000.jpg")


def test_a_sharer_name_with_path_characters_cannot_escape_the_folder():
    """The name reaches a filesystem path, so it cannot carry separators."""
    path = generate_screenshot_path(_entry("../../etc/passwd"))

    assert ".." not in path
    assert path.startswith("Screenshots/User/")
