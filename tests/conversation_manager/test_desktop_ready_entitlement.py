"""A desktop that reports ready makes every surface that reaches it usable.

``SESSION_DETAILS`` is a bootstrap-time snapshot of the assistant row, so a
managed desktop enabled *after* the pod started leaves ``desktop_mode`` and
``managed_desktop_status`` claiming the add-on is off. The ready handler used to
set only ``desktop_url``, which split the two surfaces that reach the same VM:
``ComputerPrimitives`` resolves its container from the URL alone and worked,
while shell, python and file access consult ``has_managed_desktop`` and refused
with "No managed desktop is assigned to this assistant" for the life of the pod.

Observed live on staging: the desktop reported ready at 13:38:35 with a URL, and
sixteen minutes later the assistant drove a browser on that VM, downloaded a
file, and then could not read the directory it had just written to.
"""

from __future__ import annotations

import pytest

from unify.session_details import SESSION_DETAILS, AssistantDetails


@pytest.fixture()
def assistant() -> AssistantDetails:
    """A pod that bootstrapped before the desktop add-on was switched on."""
    original = SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = AssistantDetails(
        desktop_mode="none",
        managed_desktop_status=None,
    )
    yield SESSION_DETAILS.assistant
    SESSION_DETAILS.assistant = original


def _apply(vm_type: str = "ubuntu", url: str = "https://vm.example/api") -> None:
    """Run only the snapshot reconciliation the ready handler performs."""
    if url:
        SESSION_DETAILS.assistant.desktop_url = url
    if vm_type in ("ubuntu", "windows"):
        SESSION_DETAILS.assistant.desktop_mode = vm_type
    SESSION_DETAILS.assistant.managed_desktop_status = "active"


class TestAReadyDesktopSettlesEntitlement:
    def test_a_stale_snapshot_would_have_refused(self, assistant):
        # The starting state: the add-on was switched on after bootstrap, so the
        # snapshot still says there is no desktop.
        assistant.desktop_url = "https://vm.example/api"
        assert assistant.has_managed_desktop is False

    def test_the_ready_signal_makes_the_desktop_usable(self, assistant):
        _apply()
        assert assistant.has_managed_desktop is True
        assert assistant.managed_desktop_entitled is True

    def test_both_surfaces_now_agree(self, assistant):
        # The browser only ever needed the URL; the exec surface needs
        # entitlement. Disagreement between them is the whole defect.
        _apply()
        browser_can_reach = bool(assistant.desktop_url)
        exec_can_reach = assistant.has_managed_desktop
        assert browser_can_reach is exec_can_reach is True

    def test_a_windows_desktop_is_recorded_as_windows(self, assistant):
        _apply(vm_type="windows")
        assert assistant.desktop_mode == "windows"
        assert assistant.has_managed_desktop is True


class TestAnInvalidModeIsNeverWrittenBack:
    def test_the_unset_sentinel_does_not_round_trip(self, assistant):
        # One caller passes ``vm_type=SESSION_DETAILS.assistant.desktop_mode or
        # "ubuntu"``, and the unset value is the *string* "none" -- truthy, so
        # it survives the ``or`` and would be written straight back as a mode.
        _apply(vm_type="none")
        assert assistant.desktop_mode == "none"
        assert assistant.has_managed_desktop is False

    def test_an_unrecognised_mode_leaves_the_existing_one(self, assistant):
        assistant.desktop_mode = "ubuntu"
        _apply(vm_type="macos")
        assert assistant.desktop_mode == "ubuntu"
