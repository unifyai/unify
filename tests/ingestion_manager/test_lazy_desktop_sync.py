"""A file the desktop just downloaded is pulled before it is called missing.

The managed desktop and this workspace share one tree, but they share it
through a sync that runs around desktop *execution*. A file the assistant just
downloaded in a browser therefore exists on the desktop and not yet here, and
ingesting it moments later measures an empty set -- the same shape of silent
shortfall as a listing that omits what it was never asked to include, and the
one that made a shared folder read as revoked for a week.

The sync is triggered only by an absent path under the shared root, so the
ordinary case costs nothing: files already present, and files that were never
on the desktop, do not touch the network.
"""

from __future__ import annotations

import pytest

from unify.ingestion_manager.ingestion_manager import IngestionManager


class _Source:
    def __init__(self, kind, **kw):
        self.kind = kind
        for k, v in kw.items():
            setattr(self, k, v)


class _SyncManager:
    def __init__(self):
        self._started = True
        self.calls = 0

    async def sync_remote_changes(self):
        self.calls += 1
        return True


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    root = tmp_path / "Local"
    (root / "Downloads").mkdir(parents=True)
    monkeypatch.setattr(
        "unify.file_manager.settings.get_local_root",
        lambda: str(root),
    )
    return root


@pytest.fixture()
def syncing(monkeypatch):
    manager = _SyncManager()
    monkeypatch.setattr(IngestionManager, "_sync_manager", lambda self: manager)
    return manager


def _resolve(source):
    return IngestionManager._resolve_paths(object.__new__(IngestionManager), source)


class TestWhenTheSyncRuns:
    def test_an_absent_path_under_the_workspace_pulls_first(self, workspace, syncing):
        missing = workspace / "Downloads" / "repairs.csv"
        _resolve(_Source("files", paths=[str(missing)]))

        assert syncing.calls == 1

    def test_a_present_file_does_not(self, workspace, syncing):
        present = workspace / "Downloads" / "already.csv"
        present.write_text("a,b\n")

        _resolve(_Source("files", paths=[str(present)]))

        assert syncing.calls == 0

    def test_a_path_outside_the_workspace_does_not(self, workspace, syncing, tmp_path):
        # Syncing cannot make it appear, so the round trip would buy nothing.
        _resolve(_Source("files", paths=[str(tmp_path / "elsewhere" / "x.csv")]))

        assert syncing.calls == 0

    def test_one_absent_path_among_present_ones_still_pulls(self, workspace, syncing):
        present = workspace / "Downloads" / "here.csv"
        present.write_text("x\n")
        missing = workspace / "Downloads" / "not-here.csv"

        _resolve(_Source("files", paths=[str(present), str(missing)]))

        assert syncing.calls == 1

    def test_a_folder_source_pulls_when_the_folder_is_absent(self, workspace, syncing):
        _resolve(
            _Source(
                "folder",
                path=str(workspace / "Downloads" / "extract"),
                pattern="*.csv",
                recursive=True,
            ),
        )

        assert syncing.calls == 1


class TestWhenNothingIsSyncing:
    def test_no_sync_manager_is_not_an_error(self, workspace, monkeypatch):
        # Most deployments have no managed desktop at all; ingestion must not
        # depend on one existing.
        monkeypatch.setattr(IngestionManager, "_sync_manager", lambda self: None)

        paths = _resolve(_Source("files", paths=[str(workspace / "gone.csv")]))

        assert paths == [str(workspace / "gone.csv")]

    def test_a_failing_sync_does_not_abandon_the_run(self, workspace, monkeypatch):
        # The paths may be present for another reason, and the run's own
        # reporting is a better place to discover they are not.
        class _Broken:
            _started = True

            async def sync_remote_changes(self):
                raise RuntimeError("sftp down")

        monkeypatch.setattr(IngestionManager, "_sync_manager", lambda self: _Broken())

        paths = _resolve(_Source("files", paths=[str(workspace / "gone.csv")]))

        assert paths == [str(workspace / "gone.csv")]


class TestTheContainmentCheck:
    def test_a_path_inside_the_root_is_recognised(self, tmp_path):
        assert IngestionManager._under(tmp_path, tmp_path / "a" / "b.csv") is True

    def test_a_sibling_directory_is_not_inside(self, tmp_path):
        # Prefix matching on strings would call /root-other a child of /root.
        root = tmp_path / "root"
        root.mkdir()
        assert IngestionManager._under(root, tmp_path / "root-other" / "x") is False
