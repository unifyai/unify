"""Pure arg-construction tests for the tiered user-SFTP exclude model.

These assert on the rclone ``--exclude`` args produced for each operation
without needing a live SFTP link or rclone binary.
"""

import asyncio
from pathlib import Path, PurePosixPath

import pytest

from unify.file_manager.sync.user_sftp import (
    EDITS_DIR,
    UserHomeSFTP,
    _build_excludes,
    _NOISE_EXCLUDES,
    _SECRET_EXCLUDES,
)


def _patterns(args: list[str]) -> list[str]:
    """Extract the pattern operands following each ``--exclude`` flag."""
    return [args[i + 1] for i, a in enumerate(args) if a == "--exclude"]


def test_tier1_only_when_no_noise_no_secrets():
    pats = _patterns(_build_excludes(noise=False, secrets=False))
    assert pats == [f"/{EDITS_DIR}/**"]
    # No noise / secret patterns leak into the truthful-browse set.
    assert "node_modules/**" not in pats
    assert "/.ssh/**" not in pats


def test_noise_adds_tier2_not_secrets():
    pats = _patterns(_build_excludes(noise=True, secrets=False))
    assert f"/{EDITS_DIR}/**" in pats
    assert "node_modules/**" in pats
    assert ".git/**" in pats
    assert "/.ssh/**" not in pats


def test_secrets_adds_tier3():
    pats = _patterns(_build_excludes(noise=True, secrets=True))
    assert f"/{EDITS_DIR}/**" in pats
    assert "node_modules/**" in pats
    assert "/.ssh/**" in pats
    # Full tier coverage.
    for p in _NOISE_EXCLUDES:
        assert p in pats
    for p in _SECRET_EXCLUDES:
        assert p in pats


def test_secrets_include_any_depth_forms_for_nested_credential_dirs():
    """A pull/sync of a larger subtree must still drop credentials nested
    inside it (e.g. Desktop/proj/.ssh/id_rsa), not just ones at the home root.
    """
    pats = _patterns(_build_excludes(noise=True, secrets=True))
    for p in (
        ".ssh/**",
        ".gnupg/**",
        ".aws/**",
        ".kube/**",
        ".config/gcloud/**",
        ".docker/**",
        ".netrc",
        ".git-credentials",
        "AppData/Roaming/gcloud/**",
        "AppData/Roaming/Microsoft/Credentials/**",
    ):
        assert p in pats
    # The anchored forms stay too — self-documenting, not replaced.
    for p in ("/.ssh/**", "/.aws/**", "/.netrc", "/.config/gcloud/**"):
        assert p in pats


def test_windows_patterns_in_copies_not_in_browse():
    browse = _patterns(_build_excludes(noise=False, secrets=False))
    copy = _patterns(_build_excludes(noise=True, secrets=True))

    # Windows profile noise is skipped on pull/sync ...
    for p in ("/AppData/Local/**", "/NTUSER.DAT*", "/Cookies/**"):
        assert p in copy
        assert p not in browse

    # ... and Windows credential stores never leave the machine.
    assert "/AppData/Roaming/gcloud/**" in copy
    assert "/AppData/Roaming/gcloud/**" not in browse


def test_browse_set_excludes_no_any_depth_secret_patterns():
    """list_dir must stay truthful: no secret pattern, anchored or any-depth,
    should ever reach the browse (secrets=False) exclude set."""
    pats = _patterns(_build_excludes(noise=False, secrets=False))
    for p in (".ssh/**", ".aws/**", ".netrc", ".config/gcloud/**"):
        assert p not in pats


def test_sync_args_exclude_noise_secrets_and_carry_stats():
    client = object.__new__(UserHomeSFTP)
    args = client._sync_args(PurePosixPath("Documents"), Path("/tmp/stage"))

    assert args[0] == "copy"
    assert args[1] == f"{UserHomeSFTP.REMOTE_NAME}:/Documents"
    assert args[2] == "/tmp/stage"

    pats = _patterns(args)
    assert "node_modules/**" in pats
    assert "/.ssh/**" in pats

    # Live-progress flags survive the helper extraction.
    assert "--stats" in args
    assert "--stats-one-line" in args
    assert "--stats-log-level" in args
    assert "NOTICE" in args


def _filters(args: list[str]) -> list[str]:
    """Extract the rule operands following each ``--filter`` flag."""
    return [args[i + 1] for i, a in enumerate(args) if a == "--filter"]


@pytest.mark.asyncio
async def test_pull_copies_the_parent_and_narrows_to_the_entry(monkeypatch):
    """A single-file pull must not use ``copyto`` while filters are set.

    rclone refuses ``copyto`` against one file whenever any filter is present
    ("can't limit to single files when using filters"), so every single-file
    pull failed with the exclude tiers attached.
    """
    client = object.__new__(UserHomeSFTP)
    client._user_id = "u1"
    client._op_lock = asyncio.Lock()
    client._last_error = ""

    captured: list[str] = []

    async def fake_run(args, *, operation, capture=None, stream=False):
        captured.extend(args)
        return True

    monkeypatch.setattr(client, "_run", fake_run)
    dest = await client.pull("Desktop/shot.png")

    assert dest == str(client.local_root / "Desktop" / "shot.png")
    assert captured[0] == "copy"
    assert captured[1] == f"{UserHomeSFTP.REMOTE_NAME}:/Desktop"
    assert captured[2] == str(client.local_root / "Desktop")

    rules = _filters(captured)
    assert "+ /shot.png" in rules
    assert "+ /shot.png/**" in rules  # the entry may be a directory
    assert rules[-1] == "- *"
    # Exclude tiers survive the move to filter form ...
    assert "- /.ssh/**" in rules
    assert "- node_modules/**" in rules
    # ... and none of them ride along as --exclude/--include, whose parse order
    # against --filter is indeterminate (rclone warns at ERROR level).
    assert "--exclude" not in captured
    assert "--include" not in captured


@pytest.mark.asyncio
async def test_pull_failure_names_the_rclone_cause(monkeypatch):
    """A bare "failed to pull" left the actor guessing at unrelated theories."""
    client = object.__new__(UserHomeSFTP)
    client._user_id = "u1"
    client._op_lock = asyncio.Lock()
    client._last_error = "CRITICAL: can't limit to single files when using filters"

    async def fake_run(args, *, operation, capture=None, stream=False):
        return False

    monkeypatch.setattr(client, "_run", fake_run)
    with pytest.raises(RuntimeError, match="can't limit to single files"):
        await client.pull("Desktop/shot.png")
