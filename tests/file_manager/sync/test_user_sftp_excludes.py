"""Pure arg-construction tests for the tiered user-SFTP exclude model.

These assert on the rclone ``--exclude`` args produced for each operation
without needing a live SFTP link or rclone binary.
"""

from pathlib import Path, PurePosixPath

from unity.file_manager.sync.user_sftp import (
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
