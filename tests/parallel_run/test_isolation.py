"""
Tests for terminal-based isolation in parallel_run.sh.

Verifies:
- Each run uses an isolated tmux socket
- Socket name is derived from TTY
- Sessions are isolated between different sockets
- Helper scripts work with isolation
"""

from __future__ import annotations

import os
import subprocess

from tests.parallel_run.conftest import (
    TESTS_DIR,
    list_tmux_sessions,
    get_droid_sockets,
)


class TestSocketIsolation:
    """Tests for socket-based isolation."""

    def test_output_includes_socket_name(self, runner):
        """Script output should include the socket name used.

        The explicit "socket: <name>" line was part of the removed
        Observe section (65bd78f9d, 2025-12-26). The socket name is
        still surfaced — embedded in the log-directory path the runner
        prints (e.g. `logs/pytest/2026-05-28T08-53-55_droid_test_153907/`)
        and exposed on RunResult.socket. Assert presence via either
        path so the test is robust to output reformatting that doesn't
        affect the actual socket-name surfacing contract.
        """
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0
        # The socket name should appear somewhere in stdout (currently
        # embedded in the printed log-directory path) OR be extracted
        # onto RunResult.socket — both indicate the runner surfaced it.
        assert (
            result.socket and result.socket in result.stdout
        ) or "socket:" in result.stdout.lower(), (
            f"socket name should be surfaced; result.socket={result.socket!r}, "
            f"stdout snippet={result.stdout[:300]!r}"
        )

    def test_socket_name_in_result(self, runner):
        """RunResult should have the socket name extracted."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # Socket should be extracted from output
        assert result.socket, "Socket name should be extracted from output"
        assert result.socket.startswith(
            "droid",
        ), f"Socket should start with 'droid': {result.socket}"

    def test_sessions_created_in_isolated_socket(self, runner):
        """Sessions should be created in the isolated socket."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        if result.socket:
            # List sessions from the specific socket
            sessions = list_tmux_sessions(socket=result.socket)
            session_names = [s.name for s in sessions]

            # Our session should be in this socket (may have auto-closed if passed)
            # Just verify the socket exists
            sockets = get_droid_sockets()
            assert result.socket in sockets or len(result.sessions_created) > 0

    def test_explicit_socket_override(self, runner):
        """DROID_TEST_SOCKET env var should override auto-detection."""
        custom_socket = "droid_test_custom_socket"

        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
            env={"DROID_TEST_SOCKET": custom_socket},
        )

        assert result.exit_code == 0
        assert (
            result.socket == custom_socket
        ), f"Expected socket {custom_socket}, got {result.socket}"


class TestHelperScripts:
    """Tests for helper scripts with isolation."""

    def test_watch_tests_script_exists(self):
        """The watch_tests.sh helper should exist and be executable."""
        script = TESTS_DIR / "watch_tests.sh"
        assert script.exists(), f"Script not found: {script}"
        assert os.access(script, os.X_OK), f"Script not executable: {script}"

    def test_kill_failed_supports_all_flag(self):
        """The kill_failed.sh script should support --all flag."""
        script = TESTS_DIR / "kill_failed.sh"
        result = subprocess.run(
            [str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert "--all" in result.stdout, "kill_failed.sh should support --all flag"

    def test_watch_tests_supports_all_flag(self):
        """The watch_tests.sh script should support --all flag."""
        script = TESTS_DIR / "watch_tests.sh"
        result = subprocess.run(
            [str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert "--all" in result.stdout, "watch_tests.sh should support --all flag"


class TestSocketNaming:
    """Tests for socket naming convention."""

    def test_socket_starts_with_droid_prefix(self, runner):
        """Socket names should start with 'droid' prefix."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.socket.startswith(
            "droid",
        ), f"Socket should start with 'droid': {result.socket}"

    def test_multiple_runs_same_terminal_same_socket(self, runner):
        """Multiple runs from same terminal should use same socket."""
        result1 = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        result2 = runner.run(
            runner.fixture_path("test_single_test.py"),
            wait_for_completion=True,
        )

        # Both should use the same socket (same terminal)
        assert (
            result1.socket == result2.socket
        ), f"Expected same socket, got {result1.socket} and {result2.socket}"


# NOTE: TestObserveOutputFormat (test_output_shows_watch_helper /
# test_output_shows_socket_specific_commands) was deleted intentionally.
# Both tests asserted that parallel_run.sh's output contained an "Observe"
# section listing the watch_tests.sh helper and a tmux -L socket-specific
# attach command. That section existed back when the runner was
# non-blocking by default and the user was expected to observe progress in
# another shell.
#
# Commit 65bd78f9d (2025-12-26 "refactor(parallel_run.sh): make blocking
# default, replace --wait with --timeout") made blocking the only mode and
# removed the Observe section as part of that simplification — pass/fail
# results now stream inline as tests complete, so there is no separate
# observe pane to advertise. The two assertions have been failing ever
# since but were masked by the discover_test_paths.py matrix bug.
#
# The tests do not represent a contract worth restoring (the Observe
# section was a hint about a workflow that no longer exists). Remove
# rather than soften.
