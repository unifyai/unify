"""
Individual flag tests for parallel_run.sh.

Tests each flag in isolation:
- --timeout / -t
- --serial / -s
- --match / -m
- --env / -e
- --jobs / -j
- --eval-only
- --symbolic-only
- --repeat
- --tags
- --help / -h
- -- (pytest passthrough)
"""

from __future__ import annotations

import time


class TestBlockingBehavior:
    """Tests for default blocking behavior (script always blocks until completion)."""

    def test_blocks_until_completion(self, runner):
        """Script should block until all tests complete."""
        start = time.time()
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
        )
        elapsed = time.time() - start

        # Should have blocked for some time (tests take at least a moment)
        # but not forever (300s upper bound to handle resource contention)
        assert elapsed > 0.5, "Should have waited for tests"
        assert elapsed < 300, "Should not wait forever"

    def test_returns_zero_on_all_pass(self, runner):
        """Script should return 0 when all tests pass."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
        )

        assert (
            result.exit_code == 0
        ), f"Should return 0 on success, got {result.exit_code}. stderr: {result.stderr}"

    def test_returns_nonzero_on_failure(self, runner):
        """Script should return non-zero when any test fails."""
        result = runner.run(
            runner.fixture_path("test_always_fail.py"),
        )

        assert (
            result.exit_code != 0
        ), f"Should return non-zero on failure, got {result.exit_code}"

    def test_returns_nonzero_on_mixed_results(self, runner):
        """Script should return non-zero when some tests fail."""
        result = runner.run(
            runner.fixture_path("test_mixed_results.py"),
        )

        assert result.exit_code != 0, "Should return non-zero on partial failure"

    def test_creates_log_files(self, runner):
        """Script should create log files in logs/pytest/{socket}/."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
        )

        # Should have created at least one log file
        assert len(result.log_files) >= 1, "Should create log files"


class TestTimeoutFlag:
    """Tests for --timeout / -t flag."""

    def test_timeout_completes_normally(self, runner):
        """--timeout N should complete normally if tests finish before timeout."""
        import time

        start = time.time()
        result = runner.run(
            "--timeout",
            "300",  # 5 minute timeout - handles stress test scenarios
            runner.fixture_path("test_always_pass.py"),
        )
        elapsed = time.time() - start

        # Should complete successfully (not timeout)
        assert result.exit_code == 0, f"Should pass within timeout: {result.stderr}"
        # Should complete within reasonable time (300s upper bound for stress test scenarios)
        assert elapsed < 300, f"Should complete within timeout, took {elapsed:.1f}s"

    def test_timeout_times_out(self, runner):
        """--timeout N should timeout and exit with code 2 if tests don't complete in time."""
        # Use a very short timeout (2 seconds) - tests won't complete in time
        result = runner.run(
            "--timeout",
            "2",  # 2 second timeout - too short
            runner.fixture_path("test_always_pass.py"),
        )

        # Should timeout with exit code 2
        assert (
            result.exit_code == 2
        ), f"Should timeout with code 2, got {result.exit_code}"
        # Should mention timeout in output
        assert "timeout" in result.stdout.lower() or "Timeout" in result.stdout

    def test_timeout_with_short_flag(self, runner):
        """-t N should work the same as --timeout N."""
        result = runner.run(
            "-t",
            "300",  # 5 minute timeout - handles stress test scenarios
            runner.fixture_path("test_always_pass.py"),
        )

        assert (
            result.exit_code == 0
        ), f"Short -t with timeout should work: {result.stderr}"

    def test_timeout_default_per_test(self, runner):
        """--timeout N should work correctly with default per-test mode."""
        result = runner.run(
            "--timeout",
            "300",  # 5 minute timeout - handles stress test scenarios
            runner.fixture_path("test_always_pass.py"),
        )

        assert result.exit_code == 0, f"Should pass: {result.stderr}"


class TestSessionTimeoutFlag:
    """Tests for --session-timeout (per-tmux-session hang kill)."""

    def test_session_timeout_completes_normally(self, runner):
        """--session-timeout N should not kill sessions that finish quickly."""
        result = runner.run(
            "--session-timeout",
            "120",
            runner.fixture_path("test_always_pass.py"),
        )
        assert (
            result.exit_code == 0
        ), f"Fast tests should pass under session-timeout: {result.stderr}"
        assert "Per-session hang guard" in result.stdout

    def test_session_timeout_kills_hung_session(self, runner):
        """--session-timeout N should fail a hung session instead of waiting forever."""
        result = runner.run(
            "--session-timeout",
            "3",
            "--timeout",
            "60",
            runner.hang_fixture_path("test_hang.py"),
        )
        # Hung session is killed by timeout(1) → non-zero pytest status → runner exit 1
        # (not whole-run exit 2, which only fires when sessions are still pending).
        assert result.exit_code != 0, (
            f"Hung session should fail under --session-timeout, got 0. "
            f"stdout={result.stdout[-500:]!r} stderr={result.stderr[-500:]!r}"
        )
        assert result.exit_code != 2, (
            "Should fail the hung session (exit 1), not burn the whole-run timeout (exit 2). "
            f"stdout={result.stdout[-500:]!r}"
        )


class TestSerialFlag:
    """Tests for --serial / -s flag."""

    def test_serial_creates_session_per_file(self, runner):
        """--serial should create one session per file (not per test)."""
        result = runner.run(
            "-s",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # test_always_pass.py has 3 test functions, but -s creates 1 session per file
        assert (
            len(result.sessions_created) == 1
        ), f"Expected 1 session (one per file), got {len(result.sessions_created)}: {result.sessions_created}"

    def test_serial_session_names_include_file_name(self, runner):
        """Serial mode session names should include the file name."""
        result = runner.run(
            "-s",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert len(result.sessions_created) == 1
        session_name = result.sessions_created[0]
        # Session name should contain file identifier
        assert (
            "always_pass" in session_name or "test_always_pass" in session_name
        ), f"Session name should reference file name: {session_name}"

    def test_serial_long_flag(self, runner):
        """--serial should work same as -s."""
        result = runner.run(
            "--serial",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert len(result.sessions_created) == 1

    def test_serial_with_directory(self, runner, fixtures_dir):
        """--serial with directory should create session per file."""
        # Just test with subdirectory to keep it manageable
        result = runner.run(
            "-s",
            runner.fixture_path("subdir"),
            wait_for_completion=True,
        )

        # subdir has 1 test file (test_in_subdir.py)
        assert (
            len(result.sessions_created) == 1
        ), f"Expected 1 session, got {len(result.sessions_created)}"

    def test_serial_blocks_until_completion(self, runner):
        """--serial should block until all file sessions complete."""
        result = runner.run(
            "-s",
            runner.fixture_path("test_always_pass.py"),
        )

        # Should block and return success
        assert result.exit_code == 0


class TestDefaultPerTestMode:
    """Tests verifying default per-test behavior."""

    def test_default_creates_session_per_function(self, runner):
        """Default mode should create one session per test function."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # test_always_pass.py has 3 test functions
        assert (
            len(result.sessions_created) == 3
        ), f"Expected 3 sessions (one per test), got {len(result.sessions_created)}: {result.sessions_created}"

    def test_default_session_names_include_test_name(self, runner):
        """Default mode session names should include the test function name."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # Session names should contain test function identifiers
        session_names_joined = " ".join(result.sessions_created)
        assert (
            "test_pass" in session_names_joined
            or "pass" in session_names_joined.lower()
        ), f"Session names should reference test names: {result.sessions_created}"

    def test_default_with_directory(self, runner, fixtures_dir):
        """Default mode with directory should create session per test function."""
        # Just test with subdirectory to keep it manageable
        result = runner.run(
            runner.fixture_path("subdir"),
            wait_for_completion=True,
        )

        # subdir/test_in_subdir.py has 2 tests
        assert (
            len(result.sessions_created) == 2
        ), f"Expected 2 sessions, got {len(result.sessions_created)}"


class TestMatchFlag:
    """Tests for --match / -m flag."""

    def test_match_filters_by_filename(self, runner, fixtures_dir):
        """--match should filter files by name pattern."""
        result = runner.run(
            "-s",  # Serial mode for predictable session count
            "--match",
            "*docstring*",
            fixtures_dir,
            wait_for_completion=True,
        )

        # Should only find test_docstring_pattern.py (1 file)
        assert (
            len(result.sessions_created) == 1
        ), f"Expected 1 matching file, got {len(result.sessions_created)}"

    def test_match_with_wildcard(self, runner, fixtures_dir):
        """--match should support wildcard patterns."""
        result = runner.run(
            "-s",  # Serial mode for predictable session count
            "-m",
            "*always*",
            fixtures_dir,
            wait_for_completion=True,
        )

        # Should find test_always_pass.py and test_always_fail.py (2 files)
        assert (
            len(result.sessions_created) == 2
        ), f"Expected 2 matching files, got {len(result.sessions_created)}"

    def test_match_no_matches(self, runner, fixtures_dir):
        """--match with no matches should find no tests."""
        result = runner.run(
            "-m",
            "*nonexistent_pattern*",
            fixtures_dir,
        )

        # Should report no tests found
        assert "No tests" in result.stdout or len(result.sessions_created) == 0


class TestEnvFlag:
    """Tests for --env / -e flag."""

    def test_env_single_value(self, runner):
        """--env should pass environment variable to pytest sessions."""
        result = runner.run(
            "--env",
            "TEST_VAR=test_value",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_env_multiple_values(self, runner):
        """Multiple --env flags should all be passed."""
        result = runner.run(
            "-e",
            "VAR1=value1",
            "-e",
            "VAR2=value2",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_env_requires_value(self, runner):
        """--env without a value should error."""
        result = runner.run(
            "--env",
        )

        assert result.exit_code != 0 or "Error" in result.stderr


class TestEvalOnlyFlag:
    """Tests for --eval-only flag."""

    def test_eval_only_runs_only_eval_tests(self, runner):
        """--eval-only should only run tests marked with pytest.mark.eval."""
        # Use specific file to avoid slow collection across entire fixtures dir
        result = runner.run(
            "--eval-only",
            runner.fixture_path("test_eval_marked.py"),
            wait_for_completion=True,
        )

        # test_eval_marked.py has 2 eval tests, default is per-test mode
        assert len(result.sessions_created) == 2
        assert result.exit_code == 0

    def test_eval_only_with_serial(self, runner, fixtures_dir):
        """--eval-only with -s should create one session per file with eval tests."""
        result = runner.run(
            "--eval-only",
            "-s",
            fixtures_dir,
            wait_for_completion=True,
        )

        # Should find 1 file with eval tests (test_eval_marked.py)
        assert len(result.sessions_created) == 1


class TestSymbolicOnlyFlag:
    """Tests for --symbolic-only flag."""

    def test_symbolic_only_excludes_eval_tests(self, runner):
        """--symbolic-only should exclude tests marked with pytest.mark.eval."""
        # Use specific file to avoid slow collection across entire fixtures dir
        result = runner.run(
            "--symbolic-only",
            runner.fixture_path("test_symbolic_only.py"),
            wait_for_completion=True,
        )

        # test_symbolic_only.py has 2 tests, none marked as eval (default per-test mode)
        assert len(result.sessions_created) == 2
        assert result.exit_code == 0

    def test_symbolic_only_with_serial(self, runner):
        """--symbolic-only with -s should create one session per file."""
        result = runner.run(
            "--symbolic-only",
            "-s",
            runner.fixture_path("test_symbolic_only.py"),
            wait_for_completion=True,
        )

        # Should create 1 session (per-file mode)
        assert len(result.sessions_created) == 1


class TestEvalSymbolicMutualExclusion:
    """Tests for --eval-only and --symbolic-only mutual exclusion."""

    def test_eval_and_symbolic_mutually_exclusive(self, runner):
        """--eval-only and --symbolic-only together should error."""
        result = runner.run(
            "--eval-only",
            "--symbolic-only",
            runner.fixture_path("test_always_pass.py"),
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.stderr.lower()


class TestRepeatFlag:
    """Tests for --repeat flag."""

    def test_repeat_creates_multiple_sessions(self, runner):
        """--repeat N should create N sessions per test target."""
        result = runner.run(
            "--repeat",
            "3",
            runner.fixture_path("test_single_test.py"),
            wait_for_completion=True,
        )

        # Should create 3 sessions for the single file
        assert (
            len(result.sessions_created) == 3
        ), f"Expected 3 sessions, got {len(result.sessions_created)}"

    def test_repeat_default_per_test(self, runner):
        """--repeat with default per-test mode should multiply correctly."""
        result = runner.run(
            "--repeat",
            "2",
            runner.fixture_path("test_single_test.py"),
            wait_for_completion=True,
        )

        # test_single_test.py has 1 test, repeated 2 times = 2 sessions
        assert (
            len(result.sessions_created) == 2
        ), f"Expected 2 sessions, got {len(result.sessions_created)}"

    def test_repeat_requires_number(self, runner):
        """--repeat without a number should error."""
        result = runner.run(
            "--repeat",
        )

        assert result.exit_code != 0

    def test_repeat_requires_positive(self, runner):
        """--repeat with 0 or negative should error."""
        result = runner.run(
            "--repeat",
            "0",
            runner.fixture_path("test_single_test.py"),
        )

        assert result.exit_code != 0


class TestTagsFlag:
    """Tests for --tags flag."""

    def test_tags_accepted(self, runner):
        """--tags should be accepted without error."""
        result = runner.run(
            "--tags",
            "test-tag",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_tags_multiple(self, runner):
        """--tags with comma-separated values should work."""
        result = runner.run(
            "--tags",
            "tag1,tag2,tag3",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_tags_requires_value(self, runner):
        """--tags without a value should error."""
        result = runner.run(
            "--tags",
        )

        assert result.exit_code != 0 or "Error" in result.stderr


class TestJobsFlag:
    """Tests for --jobs / -j flag (concurrency limiting)."""

    def test_jobs_limits_concurrency(self, runner):
        """--jobs N should limit concurrent sessions."""
        result = runner.run(
            "-j",
            "2",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # Should succeed with limited concurrency
        assert result.exit_code == 0
        # test_always_pass.py has 3 tests, all should still run
        # Use log_files count since sessions may auto-close before parsing
        assert len(result.log_files) == 3
        # Verify concurrency limit was applied
        assert "Concurrency limit: 2" in result.stdout

    def test_jobs_long_flag(self, runner):
        """--jobs N should work same as -j N."""
        result = runner.run(
            "--jobs",
            "5",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_jobs_with_value_one(self, runner):
        """--jobs 1 should run tests sequentially."""
        result = runner.run(
            "-j",
            "1",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # Should still succeed, just slower
        assert result.exit_code == 0
        # Use log_files count since sessions may auto-close before parsing
        assert len(result.log_files) == 3
        # Verify sequential mode was applied
        assert "Concurrency limit: 1" in result.stdout

    def test_jobs_requires_number(self, runner):
        """--jobs without a number should error."""
        result = runner.run(
            "--jobs",
        )

        assert result.exit_code != 0

    def test_jobs_zero_means_unlimited(self, runner):
        """--jobs 0 should mean unlimited concurrency."""
        result = runner.run(
            "-j",
            "0",
            runner.fixture_path("test_always_pass.py"),
            wait_for_completion=True,
        )

        # 0 means no limit, not an error
        assert result.exit_code == 0
        assert "Concurrency limit: unlimited" in result.stdout


class TestHelpFlag:
    """Tests for --help / -h flag."""

    def test_help_shows_usage(self, runner):
        """--help should show usage information."""
        result = runner.run("--help")

        assert result.exit_code == 0
        assert "Usage:" in result.stdout
        assert "parallel_run" in result.stdout.lower()

    def test_help_shows_options(self, runner):
        """--help should list available options."""
        result = runner.run("--help")

        assert "--timeout" in result.stdout
        assert "--serial" in result.stdout
        assert "--match" in result.stdout
        assert "--env" in result.stdout
        assert "--jobs" in result.stdout
        assert "--eval-only" in result.stdout
        assert "--symbolic-only" in result.stdout
        assert "--repeat" in result.stdout
        assert "--tags" in result.stdout

    def test_help_short_flag(self, runner):
        """-h should work same as --help."""
        result = runner.run("-h")

        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_help_shows_examples(self, runner):
        """--help should show usage examples."""
        result = runner.run("--help")

        assert "Examples:" in result.stdout


class TestPytestPassthrough:
    """Tests for -- pytest argument passthrough."""

    def test_passthrough_verbose(self, runner):
        """Arguments after -- should be passed to pytest."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            "--",
            "-v",
            wait_for_completion=True,
        )

        # Should succeed
        assert result.exit_code == 0

    def test_passthrough_multiple_args(self, runner):
        """Multiple arguments after -- should all be passed."""
        result = runner.run(
            runner.fixture_path("test_always_pass.py"),
            "--",
            "-v",
            "--tb=short",
            wait_for_completion=True,
        )

        assert result.exit_code == 0

    def test_passthrough_with_other_flags(self, runner):
        """-- passthrough should work with other parallel_run flags."""
        result = runner.run(
            "-s",  # serial mode
            runner.fixture_path("test_always_pass.py"),
            "--",
            "-v",
            wait_for_completion=True,
        )

        assert result.exit_code == 0
        # Serial mode: 1 session for the file
        assert len(result.sessions_created) == 1


class TestEvalOnlyDiscoveryAgreement:
    """The CI matrix discovery must agree with what --eval-only will select.

    These live together because the two halves failing to agree is a real
    failure mode, not a hypothetical: parallel_run.sh exits non-zero when a
    marker filter selects no test, so a matrix entry that discovery hands over
    without eval tests fails for having nothing to run rather than for anything
    being wrong.
    """

    SCRIPT = ".github/scripts/discover_test_paths.py"

    def _root(self):
        from pathlib import Path

        return Path(__file__).resolve().parents[2]

    def _script_module(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "_discover",
            self._root() / self.SCRIPT,
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _discover(self, *args: str) -> list[str]:
        import subprocess
        import sys
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        out = subprocess.run(
            [sys.executable, self.SCRIPT, *args],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return [line for line in out.splitlines() if line.strip()]

    def test_eval_only_narrows_the_matrix(self):
        assert 0 < len(self._discover("--eval-only")) < len(self._discover())

    def test_every_eval_entry_has_an_eval_test(self):
        """An entry the filter would empty is the hard-failure case."""
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        for entry in self._discover("--eval-only"):
            members = []
            for token in entry.split():
                target = root / token
                members.extend(
                    target.glob("test_*.py") if target.is_dir() else [target],
                )
            assert any(
                "pytest.mark.eval" in m.read_text(encoding="utf-8") for m in members
            ), f"discovery offered {entry!r}, which --eval-only would empty"

    def test_filter_is_opt_in(self):
        """Without the flag the matrix still carries paths that have no evals."""
        unfiltered = set(self._discover())
        eval_only = set(self._discover("--eval-only"))
        assert unfiltered - eval_only, "the filter dropped nothing, so it is inert"

    def test_a_path_without_evals_yields_nothing_rather_than_erroring(self):
        """Narrowing an eval-free path is empty, not a failure.

        Callers discover per path, so this has to stay quiet — the alternative
        is a matrix that cannot be built for directories that simply have no
        eval tests.
        """
        assert self._discover("--eval-only", "tests/parallel_run") == []

    def test_symbolic_only_narrows_the_matrix(self):
        """The inverse filter needs the same narrowing, for the same reason."""
        assert 0 < len(self._discover("--symbolic-only")) < len(self._discover())

    def test_every_symbolic_entry_has_a_non_eval_test(self):
        """An all-eval directory handed to --symbolic-only is the failure case.

        This is what a full-matrix sweep tripped over: ten pure-eval
        directories reported as failures for having nothing left to run.
        """
        import importlib.util
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location("_d", root / self.SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        for entry in self._discover("--symbolic-only"):
            members = []
            for token in entry.split():
                target = root / token
                members.extend(
                    target.glob("test_*.py") if target.is_dir() else [target],
                )
            assert any(
                mod.marker_content(m)[1] for m in members
            ), f"discovery offered {entry!r}, which --symbolic-only would empty"

    def test_marker_applied_through_an_alias_is_recognised(self):
        """`x = pytest.mark.eval` then `@x` marks tests just as the dotted form does."""
        import importlib.util
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location("_d", root / self.SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        aliased = root / "tests/function_manager/core/test_simulated.py"
        assert mod.marker_content(aliased)[
            0
        ], "alias-applied eval markers must count, or an eval sweep skips them"

    def test_the_two_marker_flags_are_mutually_exclusive(self):
        import subprocess
        import sys
        from pathlib import Path

        root = Path(__file__).resolve().parents[2]
        done = subprocess.run(
            [sys.executable, self.SCRIPT, "--eval-only", "--symbolic-only"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        assert done.returncode != 0
        assert not done.stdout.strip()

    def test_deterministic_only_narrows_the_matrix(self):
        assert 0 < len(self._discover("--deterministic-only")) < len(self._discover())

    def test_every_deterministic_entry_has_a_model_free_test(self):
        """A shard of purely model-backed tests is the failure case.

        "not eval" still admits every llm_call test, and those answer to
        whatever the shared cache holds — under read-only a request with no
        recorded entry raises, so a sweep for broken tests reports cache gaps
        instead. This selection has to leave only shards that can answer for
        themselves.
        """
        mod = self._script_module()
        root = self._root()
        for entry in self._discover("--deterministic-only"):
            members = []
            for token in entry.split():
                target = root / token
                members.extend(
                    target.glob("test_*.py") if target.is_dir() else [target],
                )
            assert any(
                mod.marker_content(m).has_deterministic for m in members
            ), f"discovery offered {entry!r}, which --deterministic-only empties"

    def test_llm_call_alone_is_not_deterministic(self):
        """An llm_call test without the eval marker still is not model-free."""
        mod = self._script_module()
        content = mod.marker_content(
            self._root() / "tests/event_bus/test_spending.py",
        )
        assert content.has_non_eval, "no eval marker, so a 'not eval' run keeps it"
        assert content.has_deterministic, "and it does hold model-free tests too"
