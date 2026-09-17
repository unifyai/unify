"""
Store plumbing tests for parallel_run.sh.

Every pytest session opens the SQLite store named by UNIFY_STORE_PATH. The
runner hands each session its own file under the run's log directory unless
the caller already chose a store, in which case every session shares it.
"""

from pathlib import Path

import pytest


class TestPerSessionStores:
    """Without a caller-provided store, every session gets its own."""

    def test_each_session_gets_its_own_store(self, runner):
        result = runner.run(
            "--repeat",
            "3",
            runner.store_fixture_path("test_report_env.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0, result.stdout + result.stderr
        assert result.log_dir is not None
        stores_dir = result.log_dir / "stores"
        assert stores_dir.is_dir(), "stores/ must live under the run's log directory"

        stores = sorted(stores_dir.glob("*.sqlite"))
        assert len(stores) == 3, [s.name for s in stores]
        assert len({s.name for s in stores}) == 3, "session stores must not collide"

        reports = runner.session_env_reports(result)
        assert len(reports) == 3
        reported_paths = {Path(r["UNIFY_STORE_PATH"]).resolve() for r in reports}
        assert reported_paths == {s.resolve() for s in stores}

    def test_store_is_named_after_the_session(self, runner):
        result = runner.run(
            runner.store_fixture_path("test_report_env.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0, result.stdout + result.stderr
        assert len(result.sessions_created) == 1
        base_name = result.sessions_created[0]
        for prefix in ("p ✅ ", "f ❌ ", "r ⏳ "):
            if base_name.startswith(prefix):
                base_name = base_name[len(prefix) :]
        assert (result.log_dir / "stores" / f"{base_name}.sqlite").is_file()

    def test_store_survives_the_session(self, runner):
        """The store stays on disk next to the log for post-mortem queries."""
        result = runner.run(
            runner.store_fixture_path("test_report_env.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0, result.stdout + result.stderr
        assert list((result.log_dir / "stores").glob("*.sqlite"))

    def test_banner_names_the_stores_directory(self, runner):
        result = runner.run(
            runner.store_fixture_path("test_report_env.py"),
            wait_for_completion=True,
        )

        assert result.exit_code == 0
        assert "stores/" in result.stdout


class TestCallerProvidedStore:
    """A UNIFY_STORE_PATH the caller set is shared by every session."""

    @pytest.mark.parametrize("via", ["environment", "env-flag"])
    def test_caller_store_is_honoured(self, runner, tmp_path, via):
        shared = tmp_path / "shared.sqlite"
        args = ["--repeat", "2", runner.store_fixture_path("test_report_env.py")]
        env = {}
        if via == "environment":
            env["UNIFY_STORE_PATH"] = str(shared)
        else:
            args = ["--env", f"UNIFY_STORE_PATH={shared}", *args]

        result = runner.run(*args, wait_for_completion=True, env=env)

        assert result.exit_code == 0, result.stdout + result.stderr
        assert shared.is_file(), "sessions must open the caller's store"
        assert not (
            result.log_dir / "stores"
        ).exists(), "no per-session stores when the caller chose one"
        reports = runner.session_env_reports(result, store=shared)
        assert reports and all(
            Path(r["UNIFY_STORE_PATH"]).resolve() == shared.resolve() for r in reports
        )
        assert str(shared) in result.stdout
