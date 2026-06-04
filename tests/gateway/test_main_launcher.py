"""Tests for ``python -m unity.gateway`` (the ``__main__`` entrypoint).

We don't actually start uvicorn -- patching it lets us assert that
the launcher passes the right host / port / log level / reload flags
through.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Argument parsing + flag plumbing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_defaults_match_documented_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No flags, no env vars: defaults to 0.0.0.0:8080, info, no reload."""
        for var in (
            "UNITY_GATEWAY_HOST",
            "UNITY_GATEWAY_PORT",
            "UNITY_GATEWAY_LOG_LEVEL",
            "UNITY_GATEWAY_RELOAD",
        ):
            monkeypatch.delenv(var, raising=False)

        from unity.gateway import __main__ as launcher

        parser = launcher._build_parser()
        args = parser.parse_args([])
        assert args.host == "0.0.0.0"
        assert args.port == 8080
        assert args.log_level == "info"
        assert args.reload is False

    def test_env_vars_override_defaults(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("UNITY_GATEWAY_HOST", "127.0.0.1")
        monkeypatch.setenv("UNITY_GATEWAY_PORT", "9000")
        monkeypatch.setenv("UNITY_GATEWAY_LOG_LEVEL", "debug")
        monkeypatch.setenv("UNITY_GATEWAY_RELOAD", "true")

        from unity.gateway import __main__ as launcher

        parser = launcher._build_parser()
        args = parser.parse_args([])
        assert args.host == "127.0.0.1"
        assert args.port == 9000
        assert args.log_level == "debug"
        assert args.reload is True

    def test_cli_flags_override_env_vars(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("UNITY_GATEWAY_PORT", "9000")

        from unity.gateway import __main__ as launcher

        parser = launcher._build_parser()
        args = parser.parse_args(["--port", "7777"])
        assert args.port == 7777


class TestMain:
    def test_main_invokes_uvicorn_with_correct_args(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for var in (
            "UNITY_GATEWAY_HOST",
            "UNITY_GATEWAY_PORT",
            "UNITY_GATEWAY_LOG_LEVEL",
            "UNITY_GATEWAY_RELOAD",
        ):
            monkeypatch.delenv(var, raising=False)

        from unity.gateway import __main__ as launcher

        with patch("uvicorn.run") as mock_run:
            exit_code = launcher.main([])

        assert exit_code == 0
        mock_run.assert_called_once_with(
            "unity.gateway.app:app",
            host="0.0.0.0",
            port=8080,
            log_level="info",
            reload=False,
        )

    def test_main_propagates_explicit_flags_to_uvicorn(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("UNITY_GATEWAY_RELOAD", raising=False)

        from unity.gateway import __main__ as launcher

        with patch("uvicorn.run") as mock_run:
            launcher.main(
                [
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "9090",
                    "--log-level",
                    "warning",
                    "--reload",
                ],
            )
        mock_run.assert_called_once_with(
            "unity.gateway.app:app",
            host="127.0.0.1",
            port=9090,
            log_level="warning",
            reload=True,
        )

    def test_main_help_does_not_import_uvicorn(
        self,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """``--help`` should print + exit before importing uvicorn.

        The launcher lazy-imports uvicorn inside main() to keep
        ``python -m unity.gateway --help`` snappy.
        """
        from unity.gateway import __main__ as launcher

        with pytest.raises(SystemExit) as ctx:
            launcher.main(["--help"])
        # argparse exits with code 0 on --help
        assert ctx.value.code == 0
        out = capsys.readouterr().out
        assert "unity.gateway" in out
        assert "--host" in out

    def test_urls_command_prints_callback_urls(
        self,
        capsys: pytest.CaptureFixture,
    ) -> None:
        from unity.gateway import __main__ as launcher

        exit_code = launcher.main(
            [
                "urls",
                "--public-url",
                "https://callbacks.example.com",
                "--channels",
                "twilio",
            ],
        )

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Inbound SMS webhook: https://callbacks.example.com/twilio/sms" in out
        assert "Call TwiML callback: https://callbacks.example.com/phone/twiml" in out

    def test_setup_prints_guidance_without_writing_files(
        self,
        capsys: pytest.CaptureFixture,
        tmp_path,
    ) -> None:
        from unity.gateway import __main__ as launcher

        env_file = tmp_path / ".env"
        exit_code = launcher.main(
            [
                "setup",
                "--print",
                "--channels",
                "slack",
                "--public-url",
                "https://callbacks.example.com",
                "--env-file",
                str(env_file),
            ],
        )

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Unity gateway local setup" in out
        assert "SLACK_SIGNING_SECRET=" in out
        assert not env_file.exists()

    def test_setup_can_append_missing_env_placeholders(
        self,
        capsys: pytest.CaptureFixture,
        tmp_path,
    ) -> None:
        from unity.gateway import __main__ as launcher

        env_file = tmp_path / ".env"
        exit_code = launcher.main(
            [
                "setup",
                "--channels",
                "slack",
                "--write-env",
                "--env-file",
                str(env_file),
            ],
        )

        assert exit_code == 0
        assert "appended missing" in capsys.readouterr().out
        contents = env_file.read_text(encoding="utf-8")
        assert "SLACK_SIGNING_SECRET=" in contents
        assert "ORCHESTRA_ADMIN_KEY=" in contents

    def test_doctor_reports_missing_channel_credentials(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        monkeypatch.delenv("SLACK_SIGNING_SECRET", raising=False)
        monkeypatch.delenv("ORCHESTRA_ADMIN_KEY", raising=False)

        from unity.gateway import __main__ as launcher

        exit_code = launcher.main(
            ["doctor", "--channels", "slack", "--check-credentials"],
        )

        assert exit_code == 1
        out = capsys.readouterr().out
        assert "slack: missing required credentials" in out
        assert "SLACK_SIGNING_SECRET: missing (required)" in out

    def test_smoke_checks_gateway_health(
        self,
        capsys: pytest.CaptureFixture,
    ) -> None:
        from unity.gateway import __main__ as launcher

        class _Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return b'{"status":"ok"}'

        with patch.object(launcher, "urlopen", return_value=_Response()):
            exit_code = launcher.main(
                [
                    "smoke",
                    "--base-url",
                    "http://127.0.0.1:8001",
                    "--public-url",
                    "https://callbacks.example.com",
                ],
            )

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "health: ok (http://127.0.0.1:8001/health)" in out
        assert "public-url ok (https://callbacks.example.com)" in out
