from __future__ import annotations

from io import StringIO

from unity.gateway.local_setup import select_channel_setups
from unity.gateway.wizard import (
    load_env_file,
    mask_value,
    read_env_file,
    report_env_status,
    run_interactive_setup,
    write_env_values,
)


def test_env_file_round_trip_preserves_comments_and_updates_values(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("# Existing\nTWILIO_ACCOUNT_SID=old\n", encoding="utf-8")

    write_env_values(
        env_file,
        {
            "TWILIO_ACCOUNT_SID": "new",
            "TWILIO_AUTH_TOKEN": "secret token",
        },
    )

    contents = env_file.read_text(encoding="utf-8")
    assert "# Existing" in contents
    assert "TWILIO_ACCOUNT_SID=new" in contents
    assert 'TWILIO_AUTH_TOKEN="secret token"' in contents
    assert (tmp_path / ".env.bak").exists()


def test_load_env_file_sets_missing_environment(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SLACK_SIGNING_SECRET=x\n", encoding="utf-8")
    monkeypatch.delenv("SLACK_SIGNING_SECRET", raising=False)

    parsed = load_env_file(env_file)

    assert parsed.values["SLACK_SIGNING_SECRET"] == "x"
    assert read_env_file(env_file).values["SLACK_SIGNING_SECRET"] == "x"
    assert mask_value("abcdefghi") == "ab*****hi"
    assert "SLACK_SIGNING_SECRET" in parsed.values


def test_report_env_status_uses_env_file_values(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SLACK_SIGNING_SECRET=x\nORCHESTRA_ADMIN_KEY=y\n",
        encoding="utf-8",
    )
    setups = select_channel_setups(["slack"])

    failed, lines = report_env_status(
        setups,
        env_values=read_env_file(env_file).values,
    )

    assert failed is False
    assert "  SLACK_SIGNING_SECRET: set (required)" in lines
    assert "  ORCHESTRA_ADMIN_KEY: set (required)" in lines


def test_interactive_setup_writes_selected_values(tmp_path) -> None:
    env_file = tmp_path / ".env"
    answers = iter(["https://callbacks.example.com", "secret", "admin"])
    output = StringIO()

    exit_code = run_interactive_setup(
        select_channel_setups(["slack"]),
        env_file=str(env_file),
        input_fn=lambda _prompt: next(answers),
        secret_fn=lambda _prompt: next(answers),
        output=output,
    )

    assert exit_code == 0
    contents = env_file.read_text(encoding="utf-8")
    assert "UNITY_GATEWAY_PUBLIC_URL=https://callbacks.example.com" in contents
    assert "SLACK_SIGNING_SECRET=secret" in contents
    assert "ORCHESTRA_ADMIN_KEY=admin" in contents
    assert "Unity gateway local setup wizard" in output.getvalue()
