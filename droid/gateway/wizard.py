"""Interactive local setup helpers for ``droid.gateway``."""

from __future__ import annotations

import getpass
import os
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, TextIO

from droid.gateway.local_setup import ChannelSetup, CredentialSpec, validate_public_url

InputFn = Callable[[str], str]
SecretFn = Callable[[str], str]


@dataclass(frozen=True)
class EnvFile:
    """Parsed env-file values with enough structure for safe local rewrites."""

    path: Path
    values: dict[str, str] = field(default_factory=dict)


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if len(value) >= 2 and (
        (value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")
    ):
        value = value[1:-1]
    return key, value


def read_env_file(path: str | Path) -> EnvFile:
    env_path = Path(path)
    values: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(line)
            if parsed is not None:
                key, value = parsed
                values[key] = value
    return EnvFile(path=env_path, values=values)


def load_env_file(path: str | Path, *, override: bool = False) -> EnvFile:
    env_file = read_env_file(path)
    for key, value in env_file.values.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return env_file


def _quote_env_value(value: str) -> str:
    if not value:
        return ""
    if any(char.isspace() or char in "#'\"" for char in value):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def write_env_values(path: str | Path, updates: dict[str, str]) -> None:
    env_path = Path(path)
    existing_lines = (
        env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    )
    if env_path.exists():
        backup = env_path.with_name(f"{env_path.name}.bak")
        shutil.copyfile(env_path, backup)

    remaining = dict(updates)
    rewritten: list[str] = []
    for line in existing_lines:
        parsed = _parse_env_line(line)
        if parsed is None:
            rewritten.append(line)
            continue
        key, _value = parsed
        if key in remaining:
            rewritten.append(f"{key}={_quote_env_value(remaining.pop(key))}")
        else:
            rewritten.append(line)

    if remaining:
        if rewritten and rewritten[-1].strip():
            rewritten.append("")
        rewritten.append("# Droid gateway local setup")
        for key in sorted(remaining):
            rewritten.append(f"{key}={_quote_env_value(remaining[key])}")

    env_path.write_text("\n".join(rewritten).rstrip() + "\n", encoding="utf-8")


def is_secret_name(name: str) -> bool:
    return any(
        marker in name for marker in ("SECRET", "TOKEN", "KEY", "PASSWORD", "AUTH")
    )


def mask_value(value: str) -> str:
    if not value:
        return "<missing>"
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:2]}{'*' * max(len(value) - 4, 3)}{value[-2:]}"


def _credential_value(name: str, env_values: dict[str, str]) -> str:
    return os.environ.get(name) or env_values.get(name, "")


def _prompt_value(
    spec: CredentialSpec,
    *,
    current: str,
    input_fn: InputFn,
    secret_fn: SecretFn,
    quick: bool,
) -> str | None:
    if current and quick:
        return None

    if current:
        prompt = f"{spec.name} [{mask_value(current)}] (Enter to keep): "
    elif spec.required:
        prompt = f"{spec.name} ({spec.description}): "
    else:
        prompt = f"{spec.name} optional ({spec.description}, Enter to skip): "

    value = secret_fn(prompt) if is_secret_name(spec.name) else input_fn(prompt)
    if value:
        return value.strip()
    if current:
        return None
    return "" if spec.required else None


def run_interactive_setup(
    setups: tuple[ChannelSetup, ...],
    *,
    env_file: str,
    public_url: str = "",
    quick: bool = False,
    write: bool = True,
    input_fn: InputFn | None = None,
    secret_fn: SecretFn | None = None,
    output: TextIO | None = None,
) -> int:
    stream = output or sys.stdout
    read_input = input_fn or input
    read_secret = secret_fn or getpass.getpass

    def emit(message: str = "") -> None:
        print(message, file=stream)

    env = read_env_file(env_file)
    updates: dict[str, str] = {}
    emit("Droid gateway local setup wizard")
    emit("")

    if any(setup.public_https_required for setup in setups):
        current_public_url = public_url or _credential_value(
            "DROID_GATEWAY_PUBLIC_URL",
            env.values,
        )
        if current_public_url:
            emit(f"Public callback URL: {current_public_url}")
        entered = read_input(
            (
                "Public HTTPS callback URL (Enter to keep/skip): "
                if current_public_url
                else "Public HTTPS callback URL (optional for local-only echo mode): "
            ),
        ).strip()
        selected_public_url = entered or current_public_url
        if selected_public_url:
            ok, message = validate_public_url(selected_public_url)
            emit(message)
            if not ok:
                return 1
            updates["DROID_GATEWAY_PUBLIC_URL"] = selected_public_url.rstrip("/")

    for setup in setups:
        emit("")
        emit(f"{setup.title} ({setup.name})")
        emit(setup.summary)
        if setup.signup_url:
            emit(f"Signup: {setup.signup_url}")
        if setup.dashboard_url:
            emit(f"Dashboard: {setup.dashboard_url}")
        for step in setup.setup_steps:
            emit(f"- {step}")

        for spec in setup.credentials:
            current = _credential_value(spec.name, env.values)
            prompted = _prompt_value(
                spec,
                current=current,
                input_fn=read_input,
                secret_fn=read_secret,
                quick=quick,
            )
            if prompted is None:
                continue
            if prompted:
                updates[spec.name] = prompted

    if write and updates:
        write_env_values(env_file, updates)
        emit("")
        emit(f"Wrote {len(updates)} value(s) to {env_file}")
    elif updates:
        emit("")
        emit("Collected values but did not write them because env writes are disabled.")
    else:
        emit("")
        emit("No env changes needed.")

    emit("")
    emit("Next steps:")
    emit("  python -m droid.gateway urls --public-url $DROID_GATEWAY_PUBLIC_URL")
    emit("  python -m droid.gateway doctor --check-credentials")
    emit("  python -m droid.gateway smoke --base-url http://127.0.0.1:8001")
    return 0


def report_env_status(
    setups: tuple[ChannelSetup, ...],
    *,
    env_values: dict[str, str],
) -> tuple[bool, list[str]]:
    failed = False
    lines: list[str] = []
    for setup in setups:
        missing: list[str] = []
        lines.append(f"{setup.name}: {setup.title}")
        for spec in setup.credentials:
            value = _credential_value(spec.name, env_values)
            marker = "set" if value else "missing"
            requirement = "required" if spec.required else "optional"
            lines.append(f"  {spec.name}: {marker} ({requirement})")
            if spec.required and not value:
                missing.append(spec.name)
        if missing:
            failed = True
            lines.append(f"  missing required: {', '.join(missing)}")
    return failed, lines


__all__ = [
    "EnvFile",
    "is_secret_name",
    "load_env_file",
    "mask_value",
    "read_env_file",
    "report_env_status",
    "run_interactive_setup",
    "write_env_values",
]
