"""Entrypoint for ``python -m unity.gateway``."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from unity.gateway.local_setup import (
    callback_urls,
    channel_names,
    env_placeholder_lines,
    missing_required_credentials,
    public_url_provider_from_base,
    select_channel_setups,
    validate_public_url,
)
from unity.gateway.wizard import (
    load_env_file,
    report_env_status,
    run_interactive_setup,
)

GATEWAY_MODES = ("all", "channels", "adapters", "local-single-process")


def _add_serve_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--host",
        default=os.environ.get("UNITY_GATEWAY_HOST", "0.0.0.0"),
        help="Bind host (default: 0.0.0.0, env: UNITY_GATEWAY_HOST)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("UNITY_GATEWAY_PORT", "8080")),
        help="Bind port (default: 8080, env: UNITY_GATEWAY_PORT)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("UNITY_GATEWAY_LOG_LEVEL", "info"),
        help="Uvicorn log level (default: info, env: UNITY_GATEWAY_LOG_LEVEL)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=os.environ.get("UNITY_GATEWAY_RELOAD", "").lower()
        in ("1", "true", "yes"),
        help=(
            "Enable uvicorn auto-reload (default: off, "
            "env: UNITY_GATEWAY_RELOAD=true)"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=GATEWAY_MODES,
        default=os.environ.get("UNITY_GATEWAY_MODE", "all"),
        help="Gateway route set to serve (default: all)",
    )
    parser.add_argument(
        "--public-url",
        default=os.environ.get("UNITY_GATEWAY_PUBLIC_URL", ""),
        help="Externally reachable HTTPS callback base URL.",
    )
    parser.add_argument(
        "--single-url",
        action="store_true",
        default=os.environ.get("UNITY_GATEWAY_SINGLE_URL", "").lower()
        in ("1", "true", "yes"),
        help="Point UNITY_COMMS_URL and UNITY_ADAPTERS_URL at this gateway.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m unity.gateway",
        description="Run or inspect the unity.gateway FastAPI server.",
    )
    subparsers = parser.add_subparsers(dest="command")

    serve = subparsers.add_parser("serve", help="Run the gateway server")
    _add_serve_args(serve)

    doctor = subparsers.add_parser("doctor", help="Validate gateway configuration")
    doctor.add_argument(
        "--public-url",
        default=os.environ.get("UNITY_GATEWAY_PUBLIC_URL", ""),
        help="Externally reachable HTTPS callback base URL.",
    )
    doctor.add_argument(
        "--check-credentials",
        action="store_true",
        help="Check common channel credential environment variables.",
    )
    doctor.add_argument(
        "--channels",
        nargs="*",
        default=None,
        help=f"Channels to inspect (default: all). Known: all, {', '.join(channel_names())}",
    )
    doctor.add_argument(
        "--env-file",
        default=".env",
        help="Env file to load before checks (default: .env).",
    )
    doctor.add_argument(
        "--fix",
        action="store_true",
        help="Append missing credential placeholders to --env-file.",
    )

    urls = subparsers.add_parser("urls", help="Print provider callback URLs")
    urls.add_argument(
        "--public-url",
        default=os.environ.get("UNITY_GATEWAY_PUBLIC_URL", ""),
        help="Externally reachable HTTPS callback base URL.",
    )
    urls.add_argument(
        "--channels",
        nargs="*",
        default=None,
        help=f"Channels to print (default: all). Known: all, {', '.join(channel_names())}",
    )
    urls.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    urls.add_argument(
        "--single-url",
        action="store_true",
        default=True,
        help="Use --public-url for both comms and adapter callback surfaces.",
    )

    setup = subparsers.add_parser("setup", help="Print local channel setup guidance")
    setup.add_argument(
        "--public-url",
        default=os.environ.get("UNITY_GATEWAY_PUBLIC_URL", ""),
        help="Externally reachable HTTPS callback base URL.",
    )
    setup.add_argument(
        "--channels",
        nargs="*",
        default=None,
        help=f"Channels to include (default: all). Known: all, {', '.join(channel_names())}",
    )
    setup.add_argument(
        "--env-file",
        default=".env",
        help="Env file path for placeholder output (default: .env).",
    )
    setup.add_argument(
        "--write-env",
        action="store_true",
        help="Append missing channel credential placeholders to --env-file.",
    )
    setup.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for selected channel values and write them to --env-file.",
    )
    setup.add_argument(
        "--quick",
        action="store_true",
        help="In interactive mode, prompt only for missing values.",
    )
    setup.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting when selected required values are missing.",
    )
    setup.add_argument(
        "--no-write-env",
        action="store_true",
        help="In interactive mode, print collected values summary without writing.",
    )
    setup.add_argument(
        "--print",
        action="store_true",
        help="Print setup guidance without modifying files. This is the default unless --write-env is set.",
    )

    smoke = subparsers.add_parser("smoke", help="Run local gateway smoke checks")
    _add_serve_args(smoke)
    smoke.add_argument(
        "--base-url",
        default=os.environ.get("UNITY_GATEWAY_HEALTH_URL", ""),
        help="Gateway base URL for /health (default: host/port flags).",
    )
    smoke.add_argument(
        "--channels",
        nargs="*",
        default=None,
        help=f"Channels to inspect (default: all). Known: all, {', '.join(channel_names())}",
    )
    smoke.add_argument(
        "--check-credentials",
        action="store_true",
        help="Fail when selected channels are missing required credentials.",
    )
    smoke.add_argument(
        "--env-file",
        default=".env",
        help="Env file to load before checks (default: .env).",
    )

    wizard = subparsers.add_parser("wizard", help="Run the interactive setup wizard")
    wizard.add_argument(
        "--public-url",
        default=os.environ.get("UNITY_GATEWAY_PUBLIC_URL", ""),
        help="Externally reachable HTTPS callback base URL.",
    )
    wizard.add_argument(
        "--channels",
        nargs="*",
        default=None,
        help=f"Channels to include (default: all). Known: all, {', '.join(channel_names())}",
    )
    wizard.add_argument(
        "--env-file",
        default=".env",
        help="Env file path to update (default: .env).",
    )
    wizard.add_argument(
        "--quick",
        action="store_true",
        help="Prompt only for missing values.",
    )
    wizard.add_argument(
        "--no-write-env",
        action="store_true",
        help="Do not write collected values to --env-file.",
    )

    _add_serve_args(parser)
    return parser


def _normalize_argv(argv: list[str] | None) -> list[str] | None:
    commands = {"serve", "doctor", "urls", "setup", "smoke", "wizard", "-h", "--help"}
    if argv and argv[0] not in commands:
        return ["serve", *argv]
    return argv


def _apply_url_env(args: argparse.Namespace) -> None:
    if getattr(args, "public_url", ""):
        public_url = args.public_url.rstrip("/")
        os.environ["UNITY_GATEWAY_PUBLIC_URL"] = public_url
        os.environ.setdefault("UNITY_ADAPTERS_URL", public_url)
        os.environ.setdefault("UNITY_COMMS_URL", public_url)
    if getattr(args, "single_url", False):
        base_url = getattr(args, "public_url", "").rstrip("/")
        if not base_url:
            base_url = f"http://{args.host}:{args.port}"
        os.environ["UNITY_COMMS_URL"] = base_url
        os.environ["UNITY_ADAPTERS_URL"] = base_url
    os.environ["UNITY_GATEWAY_MODE"] = getattr(args, "mode", "all")


def _serve(args: argparse.Namespace) -> int:
    _apply_url_env(args)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Import uvicorn lazily so `python -m unity.gateway --help` doesn't pay
    # the uvicorn import cost.
    import uvicorn

    uvicorn.run(
        "unity.gateway.app:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        reload=args.reload,
    )
    return 0


def _selected_setups(args: argparse.Namespace):
    try:
        return select_channel_setups(getattr(args, "channels", None))
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def _print_channel_urls(args: argparse.Namespace) -> int:
    setups = _selected_setups(args)
    public_url = args.public_url.strip()
    if not public_url:
        print("public-url: not set")
        return 1
    provider = public_url_provider_from_base(
        public_url,
        single_url=getattr(args, "single_url", True),
    )
    if args.format == "json":
        payload = {
            setup.name: [
                {
                    "name": callback.name,
                    "surface": callback.surface,
                    "path": callback.path,
                    "url": url,
                    "description": callback.description,
                }
                for callback, url in callback_urls(setup, provider)
            ]
            for setup in setups
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    for setup in setups:
        print(f"{setup.title} ({setup.name})")
        urls = callback_urls(setup, provider)
        if not urls:
            print("  no provider callback URLs")
            continue
        for callback, url in urls:
            print(f"  {callback.name}: {url}")
    return 0


def _append_env_placeholders(env_file: str, lines: tuple[str, ...]) -> None:
    path = Path(env_file)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing_names = {
        line.split("=", 1)[0].strip()
        for line in existing.splitlines()
        if line.strip() and not line.lstrip().startswith("#") and "=" in line
    }
    missing_lines: list[str] = []
    for line in lines:
        if not line or line.lstrip().startswith("#"):
            missing_lines.append(line)
            continue
        name = line.split("=", 1)[0]
        if name not in existing_names:
            missing_lines.append(line)
    if not missing_lines:
        print(f"{env_file}: already has selected credential placeholders")
        return
    prefix = "\n" if existing and not existing.endswith("\n") else ""
    with path.open("a", encoding="utf-8") as handle:
        handle.write(prefix)
        handle.write("# Unity gateway local channel credentials\n")
        handle.write("\n".join(missing_lines).rstrip())
        handle.write("\n")
    print(f"{env_file}: appended missing selected credential placeholders")


def _setup(args: argparse.Namespace) -> int:
    setups = _selected_setups(args)
    if getattr(args, "interactive", False):
        if getattr(args, "non_interactive", False):
            print("setup: --interactive and --non-interactive cannot be combined")
            return 2
        return run_interactive_setup(
            setups,
            env_file=args.env_file,
            public_url=args.public_url.strip(),
            quick=getattr(args, "quick", False),
            write=not getattr(args, "no_write_env", False),
        )

    env_file = load_env_file(args.env_file, override=False)
    print("Unity gateway local setup")
    print("")
    print("Start the gateway with:")
    print(
        "  python -m unity.gateway serve --port 8001 --single-url --public-url https://your-public-url.example",
    )
    print("")
    ok, message = validate_public_url(args.public_url.strip())
    print(message)
    if args.public_url.strip():
        _print_channel_urls(
            argparse.Namespace(
                public_url=args.public_url,
                channels=[setup.name for setup in setups],
                format="text",
            ),
        )
    print("")
    print("Credential placeholders:")
    for line in env_placeholder_lines(setups):
        print(line)
    if getattr(args, "non_interactive", False):
        failed, lines = report_env_status(setups, env_values=env_file.values)
        print("")
        print("Credential status:")
        for line in lines:
            print(line)
        if failed:
            print("")
            print("setup: missing required credentials")
            return 1
    if args.write_env:
        _append_env_placeholders(args.env_file, env_placeholder_lines(setups))
    print(
        "Use your preferred tunnel provider for the public HTTPS URL; Unity does not run a tunnel service.",
    )
    return 0


def _doctor(args: argparse.Namespace) -> int:
    setups = _selected_setups(args)
    env_file = load_env_file(args.env_file, override=False)
    failed = False
    public_url = args.public_url.strip()
    print("Unity gateway doctor")
    print("")
    print("Public URL")
    print("----------")
    if public_url:
        ok, message = validate_public_url(public_url)
        print(message)
        failed = failed or not ok
    else:
        print("public-url: not set")

    print("")
    print("Local Runtime")
    print("-------------")
    ingress_url = os.environ.get("UNITY_GATEWAY_LOCAL_INGRESS_URL", "").strip()
    if ingress_url:
        if ingress_url.startswith(("http://", "https://")):
            print(f"local ingress: configured ({ingress_url})")
        else:
            print(f"local ingress: invalid URL ({ingress_url})")
            failed = True
    else:
        print("local ingress: not set (ok for adapter-only checks)")

    if args.check_credentials:
        print("")
        print("Credentials")
        print("-----------")
        creds_failed, lines = report_env_status(setups, env_values=env_file.values)
        for line in lines:
            print(line)
        failed = failed or creds_failed

    if args.fix:
        print("")
        print("Safe fixes")
        print("----------")
        _append_env_placeholders(args.env_file, env_placeholder_lines(setups))
    return 1 if failed else 0


def _smoke(args: argparse.Namespace) -> int:
    load_env_file(args.env_file, override=False)
    failed = False
    base_url = args.base_url.strip() or f"http://{args.host}:{args.port}"
    health_url = f"{base_url.rstrip('/')}/health"
    try:
        with urlopen(health_url, timeout=5) as response:
            body = response.read().decode("utf-8")
            if response.status == 200:
                print(f"health: ok ({health_url})")
            else:
                print(f"health: unexpected status {response.status} ({body})")
                failed = True
    except (OSError, URLError) as exc:
        print(f"health: failed ({health_url}): {exc}")
        failed = True

    if args.public_url.strip():
        ok, message = validate_public_url(args.public_url.strip())
        print(message)
        failed = failed or not ok
    else:
        print("public-url: not set")

    if args.check_credentials:
        for setup in _selected_setups(args):
            missing = missing_required_credentials(setup)
            print(
                f"{setup.name}: {'credentials ok' if not missing else 'missing ' + ', '.join(missing)}",
            )
            failed = failed or bool(missing)
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    """Console entrypoint. Returns a process exit code."""
    args = _build_parser().parse_args(_normalize_argv(argv))
    command = args.command or "serve"
    if command == "doctor":
        return _doctor(args)
    if command == "urls":
        return _print_channel_urls(args)
    if command == "setup":
        return _setup(args)
    if command == "smoke":
        return _smoke(args)
    if command == "wizard":
        setups = _selected_setups(args)
        return run_interactive_setup(
            setups,
            env_file=args.env_file,
            public_url=args.public_url.strip(),
            quick=getattr(args, "quick", False),
            write=not getattr(args, "no_write_env", False),
        )
    return _serve(args)


if __name__ == "__main__":
    sys.exit(main())
