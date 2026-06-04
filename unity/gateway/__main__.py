"""Entrypoint for ``python -m unity.gateway``."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from urllib.parse import urlparse

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

    _add_serve_args(parser)
    return parser


def _normalize_argv(argv: list[str] | None) -> list[str] | None:
    if argv and argv[0] not in {"serve", "doctor", "-h", "--help"}:
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


def _credential_status() -> dict[str, bool]:
    names = [
        "ORCHESTRA_ADMIN_KEY",
        "TWILIO_ACCOUNT_SID",
        "TWILIO_AUTH_TOKEN",
        "TWILIO_WA_ACCOUNT_SID",
        "TWILIO_WA_AUTH_TOKEN",
        "SLACK_SIGNING_SECRET",
        "GOOGLE_OAUTH_CLIENT_ID",
        "GOOGLE_OAUTH_CLIENT_SECRET",
        "MS365_BYOD_CLIENT_ID",
        "MS365_BYOD_CLIENT_SECRET",
        "OAUTH_STATE_SIGNING_KEY",
        "OUTLOOK_WEBHOOK_SECRET",
        "TEAMS_WEBHOOK_SECRET",
    ]
    return {name: bool(os.environ.get(name, "").strip()) for name in names}


def _doctor(args: argparse.Namespace) -> int:
    failed = False
    public_url = args.public_url.strip()
    if public_url:
        parsed = urlparse(public_url)
        if parsed.scheme != "https":
            print("public-url: must use https for real provider callbacks")
            failed = True
        elif not parsed.netloc:
            print("public-url: missing host")
            failed = True
        else:
            print(f"public-url: ok ({public_url.rstrip('/')})")
    else:
        print("public-url: not set")

    if args.check_credentials:
        for name, present in _credential_status().items():
            print(f"{name}: {'set' if present else 'missing'}")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    """Console entrypoint. Returns a process exit code."""
    args = _build_parser().parse_args(_normalize_argv(argv))
    command = args.command or "serve"
    if command == "doctor":
        return _doctor(args)
    return _serve(args)


if __name__ == "__main__":
    sys.exit(main())
