"""Entrypoint for ``python -m unity.gateway``.

Launches the gateway FastAPI app under uvicorn. Configuration is
keyword-light by design -- override host / port / log level / reload
via flags, and use environment variables (UNITY_GATEWAY_HOST,
UNITY_GATEWAY_PORT, UNITY_GATEWAY_LOG_LEVEL, UNITY_GATEWAY_RELOAD)
when running in a container where flags are awkward.

Examples::

    python -m unity.gateway
    python -m unity.gateway --port 9000
    python -m unity.gateway --host 0.0.0.0 --reload
    UNITY_GATEWAY_PORT=9000 python -m unity.gateway

Production deployments typically run ``uvicorn unity.gateway.app:app``
directly so they can pin the worker count and reload disable, but
this entrypoint exists for the local-dev / open-source path.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m unity.gateway",
        description="Run the unity.gateway FastAPI server.",
    )
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
    return parser


def main(argv: list[str] | None = None) -> int:
    """Console entrypoint. Returns a process exit code."""
    args = _build_parser().parse_args(argv)

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


if __name__ == "__main__":
    sys.exit(main())
