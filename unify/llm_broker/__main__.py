"""Entrypoint for ``python -m unify.llm_broker``.

Started as the sidecar container's command. Runs in its own container so the
provider credentials it reads are outside the runtime's process and namespace,
which is what stops user code from reading them.
"""

from __future__ import annotations

import logging

import uvicorn

from unify.llm_broker.app import build_app
from unify.llm_broker.settings import load_settings


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    uvicorn.run(
        build_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="warning",
        # Every request carries a model and an assistant; an access log would
        # restate that at volume without adding anything the ledger does not
        # already record.
        access_log=False,
    )


if __name__ == "__main__":
    main()
