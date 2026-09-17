"""Terminal chat with the local assistant.

``unify`` (or ``python -m unify``) starts the slow brain in-process, wires the
terminal to the in-app chat medium, and renders what the assistant sends back.
Every line typed is an inbound ``UnifyMessageReceived`` event; every reply is
the ``UnifyMessageSent`` event the brain publishes, so the terminal is one
front end over the same loop any other client would drive.

Runtime logs go to ``<UNIFY_HOME>/logs`` and stay off the terminal unless
``--debug`` is given.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv

MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
BOOT_TIMEOUT_SECONDS = 300.0

HELP = """\
Type a message and press Enter. The assistant keeps working on anything you
asked for while you keep typing; follow-up messages steer it.

  /attach <path>   attach a file to your next message
  /attach          list queued attachments
  /detach          clear queued attachments
  /help            show this help
  /quit            exit (Ctrl-D and Ctrl-C work too)
"""


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="unify",
        description="Chat with the local assistant.",
    )
    parser.add_argument(
        "--home",
        metavar="DIR",
        help="where the store, embeddings cache, workspace and logs live "
        "(default: UNIFY_HOME or ~/.unify)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="stream runtime logs to the terminal as well as the log files",
    )
    return parser.parse_args(argv)


def _configure_environment(args: argparse.Namespace) -> Path:
    """Point the runtime at its home directory and route logs there."""
    load_dotenv()
    if args.home:
        os.environ["UNIFY_HOME"] = str(Path(args.home).expanduser())
    home = Path(os.environ.get("UNIFY_HOME", "").strip() or "~/.unify").expanduser()
    home.mkdir(parents=True, exist_ok=True)

    from unify.logger import LOGGER, configure_log_dir

    configure_log_dir(os.environ.get("UNIFY_LOG_DIR", "").strip() or str(home / "logs"))
    if not args.debug:
        for handler in list(LOGGER.handlers):
            if getattr(handler, "_unity_terminal", False):
                LOGGER.removeHandler(handler)
    return home


def _stage_attachment(source: Path) -> str:
    """Copy a local file into the workspace and return its workspace path."""
    from unify.workspace import get_local_root

    attachments_dir = Path(get_local_root()) / "Attachments"
    attachments_dir.mkdir(parents=True, exist_ok=True)
    target_name = f"{uuid.uuid4().hex[:8]}_{source.name}"
    shutil.copy2(source, attachments_dir / target_name)
    return f"Attachments/{target_name}"


def _assistant_name() -> str:
    from unify.session_details import PLACEHOLDER_ASSISTANT_FIRST_NAME, SESSION_DETAILS

    return SESSION_DETAILS.assistant.first_name or PLACEHOLDER_ASSISTANT_FIRST_NAME


class Chat:
    """One terminal session over a running ConversationManager."""

    def __init__(self) -> None:
        self._cm = None
        self._ready = asyncio.Event()
        self._closing = asyncio.Event()
        self._pending_attachments: list[Path] = []

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        from unify import db
        from unify.conversation_manager.main import run_conversation_manager
        from unify.session_details import SESSION_DETAILS

        SESSION_DETAILS.populate_from_env()
        db.activate(db.DEFAULT_PROJECT)
        self._seed_builtins()

        self._cm = await run_conversation_manager(project_name=db.DEFAULT_PROJECT)
        self._listener = asyncio.create_task(self._listen())

    @staticmethod
    def _seed_builtins() -> None:
        """Make the primitive and guidance catalogues available to the actor."""
        from unify.function_manager.builtins_catalog import seed_builtin_primitives
        from unify.guidance_manager.builtins_catalog import seed_builtin_guidance

        seed_builtin_primitives()
        seed_builtin_guidance()

    async def close(self) -> None:
        from unify.events.event_bus import EVENT_BUS

        self._closing.set()
        if self._cm is not None:
            self._cm.stop.set()
            try:
                await asyncio.wait_for(self._cm.cleanup(), timeout=15.0)
            except asyncio.TimeoutError:
                pass
        self._listener.cancel()
        if EVENT_BUS:
            EVENT_BUS.flush()

    # ── outbound ─────────────────────────────────────────────────────────

    async def _listen(self) -> None:
        from unify.conversation_manager.events import (
            ActorClarificationRequest,
            ActorNotification,
            ActorResult,
            DirectMessageEvent,
            Error,
            Event,
            InitializationComplete,
            UnifyMessageSent,
        )

        async with self._cm.event_broker.pubsub() as pubsub:
            await pubsub.psubscribe("app:comms:*", "app:actor:*")
            while not self._closing.is_set():
                msg = await pubsub.get_message(
                    timeout=1.0,
                    ignore_subscribe_messages=True,
                )
                if not msg:
                    continue
                event = Event.from_json(msg["data"])
                if isinstance(event, InitializationComplete):
                    self._ready.set()
                elif isinstance(event, (UnifyMessageSent, DirectMessageEvent)):
                    self._say(_assistant_name(), event.content)
                    for attachment in getattr(event, "attachments", []):
                        self._status(f"attached {attachment}")
                elif isinstance(event, ActorNotification):
                    prefix = "done" if event.completed else "working"
                    self._status(f"{prefix}: {event.response}")
                elif isinstance(event, ActorResult):
                    if not event.success:
                        self._status(f"action failed: {event.error}")
                elif isinstance(event, ActorClarificationRequest):
                    self._status(f"the assistant is asking: {event.query}")
                elif isinstance(event, Error):
                    self._status(f"error: {event.message}")

    def _say(self, who: str, text: str) -> None:
        print(f"\n{who}> {text}\n", flush=True)

    def _status(self, text: str) -> None:
        print(f"  · {text}", flush=True)

    # ── inbound ──────────────────────────────────────────────────────────

    async def send(self, text: str) -> None:
        from unify.conversation_manager.events import UnifyMessageReceived

        attachments = [_stage_attachment(p) for p in self._pending_attachments]
        self._pending_attachments.clear()
        event = UnifyMessageReceived(content=text, attachments=attachments)
        await self._cm.event_broker.publish(UnifyMessageReceived.topic, event.to_json())

    def attach(self, raw_path: str) -> str:
        path = Path(raw_path).expanduser()
        if not path.is_file():
            return f"no such file: {raw_path}"
        if path.stat().st_size > MAX_ATTACHMENT_BYTES:
            return f"too large to attach (limit 25MB): {raw_path}"
        self._pending_attachments.append(path)
        return f"queued {path.name} for your next message"

    def queued_attachments(self) -> str:
        if not self._pending_attachments:
            return "no attachments queued"
        return "queued: " + ", ".join(p.name for p in self._pending_attachments)

    def detach(self) -> str:
        count = len(self._pending_attachments)
        self._pending_attachments.clear()
        return f"cleared {count} queued attachment(s)"

    # ── loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        print("starting the assistant ...", flush=True)
        await self.start()
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=BOOT_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            print("the assistant did not finish starting; check the logs", flush=True)
            return
        print("ready. /help for commands, /quit to exit.\n", flush=True)
        while True:
            try:
                raw = await asyncio.to_thread(input, "> ")
            except (EOFError, KeyboardInterrupt):
                print()
                return
            line = raw.strip()
            if not line:
                continue
            if line.startswith("/"):
                if await self._command(line):
                    return
                continue
            await self.send(line)

    async def _command(self, line: str) -> bool:
        """Run a slash command; return True when the session should end."""
        name, _, arg = line[1:].partition(" ")
        name = name.lower()
        arg = arg.strip()
        if name in {"quit", "exit", "q"}:
            return True
        if name in {"help", "h", "?"}:
            print(HELP)
        elif name == "attach":
            print(self.attach(arg) if arg else self.queued_attachments())
        elif name == "detach":
            print(self.detach())
        else:
            print(f"unknown command: /{name} (try /help)")
        return False


async def _run(args: argparse.Namespace) -> int:
    chat = Chat()
    try:
        await chat.run()
    finally:
        await chat.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_environment(args)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
