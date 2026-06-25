"""
Shared command execution for the ConversationManager sandbox (REPL + GUI).

The parser (`commands.parse_command`) is intentionally UI-agnostic; this module
implements the shared execution semantics so both the REPL and Textual GUI route
commands identically.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

import unify

from sandboxes.conversation_manager.commands import (
    HELP_TEXT,
    ParsedCommand,
    parse_command,
)
from sandboxes.conversation_manager.event_publisher import EventPublisher
from sandboxes.conversation_manager.io_gate import gated_input
from sandboxes.conversation_manager.trace_display import TraceDisplay
from sandboxes.conversation_manager.event_tree_display import EventTreeDisplay
from sandboxes.conversation_manager.log_aggregator import LogAggregator
from sandboxes.conversation_manager.agent_service_bootstrap import (
    get_agent_service_log_path,
)

LG = logging.getLogger("conversation_manager_sandbox")

_LIVEKIT_SETUP_HINT = (
    "⚠️  Voice sessions require LiveKit. Run `unity voice` once to install the server,\n"
    "    then restart the sandbox — it will start automatically on next launch.\n"
    "  • Or use LiveKit Cloud: set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET\n"
    "    in ~/.unity/unity/.env  (sign up free at https://cloud.livekit.io)"
)

PromptFn = Callable[[str], Awaitable[str]]


@dataclass
class RouterResult:
    lines: list[str]
    should_exit: bool = False


@dataclass
class CommandRouter:
    """
    Executes a single input line against a running CM sandbox session.

    The router is stateful only via the provided `state` and `chat_history`
    references; it does not persist internal state.
    """

    cm: Any
    args: Any
    state: Any
    publisher: EventPublisher
    chat_history: list[dict]
    allow_save_project: bool = True
    trace_display: TraceDisplay | None = None
    event_tree_display: EventTreeDisplay | None = None
    log_aggregator: LogAggregator | None = None
    conversation_lines: list[str] = field(default_factory=list)
    pending_attachments: list[Path] = field(default_factory=list)

    async def execute_raw(
        self,
        raw: str,
        *,
        prompt_text: Optional[PromptFn] = None,
        in_call: Optional[bool] = None,
    ) -> RouterResult:
        """
        Execute a single raw line.

        prompt_text:
          Optional async callback used to request additional user input
          (e.g., missing scenario description) in REPL mode. GUI callers
          typically omit it.
        """
        st = self.state
        in_call_now = (
            bool(getattr(st, "in_call", False)) if in_call is None else bool(in_call)
        )

        try:
            h = getattr(self.cm, "active_ask_handle", None)
            active_now = h is not None and not h.done()
        except Exception:
            active_now = False
        cmd: ParsedCommand = parse_command(
            text=raw,
            in_call=in_call_now,
            active=active_now,
        )

        if cmd.kind not in {"unknown", "help"} and (raw or "").strip():
            try:
                self.chat_history.append({"role": "user", "content": raw.strip()})
            except Exception:
                pass

        # Unknown / error
        if cmd.kind == "unknown":
            if cmd.error and cmd.error != "empty":
                return RouterResult(lines=[cmd.error])
            return RouterResult(lines=[])

        # Meta
        if cmd.kind == "help":
            return RouterResult(lines=["\n" + HELP_TEXT + "\n"])
        if cmd.kind == "quit":
            return RouterResult(lines=["Exiting..."], should_exit=True)
        if cmd.kind == "reset":
            await self._reset_best_effort()
            # Reset display state.
            try:
                if self.trace_display is not None:
                    self.trace_display.reset_history()
            except Exception:
                pass
            try:
                if self.event_tree_display is not None:
                    self.event_tree_display.reset_tree()
            except Exception:
                pass
            try:
                if self.log_aggregator is not None:
                    self.log_aggregator.reset_expansion()
            except Exception:
                pass
            return RouterResult(lines=["✅ Reset complete."])
        if cmd.kind == "save_project":
            if not self.allow_save_project:
                return RouterResult(
                    lines=["⚠️ save_project is not available in this mode."],
                )
            try:
                commit_hash = unify.commit_project(
                    self.args.project_name,
                    commit_message=f"ConversationManager sandbox save {datetime.utcnow().isoformat()}",
                ).get("commit_hash")
                return RouterResult(lines=[f"💾 Project saved at commit {commit_hash}"])
            except Exception as exc:
                LG.error("save_project failed: %s", exc, exc_info=True)
                return RouterResult(lines=[f"❌ Failed to save project: {exc}"])

        # Display
        if cmd.kind == "trace":
            return await self._handle_trace_display(cmd.args)
        if cmd.kind == "tree":
            return await self._handle_tree_display()
        if cmd.kind == "show_logs":
            return await self._handle_log_expansion(cmd.args, expand=True)
        if cmd.kind == "collapse_logs":
            return await self._handle_log_expansion(cmd.args, expand=False)
        if cmd.kind == "agent_logs":
            return await self._handle_agent_logs(cmd.args)

        # File attachments
        if cmd.kind == "attach":
            return self._handle_attach(cmd.args)
        if cmd.kind == "detach":
            return self._handle_detach()

        if cmd.kind == "event":
            return await self._handle_event(cmd)
        return RouterResult(lines=[f"⚠️ Unhandled command kind: {cmd.kind}"])

    async def _handle_trace_display(self, args: str) -> RouterResult:
        td = self.trace_display
        cfg = getattr(self.args, "_actor_config", None)
        if td is None:
            return RouterResult(lines=["⚠️ Trace display is not initialized."])
        n = 3
        try:
            if (args or "").strip():
                n = int((args or "").strip())
        except Exception:
            n = 3
        return RouterResult(lines=[td.render_recent(n)])

    async def _handle_tree_display(self) -> RouterResult:
        tree = self.event_tree_display
        if tree is None:
            return RouterResult(lines=["⚠️ Event tree display is not initialized."])
        return RouterResult(lines=[tree.render_tree()])

    async def _handle_log_expansion(self, args: str, *, expand: bool) -> RouterResult:
        lg = self.log_aggregator
        if lg is None:
            return RouterResult(lines=["⚠️ Log aggregator is not initialized."])

        raw = (args or "").strip().lower()
        cats = []
        if raw in {"cm", "actor", "manager"}:
            cats = [raw]
        elif raw == "all":
            cats = ["cm", "actor", "manager"]
        else:
            return RouterResult(
                lines=[
                    "⚠️ Usage: show_logs <cm|actor|manager|all>  or  collapse_logs <cm|actor|manager|all>",
                ],
            )

        if expand:
            for c in cats:
                lg.expand(c)  # type: ignore[arg-type]
            blocks = []
            for c in cats:
                blocks.append(lg.render_expanded(c))  # type: ignore[arg-type]
            return RouterResult(lines=[("\n\n".join(blocks)).rstrip()])

        for c in cats:
            lg.collapse(c)  # type: ignore[arg-type]
        return RouterResult(lines=[lg.render_summary()])

    def _tail_text_file(self, path: Path, *, max_lines: int = 80) -> str:
        """
        Return the last N lines of a text file, best-effort.

        This is a sandbox UX helper; we keep it safe and bounded.
        """
        max_lines = int(max_lines)
        if max_lines <= 0:
            max_lines = 80
        if max_lines > 400:
            max_lines = 400
        try:
            if not path.exists():
                return ""
        except Exception:
            return ""
        try:
            # Bounded approach: keep only the last N lines without loading everything into memory.
            dq: deque[str] = deque(maxlen=max_lines)
            with path.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    dq.append(line.rstrip("\n"))
            return "\n".join(dq).rstrip()
        except Exception:
            return ""

    async def _handle_agent_logs(self, args: str) -> RouterResult:
        n = 80
        try:
            if (args or "").strip():
                n = int((args or "").strip())
        except Exception:
            n = 80

        # Determine repo root (same strategy as the sandbox entrypoint).
        repo_root = Path(__file__).resolve().parents[2]
        agent_server_url = getattr(
            self.args,
            "agent_server_url",
            "http://localhost:3000",
        )
        log_path = None
        try:
            lp = getattr(self.args, "_agent_service_log_path", None)
            if isinstance(lp, str) and lp.strip():
                log_path = Path(lp.strip())
        except Exception:
            log_path = None
        if log_path is None:
            log_path = get_agent_service_log_path(
                repo_root=repo_root,
                agent_server_url=str(agent_server_url),
            )

        tail = self._tail_text_file(log_path, max_lines=n)
        if not tail:
            return RouterResult(
                lines=[
                    f"agent-service log: {log_path}",
                    "⚠️ No log output yet (or file does not exist).",
                ],
            )
        return RouterResult(
            lines=[
                f"agent-service log: {log_path}",
                "",
                tail,
            ],
        )

    async def _reset_best_effort(self) -> None:
        st = self.state

        # Clean up live voice session if active.
        if getattr(st, "live_voice_active", False):
            try:
                await self.publisher.end_live_session()
            except Exception:
                pass

        try:
            st.reset_ephemeral()
        except Exception:
            pass

        # Clear CM state best-effort.
        cm = self.cm
        try:
            cm.contact_index.clear_conversations()
        except Exception:
            pass
        try:
            cm.notifications_bar.notifications = []
        except Exception:
            pass
        try:
            cm.in_flight_actions.clear()
        except Exception:
            pass
        try:
            cm.chat_history.clear()
        except Exception:
            pass
        try:
            from unity.conversation_manager.cm_types import Mode

            cm.mode = Mode.TEXT
        except Exception:
            pass
        try:
            cm.call_manager.call_contact = None
        except Exception:
            pass

    def _handle_attach(self, args: str) -> RouterResult:
        if not args:
            if not self.pending_attachments:
                return RouterResult(lines=["📎 No files attached."])
            names = ", ".join(p.name for p in self.pending_attachments)
            return RouterResult(
                lines=[f"📎 Pending ({len(self.pending_attachments)}): {names}"],
            )
        path = Path(args).expanduser().resolve()
        if not path.is_file():
            return RouterResult(lines=[f"⚠️ File not found: {path}"])
        self.pending_attachments.append(path)
        names = ", ".join(p.name for p in self.pending_attachments)
        return RouterResult(
            lines=[
                f"📎 Attached: {names} ({len(self.pending_attachments)} file(s) pending)",
            ],
        )

    def _handle_detach(self) -> RouterResult:
        count = len(self.pending_attachments)
        self.pending_attachments.clear()
        if count == 0:
            return RouterResult(lines=["📎 No files to clear."])
        return RouterResult(lines=[f"📎 Cleared {count} pending attachment(s)."])

    async def _handle_event(self, cmd: ParsedCommand) -> RouterResult:
        st = self.state
        st.last_event_published_at = asyncio.get_running_loop().time()

        if cmd.name == "message":
            attachments = (
                list(self.pending_attachments) if self.pending_attachments else None
            )
            await self.publisher.publish_unify_message(
                cmd.args,
                attachments=attachments,
            )
            if self.pending_attachments:
                self.pending_attachments.clear()
            return RouterResult(lines=[])
        if cmd.name == "sms":
            await self.publisher.publish_sms(cmd.args)
            return RouterResult(lines=[])
        if cmd.name == "meet":
            if getattr(st, "in_voice_session", False):
                return RouterResult(
                    lines=[
                        "⚠️ Already in a voice session. End it first with `end_meet`.",
                    ],
                )
            if not os.environ.get("LIVEKIT_URL"):
                return RouterResult(lines=[_LIVEKIT_SETUP_HINT])
            try:
                lines = await self.publisher.start_live_meet()
                return RouterResult(lines=lines)
            except Exception as exc:
                st.in_meet = False
                return RouterResult(
                    lines=[f"❌ Failed to start live voice session: {exc}"],
                )
        if cmd.name == "end_meet":
            if getattr(st, "live_voice_active", False):
                try:
                    lines = await self.publisher.end_live_session()
                    return RouterResult(lines=lines)
                except Exception as exc:
                    LG.error("end_live_session failed: %s", exc, exc_info=True)
                    st.in_meet = False
                    st.live_voice_session = None
                    return RouterResult(
                        lines=[f"❌ Error ending live voice session: {exc}"],
                    )
            await self.publisher.publish_meet_end()
            return RouterResult(lines=["🎥 Voice session ended."])

        return RouterResult(lines=[f"⚠️ Unknown event command: {cmd.name}"])


async def repl_prompt(prompt: str) -> str:
    """Default REPL prompt function (stdin-gated)."""
    return await asyncio.to_thread(gated_input, prompt)
