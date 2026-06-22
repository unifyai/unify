"""
Command parsing for the ConversationManager sandbox REPL.

This module is UI-agnostic: it only classifies a single input line into a
`ParsedCommand`. Execution is implemented in `command_router.py` so both REPL
and GUI behave identically.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

# ──────────────────────────────────────────────────────────────────────────────
# Parsed command model
# ──────────────────────────────────────────────────────────────────────────────

CommandKind = Literal[
    # Meta
    "help",
    "quit",
    "reset",
    "save_project",
    # Display (REPL/GUI surfaces)
    "trace",
    "tree",
    "show_logs",
    "collapse_logs",
    "agent_logs",
    # Steering
    "steering",
    # File attachments
    "attach",
    "detach",
    # Inbound events and voice sessions
    "event",
    # Unknown / invalid
    "unknown",
]


@dataclass(frozen=True)
class ParsedCommand:
    kind: CommandKind
    raw: str
    name: str
    args: str = ""
    error: Optional[str] = None


HELP_TEXT = """
ConversationManager Sandbox (REPL)
--------------------------------
Type a command at the prompt. <arg> = required, [arg] = optional.

Meta commands:
  help | h            Show this help
  quit | exit         Exit the sandbox
  reset               Reset sandbox session state
  save_project | sp   Save a Unify project snapshot

Display commands:
  trace [N]           Show recent CodeAct execution trace (default: 3)
  tree                Show current manager call event tree
  show_logs <cat>     Expand logs for category: cm | actor | manager | all
  collapse_logs <cat> Collapse logs for category: cm | actor | manager | all
  agent_logs [N]      Show last N lines of sandbox-started agent-service logs (default: 80)

Inbound events:
  msg <content>       Send a test Unify chat message to the assistant
  sms <content>       Send a test inbound SMS to the assistant
  meet                Start a LiveKit voice session (opens browser playground)
  end_meet            End the active LiveKit voice session

File attachments:
  attach <path>       Queue a local file for the next `msg` command
  attach              Show currently queued attachments
  detach              Clear all queued attachments
""".strip(
    "\n",
)


def parse_command(*, text: str, in_call: bool, active: bool) -> ParsedCommand:
    """
    Parse a user input line into a structured command.

    Precedence:
    1) Meta commands
    2) Display commands
    3) File attachment commands
    4) Inbound event commands
    """
    raw = (text or "").rstrip("\n")
    trimmed = raw.strip()
    lower = trimmed.lower()

    if not trimmed:
        return ParsedCommand(kind="unknown", raw=raw, name="empty", error="empty")

    # 1) Meta commands (highest priority)
    if lower in {"help", "h", "?"}:
        return ParsedCommand(kind="help", raw=raw, name="help")
    if lower in {"quit", "exit"}:
        return ParsedCommand(kind="quit", raw=raw, name="quit")
    if lower == "reset":
        return ParsedCommand(kind="reset", raw=raw, name="reset")
    if lower in {"save_project", "sp"}:
        return ParsedCommand(kind="save_project", raw=raw, name="save_project")
    if lower == "tree" or lower == "show_tree":
        return ParsedCommand(kind="tree", raw=raw, name="tree")
    if lower == "trace" or lower == "show_trace":
        return ParsedCommand(kind="trace", raw=raw, name="trace", args="")
    if lower.startswith("trace "):
        return ParsedCommand(
            kind="trace",
            raw=raw,
            name="trace",
            args=trimmed[6:].strip(),
        )
    if lower.startswith("show_logs "):
        return ParsedCommand(
            kind="show_logs",
            raw=raw,
            name="show_logs",
            args=trimmed[len("show_logs ") :].strip(),
        )
    if lower.startswith("collapse_logs "):
        return ParsedCommand(
            kind="collapse_logs",
            raw=raw,
            name="collapse_logs",
            args=trimmed[len("collapse_logs ") :].strip(),
        )
    if lower == "agent_logs":
        return ParsedCommand(kind="agent_logs", raw=raw, name="agent_logs", args="")
    if lower.startswith("agent_logs "):
        return ParsedCommand(
            kind="agent_logs",
            raw=raw,
            name="agent_logs",
            args=trimmed[len("agent_logs ") :].strip(),
        )

    # 2) File attachment commands
    if lower == "attach":
        return ParsedCommand(kind="attach", raw=raw, name="attach", args="")
    if lower.startswith("attach "):
        return ParsedCommand(
            kind="attach",
            raw=raw,
            name="attach",
            args=trimmed[len("attach ") :].strip(),
        )
    if lower == "detach":
        return ParsedCommand(kind="detach", raw=raw, name="detach")

    # 3) Event commands
    if lower.startswith("msg "):
        return ParsedCommand(
            kind="event",
            raw=raw,
            name="message",
            args=trimmed[4:].strip(),
        )
    if lower.startswith("sms "):
        return ParsedCommand(
            kind="event",
            raw=raw,
            name="sms",
            args=trimmed[4:].strip(),
        )
    if lower == "meet":
        return ParsedCommand(kind="event", raw=raw, name="meet")
    if lower == "end_meet":
        return ParsedCommand(kind="event", raw=raw, name="end_meet")

    return ParsedCommand(
        kind="unknown",
        raw=raw,
        name="unknown",
        error=f"⚠️ Unknown command: {trimmed}. Type 'help' for available commands.",
    )
