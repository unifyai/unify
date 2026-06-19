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
    "save_state",
    # Configuration + display (REPL/GUI surfaces)
    "config",
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
    # Event simulation (inbound)
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
  save_state [path]   Save structured state snapshot (logs, tree, traces) to JSON file

Configuration:
  config              Switch actor configuration (restarts sandbox; state is reset)

Display commands:
  trace [N]           Show recent CodeAct execution trace (default: 3)
  tree                Show current manager call event tree
  show_logs <cat>     Expand logs for category: cm | actor | manager | all
  collapse_logs <cat> Collapse logs for category: cm | actor | manager | all
  agent_logs [N]      Show last N lines of sandbox-started agent-service logs (default: 80)

Inbound event simulation:
  msg <content>                 Simulate incoming Unify message
  sms <content>                 Simulate incoming SMS
  email <subject> | <body>      Simulate incoming email
  call                          Start a LiveKit voice call (opens browser)
  meet                          Start a LiveKit Unify Meet session (opens browser)
  end_call                      End active phone call
  end_meet                      End active Unify Meet session

File attachments:
  attach <path>                 Queue a local file for the next `msg` command
  attach                        Show currently queued attachments
  detach                        Clear all queued attachments

Meet interaction events (requires active meet):
  assistant_screen_share_start [reason]    User enables viewing the assistant's desktop
  assistant_screen_share_stop [reason]     User disables viewing the assistant's desktop
  user_screen_share_start [reason]         User starts sharing their screen with the assistant
  user_screen_share_stop [reason]          User stops sharing their screen
  user_webcam_start [reason]               User enables their webcam
  user_webcam_stop [reason]                User disables their webcam
  user_remote_control_start [reason]       User takes remote control of the assistant's desktop
  user_remote_control_stop [reason]        User releases remote control
""".strip(
    "\n",
)


_MEET_INTERACTION_COMMANDS = (
    "assistant_screen_share_start",
    "assistant_screen_share_stop",
    "user_screen_share_start",
    "user_screen_share_stop",
    "user_webcam_start",
    "user_webcam_stop",
    "user_remote_control_start",
    "user_remote_control_stop",
)


def parse_command(*, text: str, in_call: bool, active: bool) -> ParsedCommand:
    """
    Parse a user input line into a structured command.

    Precedence:
    1) Meta commands
    2) File attachment commands
    3) Steering commands (only when active)
    4) Event commands
    5) Freeform text: error (plain text during a call is no longer supported;
       use the LiveKit playground browser mic instead)
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
    if lower == "save_state":
        return ParsedCommand(kind="save_state", raw=raw, name="save_state", args="")
    if lower.startswith("save_state "):
        return ParsedCommand(
            kind="save_state",
            raw=raw,
            name="save_state",
            args=trimmed[len("save_state ") :].strip(),
        )
    if lower in {"config", "switch_actor"}:
        if active:
            return ParsedCommand(
                kind="unknown",
                raw=raw,
                name="config",
                error="⚠️ Cannot switch configuration while execution is active. Use /stop first.",
            )
        return ParsedCommand(kind="config", raw=raw, name="config")
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
    if lower.startswith("email "):
        return ParsedCommand(
            kind="event",
            raw=raw,
            name="email",
            args=trimmed[6:].strip(),
        )
    if lower == "call":
        return ParsedCommand(kind="event", raw=raw, name="call")
    if lower == "meet":
        return ParsedCommand(kind="event", raw=raw, name="meet")
    if lower == "end_call":
        return ParsedCommand(kind="event", raw=raw, name="end_call")
    if lower == "end_meet":
        return ParsedCommand(kind="event", raw=raw, name="end_meet")

    for _cmd_name in _MEET_INTERACTION_COMMANDS:
        if lower == _cmd_name:
            return ParsedCommand(kind="event", raw=raw, name=_cmd_name)
        if lower.startswith(_cmd_name + " "):
            return ParsedCommand(
                kind="event",
                raw=raw,
                name=_cmd_name,
                args=trimmed[len(_cmd_name) + 1 :].strip(),
            )

    return ParsedCommand(
        kind="unknown",
        raw=raw,
        name="unknown",
        error=f"⚠️ Unknown command: {trimmed}. Type 'help' for available commands.",
    )
