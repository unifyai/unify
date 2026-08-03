"""Reading back what the console did with the moves that were asked for."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unify.conversation_manager.conversation_manager import ConversationManager
    from unify.conversation_manager.events import ConsoleScriptResult

# Outcomes meaning the move did not land. Anything here is worth interrupting
# for, because the assistant may have just described it as done.
_FAILED = {
    "unknown": "is not something I can open",
    "not-found": "was not there",
    "not-interactive": "was there but not clickable",
}

# Outcomes meaning it correctly did not happen. Worth knowing, not worth
# correcting: the user cut in, or told me not to navigate.
_DID_NOT_RUN = {
    "skipped": "did not happen -- I was interrupted before reaching it",
    "blocked": "did not happen -- my boss has turned off letting me navigate",
}


def summarize_console_script(event: "ConsoleScriptResult") -> tuple[str, bool]:
    """A line for the notifications bar, and whether the brain should wake.

    Successes are stated too. Knowing a move landed is what lets the assistant
    refer to where the user now is without checking, and it costs one line.
    """
    landed: list[str] = []
    problems: list[str] = []
    quiet: list[str] = []

    for entry in event.outcomes:
        if not isinstance(entry, dict):
            continue
        target = str(entry.get("target") or "")
        outcome = str(entry.get("outcome") or "")
        if not target:
            continue
        if outcome in ("done", "clicked"):
            landed.append(target)
        elif outcome in _FAILED:
            problems.append(f"{target} {_FAILED[outcome]}")
        elif outcome in _DID_NOT_RUN:
            quiet.append(f"{target} {_DID_NOT_RUN[outcome]}")

    parts: list[str] = []
    if landed:
        parts.append(f"Opened for my boss: {', '.join(landed)}.")
    if problems:
        parts.append(
            "Did not work: " + "; ".join(problems) + ". I should not say otherwise.",
        )
    if quiet:
        parts.append(" ".join(quiet) + ".")

    return " ".join(parts), bool(problems)


async def handle_console_script_result(
    event: "ConsoleScriptResult",
    cm: "ConversationManager",
) -> bool:
    """Record the outcome; report whether it needs the brain's attention.

    A successful move is noted silently. Waking a run for every landed click
    would be a turn per navigation, and there is nothing to decide -- the
    assistant already said what it was doing and it happened.
    """
    summary, needs_attention = summarize_console_script(event)
    if not summary:
        return False
    cm.notifications_bar.push_notif("Console", summary, event.timestamp)
    return needs_attention
