"""Which outbound channels this assistant actually has.

A live session learns its channels from the activation payload and from
inbound traffic it can see rendered in the thread. A headless run — a
scheduled task, an offline trigger — sees neither, so unless it is told, it
will either assume a channel exists and fail at the transport, or assume it
does not and silently pick a worse one. Both have happened.

Derived from :data:`SESSION_DETAILS`, which the offline runner populates
from environment at boot.
"""

from __future__ import annotations

from unify.session_details import SESSION_DETAILS


def _channel_lines() -> list[str]:
    assistant = SESSION_DETAILS.assistant
    lines: list[str] = []
    if assistant.number:
        lines.append("- SMS and phone calls: `send_sms`, `make_call`.")
    if assistant.email:
        lines.append("- Email: `send_email` (sends from my own mailbox).")
    if assistant.whatsapp_number:
        lines.append("- WhatsApp: `send_whatsapp`, `make_whatsapp_call`.")
    if assistant.discord_bot_id:
        lines.append(
            "- Discord: `send_discord_message`, `send_discord_channel_message`.",
        )
    if assistant.slack_bot_user_id:
        lines.append(
            "- Slack: `send_slack_message` (the workspace is resolved for me; "
            "pass `team_id` only to override it).",
        )
    if assistant.email_provider == "microsoft_365":
        lines.append(
            "- Microsoft Teams via my own connected account: "
            "`send_teams_message`. This one can start a new chat — pass just "
            "the `contact_id` and it finds or creates the 1:1.",
        )
    if assistant.has_ms_teams_bot:
        lines.append(
            "- Microsoft Teams via the org-installed Unify app: "
            "`send_ms_teams_bot_message`. Reply-only — it can answer in a "
            "conversation someone already started with the app, and I can "
            "omit the ids to reuse the last one with that contact, but it "
            "cannot open a new conversation. If it reports none on record, "
            "say I could not reach them on Teams rather than quietly using "
            "another channel.",
        )
    lines.append("- Unify platform messages: `send_unify_message`.")
    lines.append(
        "Send by calling the tool. Do not try to confirm a channel will work "
        "by searching transcripts, logs or contact records first — routing "
        "identifiers are resolved inside the send tools, so a search that "
        "comes up empty is not evidence the send would fail, and refusing on "
        "it drops a message that would have gone through. The tool's return "
        "value is the source of truth for whether anything was delivered.",
    )
    return lines


def offline_comms_guidance() -> str:
    """Channel inventory for a headless run, or "" when nothing is known."""
    lines = _channel_lines()
    if not lines:
        return ""
    return "\n".join(
        [
            "Outbound channels available to me on this run (`primitives.comms.*`):",
            *lines,
            "Channels not listed here are not configured for me — do not "
            "attempt them, and do not claim to have used one.",
        ],
    )
