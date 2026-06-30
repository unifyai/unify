from __future__ import annotations

from typing import Any

_UNKNOWN_TEMPLATE_DELIVERY = "WhatsApp approved template notification sent."


def whatsapp_sent_history_content(event: Any) -> str:
    """Render outbound WhatsApp history as what was actually delivered."""
    intended = getattr(event, "content", "") or ""
    if not getattr(event, "via_template", False):
        return getattr(event, "delivered_content", None) or intended

    delivered = getattr(event, "delivered_content", None) or _UNKNOWN_TEMPLATE_DELIVERY
    return (
        f"{delivered}\n\n"
        "[WhatsApp template fallback: the intended message was not delivered "
        "verbatim because the WhatsApp free-form window was closed. Original "
        f'message pending resend after the user replies: "{intended}"]'
    )
