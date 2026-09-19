"""Request-id tagging of the messages that flow through a multi-handle
tool loop, and the notices that tell the LLM about request lifecycle."""

from __future__ import annotations


def tag_message_with_request(message: str, request_id: int) -> str:
    return f"[Request {request_id}] {message}"


def format_request_cancelled_notice(request_id: int, reason: str | None = None) -> str:
    notice = f"[System] Request {request_id} has been cancelled by the user"
    if reason:
        notice += f": {reason}"
    notice += ". Stop working on this request."
    return notice


def format_request_paused_notice(request_id: int) -> str:
    return f"[System] Request {request_id} has been paused by the user. Deprioritize work on this request until resumed."


def format_request_resumed_notice(request_id: int) -> str:
    return f"[System] Request {request_id} has been resumed by the user. You may continue work on this request."
