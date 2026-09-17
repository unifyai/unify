"""Message tagging utilities for multi-handle async tool loops.

Provides simple, isolated functions for tagging messages with request IDs
as they flow through multi-handle tool loops.
"""

from __future__ import annotations


def tag_message_with_request(message: str, request_id: int) -> str:
    """Add a request ID tag prefix to a message.

    Parameters
    ----------
    message : str
        The message to tag.
    request_id : int
        The request ID to tag with.

    Returns
    -------
    str
        The tagged message with format: "[Request {id}] {message}"
    """
    return f"[Request {request_id}] {message}"


def format_request_cancelled_notice(request_id: int, reason: str | None = None) -> str:
    """Format a cancellation notice for the LLM.

    Parameters
    ----------
    request_id : int
        The request ID that was cancelled.
    reason : str | None
        Optional reason for cancellation.

    Returns
    -------
    str
        A formatted notice string for the LLM.
    """
    notice = f"[System] Request {request_id} has been cancelled by the user"
    if reason:
        notice += f": {reason}"
    notice += ". Stop working on this request."
    return notice


def format_request_paused_notice(request_id: int) -> str:
    """Format a pause notice for the LLM.

    Parameters
    ----------
    request_id : int
        The request ID that was paused.

    Returns
    -------
    str
        A formatted notice string for the LLM.
    """
    return f"[System] Request {request_id} has been paused by the user. Deprioritize work on this request until resumed."


def format_request_resumed_notice(request_id: int) -> str:
    """Format a resume notice for the LLM.

    Parameters
    ----------
    request_id : int
        The request ID that was resumed.

    Returns
    -------
    str
        A formatted notice string for the LLM.
    """
    return f"[System] Request {request_id} has been resumed by the user. You may continue work on this request."
