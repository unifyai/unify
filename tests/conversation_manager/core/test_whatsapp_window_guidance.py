"""
tests/conversation_manager/core/test_whatsapp_window_guidance.py
================================================================

Guards the WhatsApp delivery-honesty behaviour:

* Layer 1 — the ``send_whatsapp`` docstrings and the slow-brain prompt tell the
  brain that an outbound is "sent", never "arrived", until a proof row appears
  (so it never over-asserts delivery in the send turn).
* Layer 2 — the ``ConversationManager`` window-state tracking and the
  window-aware ``send_whatsapp`` docstring appendix rendered by
  ``ConversationManagerBrainActionTools``.
"""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from unity.comms.primitives import CommsPrimitives
from unity.conversation_manager.conversation_manager import ConversationManager
from unity.conversation_manager.domains import brain_action_tools as bat_mod
from unity.conversation_manager.domains.brain_action_tools import (
    ConversationManagerBrainActionTools,
)
from unity.conversation_manager.prompt_builders import build_system_prompt

pytestmark = pytest.mark.no_unify_context


# ---------------------------------------------------------------------------
# Layer 1 — docstrings + prompt never over-assert delivery
# ---------------------------------------------------------------------------


def _normalize(text: str | None) -> str:
    return " ".join((text or "").split())


def test_send_whatsapp_primitive_docstring_states_delivery_is_unconfirmed() -> None:
    doc = _normalize(CommsPrimitives.send_whatsapp.__doc__)
    assert "(not delivered directly)" in doc
    assert "placeholder" in doc
    # Must explicitly forbid claiming arrival in the send turn.
    assert "never that it has arrived" in doc


def test_send_whatsapp_to_boss_docstring_states_delivery_is_unconfirmed() -> None:
    doc = _normalize(ConversationManagerBrainActionTools.send_whatsapp_to_boss.__doc__)
    assert "(not delivered directly)" in doc
    assert "placeholder" in doc


def test_slow_brain_prompt_warns_sent_is_not_arrived() -> None:
    prompt = build_system_prompt(
        bio="A helpful assistant.",
        contact_id=1,
        first_name="Alice",
        surname="Smith",
    ).flatten()
    assert '"sent", never "arrived"' in prompt
    assert "(not delivered directly)" in prompt


# ---------------------------------------------------------------------------
# Layer 2 — ConversationManager window-state tracking
# ---------------------------------------------------------------------------


def _window_state(pending: dict, window: dict, contact_id):
    cm = SimpleNamespace(
        _pending_whatsapp_resends=pending,
        _whatsapp_window_open=window,
    )
    return ConversationManager.whatsapp_window_state(cm, contact_id)


def test_window_state_unknown_when_nothing_recorded() -> None:
    assert _window_state({}, {}, 5) is None
    assert _window_state({}, {}, None) is None


def test_window_state_reflects_observed_open_and_closed() -> None:
    assert _window_state({}, {5: True}, 5) is True
    assert _window_state({}, {5: False}, 5) is False


def test_pending_resend_forces_closed_even_if_marked_open() -> None:
    # A pending template resend is authoritative proof the window is closed.
    assert _window_state({5: "Original clue"}, {5: True}, 5) is False


def test_note_whatsapp_window_open_records_and_ignores_none() -> None:
    cm = SimpleNamespace(_whatsapp_window_open={})
    ConversationManager.note_whatsapp_window_open(cm, 5, True)
    ConversationManager.note_whatsapp_window_open(cm, 7, False)
    ConversationManager.note_whatsapp_window_open(cm, None, True)
    assert cm._whatsapp_window_open == {5: True, 7: False}


# ---------------------------------------------------------------------------
# Layer 2 — window-aware docstring appendix
# ---------------------------------------------------------------------------


def _make_brain_tools(
    *,
    window: dict,
    monkeypatch,
    pending: dict | None = None,
    is_coordinator: bool = True,
    boss_id: int = 1,
    contacts: dict | None = None,
) -> ConversationManagerBrainActionTools:
    contacts = contacts or {}
    cm = SimpleNamespace(
        _pending_whatsapp_resends=pending or {},
        _whatsapp_window_open=window,
        contact_index=SimpleNamespace(get_contact=lambda cid: contacts.get(cid)),
    )
    cm.whatsapp_window_state = lambda cid: ConversationManager.whatsapp_window_state(
        cm,
        cid,
    )
    bat = object.__new__(ConversationManagerBrainActionTools)
    bat._cm = cm
    monkeypatch.setattr(
        bat_mod.SESSION_DETAILS,
        "is_coordinator",
        is_coordinator,
        raising=False,
    )
    monkeypatch.setattr(
        bat_mod.SESSION_DETAILS,
        "boss_contact_id",
        boss_id,
        raising=False,
    )
    return bat


def test_window_doc_suffix_empty_when_state_unknown(monkeypatch) -> None:
    # Non-coordinator with no recorded state -> nothing to say.
    bat = _make_brain_tools(window={}, is_coordinator=False, monkeypatch=monkeypatch)
    assert bat._whatsapp_window_doc_suffix() == ""

    # Coordinator whose boss window is still unknown -> also nothing to say.
    bat = _make_brain_tools(window={}, is_coordinator=True, monkeypatch=monkeypatch)
    assert bat._whatsapp_window_doc_suffix() == ""


def test_window_doc_suffix_reports_closed_window(monkeypatch) -> None:
    bat = _make_brain_tools(
        window={1: False},
        contacts={1: {"first_name": "Daniel"}},
        monkeypatch=monkeypatch,
    )
    suffix = bat._whatsapp_window_doc_suffix()
    assert "CLOSED for: Daniel" in suffix
    assert "placeholder" in suffix
    assert "do NOT claim" in suffix


def test_window_doc_suffix_reports_open_window(monkeypatch) -> None:
    bat = _make_brain_tools(
        window={1: True},
        contacts={1: {"first_name": "Daniel"}},
        monkeypatch=monkeypatch,
    )
    suffix = bat._whatsapp_window_doc_suffix()
    assert "OPEN for: Daniel" in suffix
    assert "delivered verbatim" in suffix


def test_with_window_doc_returns_base_unchanged_when_unknown(monkeypatch) -> None:
    bat = _make_brain_tools(window={}, is_coordinator=False, monkeypatch=monkeypatch)

    async def base(*, contact_id, content):
        """Base doc."""
        return {"ok": True}

    assert bat._with_whatsapp_window_doc(base) is base


def test_with_window_doc_appends_and_preserves_signature(monkeypatch) -> None:
    bat = _make_brain_tools(
        window={1: False},
        contacts={1: {"first_name": "Daniel"}},
        monkeypatch=monkeypatch,
    )

    async def base(
        *,
        contact_id,
        content,
        whatsapp_number=None,
        attachment_filepath=None,
    ):
        """Send an assistant-owned WhatsApp message."""
        return {"ok": True, "content": content}

    wrapped = bat._with_whatsapp_window_doc(base)
    assert wrapped is not base
    assert "Send an assistant-owned WhatsApp message." in (wrapped.__doc__ or "")
    assert "CLOSED for: Daniel" in (wrapped.__doc__ or "")
    assert list(inspect.signature(wrapped).parameters) == [
        "contact_id",
        "content",
        "whatsapp_number",
        "attachment_filepath",
    ]
    result = asyncio.run(wrapped(contact_id=1, content="hi"))
    assert result == {"ok": True, "content": "hi"}


def test_with_window_doc_does_not_leak_self_from_bound_method(monkeypatch) -> None:
    # The real bound method is @wraps()'d over the underlying primitive, whose
    # signature still carries ``self``. The wrapper must pin the bound
    # signature so ``self`` never leaks into the tool schema.
    bat = _make_brain_tools(
        window={1: False},
        contacts={1: {"first_name": "Daniel"}},
        monkeypatch=monkeypatch,
    )
    wrapped = bat._with_whatsapp_window_doc(bat.send_whatsapp_to_boss)
    params = list(inspect.signature(wrapped).parameters)
    assert "self" not in params
    assert params == ["content", "attachment_filepath"]
