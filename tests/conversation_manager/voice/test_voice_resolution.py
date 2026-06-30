"""Voice resolution for outbound calls and meets.

The assistant's saved voice must reach every call path (Meet, phone, WhatsApp).
These cover the three seams that previously dropped it and fell back to the
provider default:

- ``ConversationManager.get_call_config`` resolves voice from the live runtime
  source (``SESSION_DETAILS.voice``) when the CM's own field is empty.
- ``set_details`` never wipes a real voice when a sparse update payload omits it.
- ``LivekitCallManager`` re-reads its config via the registered provider just
  before dispatch, so a refreshed voice is what actually gets sent.
"""

from __future__ import annotations


from unify.conversation_manager import conversation_manager as cm_mod
from unify.conversation_manager.conversation_manager import ConversationManager
from unify.conversation_manager.domains.call_manager import (
    CallConfig,
    LivekitCallManager,
)
from unify.session_details import SessionDetails


def _bare_cm() -> ConversationManager:
    """A ConversationManager with only the attributes get_call_config/set_details
    touch, bypassing the heavy real __init__."""
    cm = ConversationManager.__new__(ConversationManager)
    cm.assistant_id = "1"
    cm.user_id = "user-1"
    cm.assistant_about = "bio"
    cm.assistant_number = "+15555550000"
    cm.assistant_first_name = "T"
    cm.assistant_surname = "W1N"
    cm.job_name = "job-1"
    cm.voice_provider = ""
    cm.voice_id = ""
    return cm


def _full_details_payload(**overrides) -> dict:
    payload = {
        "user_id": "user-1",
        "assistant_id": "1",
        "assistant_first_name": "T",
        "assistant_surname": "W1N",
        "assistant_age": "20",
        "assistant_nationality": "US",
        "assistant_about": "bio",
        "assistant_number": "+15555550000",
        "assistant_email": "t@unify.ai",
        "self_contact_id": 0,
        "boss_contact_id": 1,
        "user_first_name": "U",
        "user_surname": "Ser",
        "user_number": "+15555551111",
        "user_email": "u@unify.ai",
        "voice_provider": "elevenlabs",
        "voice_id": "iP95p4xoKVk53GoZ742B",
    }
    payload.update(overrides)
    return payload


def test_get_call_config_falls_back_to_session_voice(monkeypatch):
    """Empty CM voice -> use the voice populated on SESSION_DETAILS (from env)."""
    sd = SessionDetails()
    sd.voice.provider = "elevenlabs"
    sd.voice.id = "iP95p4xoKVk53GoZ742B"
    monkeypatch.setattr(cm_mod, "SESSION_DETAILS", sd)

    cm = _bare_cm()  # voice_provider="" / voice_id=""
    cfg = cm.get_call_config()

    assert cfg.voice_provider == "elevenlabs"
    assert cfg.voice_id == "iP95p4xoKVk53GoZ742B"


def test_get_call_config_prefers_cm_voice_over_session(monkeypatch):
    """A real CM voice (a user selection) wins over the SESSION_DETAILS fallback."""
    sd = SessionDetails()
    sd.voice.provider = "elevenlabs"
    sd.voice.id = "session-default"
    monkeypatch.setattr(cm_mod, "SESSION_DETAILS", sd)

    cm = _bare_cm()
    cm.voice_provider = "cartesia"
    cm.voice_id = "chosen-voice"
    cfg = cm.get_call_config()

    assert cfg.voice_provider == "cartesia"
    assert cfg.voice_id == "chosen-voice"


def test_get_call_config_defaults_when_nothing_set(monkeypatch):
    sd = SessionDetails()  # empty voice
    monkeypatch.setattr(cm_mod, "SESSION_DETAILS", sd)

    cfg = _bare_cm().get_call_config()

    assert cfg.voice_provider == "cartesia"
    assert cfg.voice_id == ""


def test_set_details_does_not_clobber_voice_with_empty_payload(monkeypatch):
    """A sparse update (voice omitted / coerced to "") must keep the live voice."""
    sd = SessionDetails()
    monkeypatch.setattr(cm_mod, "SESSION_DETAILS", sd)

    cm = _bare_cm()
    cm.set_details(_full_details_payload())
    assert cm.voice_provider == "elevenlabs"
    assert cm.voice_id == "iP95p4xoKVk53GoZ742B"

    # Sparse follow-up with blank voice fields must not wipe the selection.
    cm.set_details(_full_details_payload(voice_provider="", voice_id=""))
    assert cm.voice_provider == "elevenlabs"
    assert cm.voice_id == "iP95p4xoKVk53GoZ742B"
    assert sd.voice.id == "iP95p4xoKVk53GoZ742B"


def test_set_details_adopts_a_new_nonempty_voice(monkeypatch):
    """A real voice change (e.g. user picks a new voice) is adopted."""
    sd = SessionDetails()
    monkeypatch.setattr(cm_mod, "SESSION_DETAILS", sd)

    cm = _bare_cm()
    cm.set_details(_full_details_payload())
    cm.set_details(
        _full_details_payload(voice_provider="cartesia", voice_id="new-voice"),
    )

    assert cm.voice_provider == "cartesia"
    assert cm.voice_id == "new-voice"


def test_dispatch_config_refreshed_from_provider():
    """The CallManager pulls fresh config (resolved voice) just before dispatch."""
    stale = CallConfig(
        assistant_id="1",
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="cartesia",
        voice_id="",
        assistant_name="T W1N",
        job_name="job-1",
    )
    manager = LivekitCallManager(stale)
    assert manager.voice_id == ""

    resolved = CallConfig(
        assistant_id="1",
        user_id="user-1",
        assistant_bio="bio",
        assistant_number="+15555550000",
        voice_provider="elevenlabs",
        voice_id="iP95p4xoKVk53GoZ742B",
        assistant_name="T W1N",
        job_name="job-1",
    )
    manager.set_config_provider(lambda: resolved)
    manager._refresh_config()

    assert manager.voice_provider == "elevenlabs"
    assert manager.voice_id == "iP95p4xoKVk53GoZ742B"
