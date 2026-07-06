"""
tests/conversation_manager/voice/test_voice_enrollment_events.py
=================================================================

Symbolic (infrastructure) tests for the voice-enrollment event flow between
the voice agent child process and the ConversationManager parent:

- ``VoiceEnrollmentCaptured`` persists the embedding + WAV sample onto the
  contact row and surfaces a notification.
- ``VoiceEnrollmentSuggested`` surfaces a notification and, for the boss,
  injects a guidance note so the slow brain can suggest the account-page
  enrollment recorder.
- Speaker-labelled inbound utterances are attributed to the anonymous label
  in the conversation thread instead of the registered contact's name.

No LLM runs are required (``run_llm=False`` throughout); these verify the
programmatic event handling only.
"""

from __future__ import annotations

import os
import tempfile

import numpy as np
import pytest

from tests.conversation_manager.conftest import BOSS, TEST_CONTACTS
from unify.conversation_manager.domains.contact_index import GuidanceMessage
from unify.conversation_manager.events import (
    InboundPhoneUtterance,
    PhoneCallStarted,
    VoiceEnrollmentCaptured,
    VoiceEnrollmentSuggested,
)
from unify.conversation_manager.speaker_id import pcm_to_wav_bytes

ALICE = TEST_CONTACTS[0]


def _write_temp_wav(seconds: float = 2.0) -> str:
    pcm = (np.sin(np.linspace(0, 400, int(16000 * seconds))) * 8000).astype(np.int16)
    wav_bytes = pcm_to_wav_bytes(pcm, 16000)
    fd, path = tempfile.mkstemp(prefix="test_voice_enroll_", suffix=".wav")
    with os.fdopen(fd, "wb") as f:
        f.write(wav_bytes)
    return path


async def _drain_operations_queue() -> None:
    """Run queued CM operations inline.

    The driver tests initialize managers directly and never start the
    ``listen_to_operations`` background worker, so operations queued by event
    handlers (transcript writes, enrollment persistence) are executed here
    deterministically.
    """
    from unify.conversation_manager.domains import managers_utils

    queue = managers_utils._operations_queue
    while not queue.empty():
        async_func, args, kwargs = queue.get_nowait()
        try:
            await async_func(*args, **kwargs)
        finally:
            queue.task_done()


def _guidance_messages(cm) -> list[GuidanceMessage]:
    return [
        entry.message
        for entry in cm.cm.contact_index.global_thread
        if isinstance(entry.message, GuidanceMessage)
    ]


@pytest.mark.asyncio
async def test_enrollment_captured_persists_on_contact(initialized_cm):
    """VoiceEnrollmentCaptured stores the embedding on the contact row,
    deletes the temp WAV, and pushes a notification."""
    cm = initialized_cm
    wav_path = _write_temp_wav()
    embedding = [0.6, 0.8, 0.0]

    await cm.step(
        VoiceEnrollmentCaptured(
            contact=ALICE,
            embedding=embedding,
            wav_path=wav_path,
            duration_s=42.0,
            channel="phone_call",
        ),
        run_llm=False,
    )
    await _drain_operations_queue()

    info = cm.cm.contact_manager.get_voice_enrollment_info(ALICE["contact_id"])
    assert info["enrolled"], info
    assert info["source"] == "auto_call"
    profiles = cm.cm.contact_manager.get_voice_profiles([ALICE["contact_id"]])
    assert profiles[ALICE["contact_id"]] == pytest.approx(embedding)

    # The temp WAV is consumed (read + deleted) by the handler.
    assert not os.path.exists(wav_path)

    notif_texts = [n.content for n in cm.cm.notifications_bar.notifications]
    assert any("Voice profile enrolled" in text for text in notif_texts), notif_texts


@pytest.mark.asyncio
async def test_enrollment_suggested_pushes_boss_guidance(initialized_cm):
    """For the boss, VoiceEnrollmentSuggested pushes both a notification and a
    guidance message nudging the in-app fallback recorder."""
    cm = initialized_cm

    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    await cm.step(
        VoiceEnrollmentSuggested(contact=BOSS, num_speakers=3, channel="phone_call"),
        run_llm=False,
    )

    notif_texts = [n.content for n in cm.cm.notifications_bar.notifications]
    assert any("3 distinct voices" in text for text in notif_texts), notif_texts

    guidance = _guidance_messages(cm)
    assert any("voice enrollment" in msg.content for msg in guidance), guidance


@pytest.mark.asyncio
async def test_enrollment_suggested_non_boss_no_guidance(initialized_cm):
    """Non-boss contacts get the notification but no fallback-recorder guidance
    (they cannot use the account-holder recorder)."""
    cm = initialized_cm

    await cm.step(
        VoiceEnrollmentSuggested(contact=ALICE, num_speakers=2, channel="phone_call"),
        run_llm=False,
    )

    notif_texts = [n.content for n in cm.cm.notifications_bar.notifications]
    assert any("2 distinct voices" in text for text in notif_texts), notif_texts

    guidance = [
        msg for msg in _guidance_messages(cm) if "voice enrollment" in msg.content
    ]
    assert not guidance


@pytest.mark.asyncio
async def test_speaker_labelled_utterance_uses_anonymous_label(initialized_cm):
    """An inbound phone utterance carrying a speaker_label is attributed to
    that label in the conversation thread, not the registered contact name."""
    cm = initialized_cm

    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    await cm.step(
        InboundPhoneUtterance(
            contact=BOSS,
            content="Hi, I run a logistics company and need help with invoicing.",
            speaker_label="Speaker 2",
            diarization_speaker_id="S1",
            voice_verified=False,
        ),
        run_llm=False,
    )

    messages = [
        entry.message
        for entry in cm.cm.contact_index.global_thread
        if getattr(entry.message, "content", "").startswith("Hi, I run a logistics")
    ]
    assert messages, "utterance was not pushed to the conversation thread"
    assert messages[-1].name == "Speaker 2"


@pytest.mark.asyncio
async def test_verified_utterance_keeps_contact_name(initialized_cm):
    """A voice-verified utterance without a speaker label keeps the contact's
    registered name."""
    cm = initialized_cm

    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    await cm.step(
        InboundPhoneUtterance(
            contact=BOSS,
            content="Please move my dentist appointment to Friday.",
            speaker_label=None,
            diarization_speaker_id="S0",
            voice_verified=True,
        ),
        run_llm=False,
    )

    messages = [
        entry.message
        for entry in cm.cm.contact_index.global_thread
        if getattr(entry.message, "content", "").startswith("Please move my dentist")
    ]
    assert messages, "utterance was not pushed to the conversation thread"
    assert BOSS["first_name"] in messages[-1].name
