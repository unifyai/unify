"""
tests/conversation_manager/voice/test_speaker_engagement.py
============================================================

Tests for the engaged-speakers attention layer spanning both processes:

Fast brain (Assistant unit tests):
- A committed turn from a non-engaged voice yields no filler and schedules
  no slow-brain run (no ``FastBrainTurnCompleted``).
- The STT filter swallows finals from confidently non-engaged voices (they
  divert to the background sink) while forwarding everything unresolved.

ConversationManager (driver tests):
- ``set_speaker_engagement`` round-trips a label engagement to the voice
  agent over IPC and mirrors it locally; primary participants are refused.
- A non-engaged inbound utterance is pushed as labeled context and requests
  a debounced (non-user-origin) slow-brain run instead of a user turn.
- The engagement tools are exposed during voice sessions with a live status
  appendix.

Plus one eval-style handoff test: on "talk to my friend", the slow brain
calls ``engage_speaker``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from tests.conversation_manager.conftest import BOSS
from unify.conversation_manager.cm_types.mode import Mode
from unify.conversation_manager.events import (
    FAST_BRAIN_TURN_DEFER,
    FastBrainTurnCompleted,
    InboundPhoneUtterance,
    PhoneCallStarted,
)
from unify.conversation_manager.speaker_id import (
    EngagedSpeakers,
    SpeakerResolution,
)


@pytest.fixture
def boss_contact():
    return {
        "contact_id": 1,
        "first_name": BOSS["first_name"],
        "surname": BOSS["surname"],
        "phone_number": BOSS["phone_number"],
        "email_address": BOSS["email_address"],
        "is_system": True,
    }


def _make_assistant(boss_contact, **kwargs):
    from unify.conversation_manager.medium_scripts.call import Assistant

    return Assistant(
        contact=boss_contact,
        boss=boss_contact,
        channel="phone_call",
        instructions="x",
        outbound=False,
        **kwargs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fast brain: non-engaged turn suppression
# ─────────────────────────────────────────────────────────────────────────────


class TestNonEngagedTurnSuppression:
    @pytest.mark.asyncio
    async def test_llm_node_suppressed_for_non_engaged_turn(self, boss_contact):
        from livekit.agents import llm

        a = _make_assistant(boss_contact)
        a.call_received = True
        a._current_turn_engaged = False
        a._publish_fast_brain_turn_completed = AsyncMock()

        chunks = [chunk async for chunk in a.llm_node(llm.ChatContext(), [], None)]

        assert chunks == []
        a._publish_fast_brain_turn_completed.assert_not_awaited()
        assert a.user_turn_generating is False

    @pytest.mark.asyncio
    async def test_on_user_turn_completed_captures_engagement(self, boss_contact):
        from livekit.agents import llm

        a = _make_assistant(boss_contact)
        a._turn_engaged_provider = lambda: False

        ctx = llm.ChatContext()
        msg = ctx.add_message(role="user", content=["hello there"])
        await a.on_user_turn_completed(ctx, msg)

        assert a._current_turn_engaged is False

        a._turn_engaged_provider = lambda: True
        msg = ctx.add_message(role="user", content=["hello again"])
        await a.on_user_turn_completed(ctx, msg)
        assert a._current_turn_engaged is True


# ─────────────────────────────────────────────────────────────────────────────
# Fast brain: STT filter
# ─────────────────────────────────────────────────────────────────────────────


class _FakeTracker:
    def __init__(self, resolutions: dict[str, SpeakerResolution]) -> None:
        self._resolutions = resolutions
        self.observed: list[str | None] = []

    def observe_final_transcript(self, speaker_id, *, end_ts=None) -> None:
        self.observed.append(speaker_id)

    def resolve(self, speaker_id):
        return self._resolutions.get(speaker_id)


def _final_event(text: str, speaker_id: str | None):
    from livekit.agents import stt

    return stt.SpeechEvent(
        type=stt.SpeechEventType.FINAL_TRANSCRIPT,
        alternatives=[
            stt.SpeechData(language="en", text=text, speaker_id=speaker_id),
        ],
    )


def _interim_event(text: str):
    from livekit.agents import stt

    return stt.SpeechEvent(
        type=stt.SpeechEventType.INTERIM_TRANSCRIPT,
        alternatives=[stt.SpeechData(language="en", text=text)],
    )


async def _run_filter(assistant, events):
    async def _gen():
        for ev in events:
            yield ev

    return [ev async for ev in assistant._filter_stt_events(_gen())]


class TestSttEngagementFilter:
    @pytest.mark.asyncio
    async def test_swallows_non_engaged_final(self, boss_contact):
        engaged = EngagedSpeakers(permanent_contact_ids={1})
        tracker = _FakeTracker(
            {
                "S0": SpeakerResolution(contact_id=1, verified=True),
                "S1": SpeakerResolution(label="Speaker 2"),
            },
        )
        a = _make_assistant(
            boss_contact,
            speaker_tracker=tracker,
            engaged_speakers=engaged,
        )
        background: list[tuple[str, str | None]] = []
        a._on_background_final = lambda text, sid: background.append((text, sid))

        forwarded = await _run_filter(
            a,
            [
                _final_event("hello, please move my meeting", "S0"),
                _final_event("so anyway about the invoice", "S1"),
                _final_event("unresolved voice speaks", "S3"),
            ],
        )

        texts = [ev.alternatives[0].text for ev in forwarded]
        assert texts == [
            "hello, please move my meeting",
            "unresolved voice speaks",
        ]
        assert background == [("so anyway about the invoice", "S1")]
        # Every final (gated or not) feeds the tracker.
        assert tracker.observed == ["S0", "S1", "S3"]

    @pytest.mark.asyncio
    async def test_engaged_label_forwards_again(self, boss_contact):
        engaged = EngagedSpeakers(permanent_contact_ids={1})
        tracker = _FakeTracker({"S1": SpeakerResolution(label="Speaker 2")})
        a = _make_assistant(
            boss_contact,
            speaker_tracker=tracker,
            engaged_speakers=engaged,
        )
        a._on_background_final = lambda text, sid: None

        assert await _run_filter(a, [_final_event("first", "S1")]) == []
        engaged.engage(label="Speaker 2")
        forwarded = await _run_filter(a, [_final_event("second", "S1")])
        assert [ev.alternatives[0].text for ev in forwarded] == ["second"]

    @pytest.mark.asyncio
    async def test_interims_dropped_only_when_scorer_confident(self, boss_contact):
        class _Scorer:
            confidently_non_engaged = False

        scorer = _Scorer()
        a = _make_assistant(
            boss_contact,
            speaker_tracker=_FakeTracker({}),
            engaged_speakers=EngagedSpeakers(permanent_contact_ids={1}),
            realtime_scorer=scorer,
        )

        forwarded = await _run_filter(a, [_interim_event("partial words")])
        assert len(forwarded) == 1

        scorer.confidently_non_engaged = True
        forwarded = await _run_filter(a, [_interim_event("background words")])
        assert forwarded == []

    @pytest.mark.asyncio
    async def test_fail_open_without_engagement_state(self, boss_contact):
        a = _make_assistant(boss_contact)
        events = [
            _final_event("anything", "S5"),
            _interim_event("partial"),
        ]
        forwarded = await _run_filter(a, events)
        assert len(forwarded) == 2


# ─────────────────────────────────────────────────────────────────────────────
# CM: engagement round-trip + background utterances
# ─────────────────────────────────────────────────────────────────────────────


def _capture_ipc(cm, monkeypatch) -> list[tuple[str, str]]:
    published: list[tuple[str, str]] = []
    original = cm.cm.event_broker.publish

    async def _recording_publish(channel: str, message: str) -> int:
        published.append((channel, message))
        return await original(channel, message)

    monkeypatch.setattr(cm.cm.event_broker, "publish", _recording_publish)
    return published


@pytest.mark.asyncio
async def test_engage_label_round_trip(initialized_cm, monkeypatch):
    """Engaging/disengaging a label updates the mirror and publishes IPC."""
    import json

    cm = initialized_cm
    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    cm.cm.call_manager.reset_speaker_engagement(BOSS, BOSS)
    cm.cm.call_manager.note_speaker_label("Speaker 2")
    published = _capture_ipc(cm, monkeypatch)

    result = await cm.cm.set_speaker_engagement(speaker="speaker 2", engaged=True)
    assert result == {"status": "engaged", "speaker": "Speaker 2"}
    assert cm.cm.call_manager.engaged_labels == {"Speaker 2"}
    channel, message = published[-1]
    assert channel == "app:call:speaker_engagement"
    assert json.loads(message) == {"action": "engage", "label": "Speaker 2"}

    result = await cm.cm.set_speaker_engagement(speaker="Speaker 2", engaged=False)
    assert result["status"] == "disengaged"
    assert cm.cm.call_manager.engaged_labels == set()
    channel, message = published[-1]
    assert json.loads(message)["action"] == "disengage"

    cm.cm.mode = Mode.TEXT


@pytest.mark.asyncio
async def test_disengage_primary_participant_refused(initialized_cm, monkeypatch):
    cm = initialized_cm
    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    cm.cm.call_manager.reset_speaker_engagement(BOSS, BOSS)
    published = _capture_ipc(cm, monkeypatch)

    result = await cm.cm.set_speaker_engagement(
        speaker=BOSS["first_name"],
        engaged=False,
    )
    assert result["status"] == "refused"
    assert not published

    cm.cm.mode = Mode.TEXT


@pytest.mark.asyncio
async def test_engagement_requires_active_call(initialized_cm):
    cm = initialized_cm
    cm.cm.mode = Mode.TEXT
    result = await cm.cm.set_speaker_engagement(speaker="Speaker 2", engaged=True)
    assert result == {"status": "no_active_call"}


@pytest.mark.asyncio
async def test_background_utterance_is_context_not_turn(initialized_cm):
    """An engaged=False utterance lands as a labeled thread message and only
    requests a debounced slow-brain run (no user-origin turn handling)."""
    cm = initialized_cm
    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    cm.cm.call_manager.reset_speaker_engagement(BOSS, BOSS)

    result = await cm.step(
        InboundPhoneUtterance(
            contact=BOSS,
            content="Hey, has anyone seen the projector cable?",
            speaker_label="Speaker 2",
            diarization_speaker_id="S1",
            voice_verified=False,
            engaged=False,
        ),
        run_llm=False,
    )

    assert result.llm_requested
    messages = [
        entry.message
        for entry in cm.cm.contact_index.global_thread
        if getattr(entry.message, "content", "").startswith("Hey, has anyone seen")
    ]
    assert messages, "background utterance missing from the conversation thread"
    assert messages[-1].name == "Speaker 2"
    assert "Speaker 2" in cm.cm.call_manager.known_speaker_labels

    cm.cm.mode = Mode.TEXT


@pytest.mark.asyncio
async def test_engagement_tools_exposed_during_call(initialized_cm):
    from unify.conversation_manager.domains.brain_action_tools import (
        ConversationManagerBrainActionTools,
    )

    cm = initialized_cm
    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    cm.cm.call_manager.reset_speaker_engagement(BOSS, BOSS)
    cm.cm.call_manager.note_speaker_label("Speaker 2")

    tools = ConversationManagerBrainActionTools(cm.cm).as_tools()
    assert "engage_speaker" in tools
    assert "disengage_speaker" in tools
    doc = tools["engage_speaker"].__doc__ or ""
    assert "Speaker 2" in doc, doc

    cm.cm.mode = Mode.TEXT
    tools = ConversationManagerBrainActionTools(cm.cm).as_tools()
    assert "engage_speaker" not in tools


# ─────────────────────────────────────────────────────────────────────────────
# Eval: handoff engages the guest
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.eval
@pytest.mark.asyncio
async def test_handoff_engages_background_speaker(initialized_cm):
    """On "talk to my friend", the slow brain engages the guest's label."""
    cm = initialized_cm
    await cm.step(PhoneCallStarted(contact=BOSS), run_llm=False)
    cm.cm.call_manager.reset_speaker_engagement(BOSS, BOSS)

    # The guest's earlier words are already in the transcript as background.
    await cm.step(
        InboundPhoneUtterance(
            contact=BOSS,
            content="Hi, I'm Sam — I had a quick question about the invoices.",
            speaker_label="Speaker 2",
            diarization_speaker_id="S1",
            voice_verified=False,
            engaged=False,
        ),
        run_llm=False,
    )

    handoff = (
        "I'm handing you over to my friend Sam for a moment — "
        "he's the one you heard as Speaker 2. Please talk to him."
    )
    await cm.step(
        InboundPhoneUtterance(
            contact=BOSS,
            content=handoff,
            diarization_speaker_id="S0",
            voice_verified=True,
        ),
        run_llm=False,
    )
    await cm.step(
        FastBrainTurnCompleted(
            contact=BOSS,
            turn_id=1,
            user_content=handoff,
            classification=FAST_BRAIN_TURN_DEFER,
            intended_speech="One moment.",
        ),
        run_llm=True,
    )

    assert "engage_speaker" in cm.all_tool_calls, cm.all_tool_calls
    assert cm.cm.call_manager.engaged_labels == {"Speaker 2"}

    cm.cm.mode = Mode.TEXT
