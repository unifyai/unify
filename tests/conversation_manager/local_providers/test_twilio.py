"""Conference TwiML built by the local Twilio provider.

The rendered verbs carry the audio-hygiene attributes: no join beep (Twilio's
default beep plays an artificial "call answered" tone at the callee the moment
they pick up, and into the agent's STT) and ring audio only on an inbound
caller's own leg.
"""

from __future__ import annotations

from unify.conversation_manager.local_providers.twilio import (
    create_conference_response,
)


def test_conference_disables_join_beep() -> None:
    twiml = create_conference_response("conf-1")
    assert 'beep="false"' in twiml


def test_agent_leg_waits_in_silence() -> None:
    twiml = create_conference_response("conf-1", ringback=False)
    assert 'waitUrl=""' in twiml
    assert 'beep="false"' in twiml
