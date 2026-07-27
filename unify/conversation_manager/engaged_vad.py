"""Speaker-gated VAD: floor control keyed to engaged speakers.

Wraps the session VAD (Silero) so that only speech attributable to engaged
speakers reaches the AgentSession's turn/floor machinery. Everything the gate
does fails open via the scorer's conservative verdicts: an unidentified voice
is never suppressed.

Gating behavior, per inner-VAD event:

- ``START_OF_SPEECH`` while the scorer is confidently non-engaged is
  suppressed, so background chatter stops setting ``user_state=speaking``
  (which blocks queued assistant speech) and stops triggering barge-ins.
- ``INFERENCE_DONE`` during suppressed speech is neutralized
  (``speech_duration``/``speaking`` zeroed) so audio-activity interruption
  never fires for it. If the scorer flips engaged/unknown mid-speech, a
  ``START_OF_SPEECH`` is synthesized so the engaged speaker regains the floor
  without needing a fresh Silero boundary.
- A sustained non-engaged verdict while a turn is open synthesizes
  ``END_OF_SPEECH``, collapsing the "phantom turn" a noisy room otherwise
  holds open indefinitely.

STT is not routed through this class; transcription continues for everyone
and background lines surface as labeled context via the speaker tracker.
"""

from __future__ import annotations

import asyncio
import dataclasses

from livekit.agents import vad as agents_vad
from livekit.agents.utils import aio
from livekit.agents.vad import VADEventType

from unify.conversation_manager.speaker_id import RealtimeSpeakerScorer


class EngagedGateVAD(agents_vad.VAD):
    """VAD wrapper that filters floor signals through the engaged-speaker scorer."""

    def __init__(
        self,
        *,
        inner: agents_vad.VAD,
        scorer: RealtimeSpeakerScorer,
        feed_scorer: bool = True,
    ) -> None:
        """``feed_scorer`` selects where the scorer's audio comes from.

        The scorer keeps one rolling window, so it must have exactly one audio
        source; mixing two interleaves unrelated streams into the same window
        and the embeddings become meaningless. On browser meets the caller
        audio arrives over the PortAudio bridge rather than the LiveKit room,
        so the call script feeds the scorer there and sets this False.
        """
        super().__init__(capabilities=inner.capabilities)
        self._inner = inner
        self._scorer = scorer
        self._feed_scorer = feed_scorer

    @property
    def model(self) -> str:
        return self._inner.model

    @property
    def provider(self) -> str:
        return self._inner.provider

    def stream(self) -> "EngagedGateVADStream":
        return EngagedGateVADStream(self)


class EngagedGateVADStream(agents_vad.VADStream):
    def __init__(self, gate: EngagedGateVAD) -> None:
        self._gate = gate
        self._inner_stream = gate._inner.stream()
        super().__init__(gate)

    async def _main_task(self) -> None:
        inner = self._inner_stream
        scorer = self._gate._scorer

        async def _feed() -> None:
            async for item in self._input_ch:
                if isinstance(item, self._FlushSentinel):
                    inner.flush()
                    continue
                if self._gate._feed_scorer:
                    scorer.add_audio(
                        bytes(item.data),
                        item.sample_rate,
                        item.num_channels,
                    )
                inner.push_frame(item)
            inner.end_input()

        feed_task = asyncio.create_task(_feed())
        reported_speaking = False
        try:
            async for ev in inner:
                if ev.type == VADEventType.START_OF_SPEECH:
                    if scorer.confidently_non_engaged:
                        continue
                    reported_speaking = True
                    self._event_ch.send_nowait(ev)
                elif ev.type == VADEventType.END_OF_SPEECH:
                    if reported_speaking:
                        reported_speaking = False
                        self._event_ch.send_nowait(ev)
                else:  # INFERENCE_DONE
                    if (
                        ev.speaking
                        and reported_speaking
                        and scorer.confidently_non_engaged
                    ):
                        # Only non-engaged voices are holding the mic: close
                        # the open turn instead of letting it run on.
                        reported_speaking = False
                        self._event_ch.send_nowait(
                            dataclasses.replace(
                                ev,
                                type=VADEventType.END_OF_SPEECH,
                                speaking=False,
                                silence_duration=0.0,
                            ),
                        )
                        continue
                    if (
                        ev.speaking
                        and not reported_speaking
                        and not scorer.confidently_non_engaged
                    ):
                        # Speech previously suppressed (or ended early) now
                        # attributable to an engaged/unknown voice: reopen.
                        reported_speaking = True
                        self._event_ch.send_nowait(
                            dataclasses.replace(
                                ev,
                                type=VADEventType.START_OF_SPEECH,
                            ),
                        )
                    if ev.speaking and not reported_speaking:
                        # Suppressed background speech: neutralize the floor
                        # and interruption signals but keep metrics flowing.
                        ev = dataclasses.replace(
                            ev,
                            speaking=False,
                            speech_duration=0.0,
                            raw_accumulated_speech=0.0,
                        )
                    self._event_ch.send_nowait(ev)
        finally:
            await aio.cancel_and_wait(feed_task)
            await inner.aclose()
