---
title: "Two brains, one voice"
description: "How Unify runs live voice calls with a fast brain that owns the audio loop and a slow brain that does the thinking — every design decision in the split: turn classification, filler dedup, proactive silence-filling, barge-in resume, and the hang-up gate."
status: draft
---

# Two brains, one voice

Voice is the least forgiving surface we ship. In chat, a ten-second pause reads as "working on it". On a phone call, three seconds of silence and the caller says "hello?" — and the moment they have to check whether you're still there, the illusion of talking to someone is gone. The problem is that the model doing the actual work — reading threads, calling tools, updating memory — takes exactly those seconds, sometimes many more.

So we run two. A **fast brain** sits inside the voice agent process itself, attached directly to the audio pipeline — speech-to-text, voice activity detection, text-to-speech — running a mini model on low reasoning effort. A **slow brain** — the full ConversationManager, our big model on high effort, with all its tools and managers — runs in the parent process, connected over a Unix socket. Both existed already in some form: I've [written before](conversation-layer.md) about keeping the conversation layer separate from the work loops. Voice just applies the same split one level further down, and much more aggressively, because the latency budget shrinks from "a chat feels responsive" to "a human doesn't say hello twice".

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-dual-brain-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-dual-brain-light.png" alt="Dual-brain voice architecture: the caller connects through a LiveKit room to the fast brain process (STT, VAD, TTS, fast LLM), which exchanges utterance events and FastBrainNotifications with the slow brain ConversationManager over an IPC socket." width="820">
  </picture>
</p>

The rule that makes the whole thing coherent: *there is one voice*. The caller should never be able to tell which brain produced a line. Everything below is in service of that.

## One structured call per turn

When the caller finishes speaking, the fast brain makes exactly one LLM call. Not a chat completion that free-associates a reply — a structured call that returns a classification and, optionally, a short line to speak. The classifications are the real design decision here, because on most turns the interesting question isn't what to say. It's whether to speak at all.

- **silence** — the caller said "ok" or "mm-hm". Say nothing, and don't wake the slow brain either. Half of a natural phone call is acknowledgement noises, and a system that replies to every "yeah" is unbearable. This is also where most of the cost saving lives: bare acks never touch the big model.
- **smalltalk** — "how's your day going?" The fast brain answers alone. The slow brain still sees the exchange afterwards but almost always decides to stay quiet rather than pile a second answer onto a pleasantry.
- **defer** — anything needing data, tools, or actual thought. The fast brain speaks one short contextual line (capped around 160 characters) as a lead-in — "let me check that thread now" — and hands the turn down. The slow brain composes the real answer.
- **continuation** and **hang_up** — for interruptions and call endings, which get their own sections below.

Why a single structured call instead of a small agent with its own tools? Latency, mostly — one round-trip on a small model is the whole budget. But also discipline: the fast brain's prompt tells it flatly that a slower, smarter version of itself will answer substantive turns moments later. Its job is not to be helpful. Its job is to hold the floor honestly for a few seconds without saying anything that could turn out to be wrong.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-turn-timeline-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-turn-timeline-light.png" alt="Swimlane timeline of one engaged turn: the caller asks a question, the fast brain classifies it as defer in one structured LLM call and speaks a contextual filler, the slow brain reads the email thread and composes the answer, and the fast brain speaks it verbatim." width="820">
  </picture>
</p>

## The slow brain writes every real answer

The substantive reply always comes from the ConversationManager, and the fast brain speaks it *verbatim*. There is no paraphrasing layer, no "fast brain rewrites the answer in a chattier register". We tried to keep the temptation out of the design entirely: the line the slow brain reasoned about, logged, and committed to memory is the exact line the caller hears. A rewriting step would be one more place for the two brains to drift apart, and drift is the failure mode this whole architecture exists to prevent.

The state model is symmetric and a little strange on first read. The slow brain's prompt describes the voice agent as its own mouth — the agent's spoken lines appear in its history as if the CM had said them itself, and it's told the agent emits a brief filler on each turn to cover latency but never composes substantive replies. Meanwhile the fast brain is hydrated at call start with the last fifty messages of history on that channel, so a caller who says "about what you emailed me yesterday" isn't met by an amnesiac. Two processes, two models, one continuous identity from both directions.

## The race between the filler and the answer

Once a turn is deferred, two things are in flight: the filler line queued for TTS and the real answer being composed. Usually the filler plays first and the answer follows — that's the designed order. But sometimes the slow brain is quick, and the worst possible output would be the answer followed by a stale lead-in: "Thursday at 2pm is confirmed. Let me check that thread now—".

The fix is unglamorous: turn counters. Every filler remembers which turn it belongs to, and if the slow brain has already produced spoken output for that turn, the filler is suppressed before it reaches the speaker. The slow brain's side of the contract is prompt-level: continue naturally from whatever the filler said, never restate it. And if the caller asks two things in a row while the slow brain is still working, the fast brain gets an injected note telling it not to repeat its previous deferral wording — nothing sounds more robotic than hearing "let me look into that" twice in ten seconds.

What we deliberately did *not* build is a reconciliation model — some third LLM that merges the two brains' output into one smooth stream. Every coordination problem in this path is solved with counters, queue clearing, and prompt rules, because a model in the hot path would reintroduce the latency the split exists to remove.

## Filling silence, reluctantly

Long silences happen: the slow brain is three tools deep into something, or the caller is thinking, or both. We fill some of them — but the design here is shaped by a strong opinion: *a bad unprompted line is worse than silence*. Nothing marks a bot faster than chirpy filler on a timer.

So the proactive speech path is slower and more considered than you'd expect. Every utterance from either side resets the cycle. After five seconds of true quiet — no one speaking, nothing queued — the decision goes to the *slow* model, on high reasoning effort, with the full call transcript, the status of any in-flight actions, and even screenshots of what's on screen when relevant. It chooses what to say and also *when*: it returns its own additional delay, and the prompt explicitly biases it toward choosing a long delay over saying something hollow. If it does produce a line and the caller starts talking before it plays, the line is discarded — stale proactive speech never gets played late.

Using the expensive model to decide whether to say "still on it — the export is about half done" looks extravagant until you invert it: the cheap part of filler is generating words, and the hard part is knowing whether anything is worth saying. That's a judgment call over the entire call state, which is exactly the slow brain's job description.

## Hanging up without being weird

Ending a call is a two-part decision, and the parts belong to different brains. Whether the call is finished is a judgment call — is the task actually wrapped? does the caller sound done or just distracted? — which makes it slow-brain work. But the moment to hang up is a timing call, sub-second, wrapped up in exactly the turn-taking machinery the fast brain owns. Give the whole decision to either brain and you get a familiar failure: the fast brain hangs up on someone mid-thought, or the slow brain says goodbye two awkward turns too late.

So we split it with a gate. By default the fast brain cannot end a call at all — "ok, bye then" gets a normal reply, not a dropped line. When the slow brain judges the call is genuinely done, it arms the gate with a reason. Only then does `hang_up` appear among the fast brain's classifications: a goodbye-shaped turn now earns a brief warm closing line and then the hang-up, and twelve seconds of dead air earns a fixed farewell instead of more filler — proactive silence-filling switches off when the gate is armed, because chattering at someone you're trying to let go of is its own kind of rude.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-hang-up-gate-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-hang-up-gate-light.png" alt="Three-stage hang-up gate: gate closed by default (fast brain cannot end the call), the slow brain arms it with allow_hang_up, and once armed a goodbye-shaped turn or 12 seconds of dead air leads to a farewell with a 1-second grace window that any speech aborts." width="820">
  </picture>
</p>

Even then there's an escape hatch: after the farewell is spoken there's a one-second grace window, and any speech from the caller aborts the close. "Oh wait, one more thing—" works on our calls, because it works on human calls. The actual teardown — Twilio hangup, LiveKit room deletion — is owned by the ConversationManager, so a call always ends through the same path regardless of which brain initiated it.

## Getting interrupted gracefully

Callers talk over the assistant constantly, and that's fine — barge-in stops the TTS immediately, and only the words that were actually spoken aloud are kept in the transcript. That last part matters more than it sounds: the transcript is the shared ground truth for both brains, so it has to record what the caller *heard*, not what the system intended to say.

The unspoken remainder isn't thrown away, though. It's stashed, and on the caller's next turn the fast brain decides what the interruption meant. "Sorry, go on" → resume the stashed line verbatim, no slow brain involved. A new question that changes the ask → the remainder is dropped and the interruption is forwarded down as a voice interrupt for the slow brain to handle properly. If the caller barged in with no actual words — a cough, a false start — the line just resumes without any model call at all. One asymmetry worth noting: an interrupted *filler* is never resumed. If you cut off "let me check that thread now—", nothing of value was lost.

## The boring failure modes

A few smaller decisions that only show up when things go wrong, which is when voice design is really tested. If the slow brain's model call fails mid-turn, the fast brain speaks a fixed apology — "sorry, I'm having trouble thinking right now, could you say that again in a moment?" — and any pending proactive line is cancelled, because following an error with a cheerful "still looking into it!" would be a lie. On outbound calls, the assistant's opener is held until the callee says something short or three seconds pass — people answer phones with "hello?", and talking over it is the fastest way to get hung up on. And every utterance on the call flows through the same message pipeline as any chat, so the transcript persists into memory and the next conversation — on any channel — can pick up where the call left off.

## One constraint, every decision

Looking back over the list, each choice falls out of the same constraint: on a live call, silence and latency are UX failures, but *wrongness is worse*. So the fast brain gets the smallest job we could carve — classify the turn, hold the floor, never commit to substance — and everything with consequences routes through the brain that can afford to think. The seams get patched with the cheapest tools that work: a turn counter, a debounce, a gate, a grace window. None of it is clever in isolation. The design is the accumulation.

## Where to look

All open at [github.com/unifyai/unify](https://github.com/unifyai/unify):

- The voice agent process — audio pipeline, turn handling, barge-in, hang-up timers: [`unify/conversation_manager/medium_scripts/call.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/medium_scripts/call.py)
- The structured turn classification and its prompt: [`unify/conversation_manager/domains/fast_brain_turn.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/fast_brain_turn.py)
- Proactive silence-filling: [`unify/conversation_manager/domains/proactive_speech.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/proactive_speech.py)
- The slow brain's side — `guide_voice_agent`, the hang-up gate, and teardown: [`unify/conversation_manager/conversation_manager.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/conversation_manager.py)
