---
title: "The conversation is not the work loop"
description: "Most agent frameworks route every message into the same loop that does the work. Unify keeps a dedicated conversation layer above the workers — the same split Thinking Machines argued for with interaction models."
status: draft
---

# The conversation is not the work loop

When people compare agent frameworks they compare the work loop: which model, which tools, how skills are stored, how long a task can run. The layer that decides how the thing actually talks to you barely comes up. I think that's because in most frameworks it doesn't exist — the chat loop and the work loop are the same loop — and you can't compare a layer nobody built.

## Where your message actually goes

In the current crop of open-source agents, every message you send becomes a prompt to the loop that does the work. There's real engineering in front of it — gateways with session routing, per-channel adapters, mention gating — but that layer is transport. It decides *which* agent run your message reaches, not what should happen conversationally.

You feel the difference the moment the agent is busy. Send a message mid-task and the framework has to pick from a menu: queue it until the run finishes, abort the run and start over, or "steer" — which in one popular implementation means your message gets appended to the next tool result inside the worker's transcript, prefixed with `User guidance:`. Your side of the conversation is literally an annotation on the worker's paperwork. In another, the default mode collects everything you said while the agent was busy and replays it as a single follow-up turn after the run ends. Scheduled heartbeat prompts get dropped entirely when the lane is busy.

None of these are bugs. They're sensible policies for the architecture they live in. But notice what's missing: nothing in the system is *thinking about the conversation* while the work runs. The model doing the work is the only thing that can decide whether you get a reply, and it's occupied.

## A layer whose only job is the conversation

Unify's runtime has a [ConversationManager](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/conversation_manager.py): a separate LLM loop whose only job is presence and judgment. It has no tools that do work. When work needs doing it calls `act(...)`, gets a handle back immediately, and carries on. The running tasks appear in its context as `in_flight_actions`, each with its own steering tools — interject, ask, pause, stop.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/conversation-layer-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/conversation-layer-light.png" alt="Left: channels feed a gateway with queue/steer/interrupt busy policies into one agent loop, where mid-task messages are queued or pasted into tool results. Right: mediums feed a ConversationManager with its own LLM loop — speak or wait, response policies, act and steering tools — delegating to two in-flight CodeActActors, with notifications flowing back." width="820">
  </picture>
</p>

So when your message lands mid-task, it doesn't wait for the work and it doesn't interrupt it either. The conversation layer takes its own turn and makes a judgment call: answer directly, push a correction into the running task, ask the task how it's going, or say nothing at all. Saying nothing is a real option — the layer carries per-contact response policies and its prompt spends a surprising amount of ink on restraint. On a live call every turn is an explicit choice between SPEAK and WAIT.

The same judgment applies in the other direction. Workers emit progress notifications as they run, but those don't go straight to you — they surface into the conversation layer's context, and it decides whether you'd want to hear about it now, later, or never. And because the layer is medium-agnostic, it's one presence across everything: the same brain, with the same memory of you, sees its active conversations across chat, email, SMS, WhatsApp, and live calls in a single view, and answers in whichever one you used.

## Thinking Machines drew the same picture

Last week Thinking Machines published [Interaction Models: A Scalable Approach to Human-AI Collaboration](https://thinkingmachines.ai/blog/interaction-models/). Most of the coverage focused on the model itself — 200ms micro-turns over continuous audio, video, and text, so the model perceives and responds at the same time instead of waiting for turn boundaries. The part I keep rereading is the diagnosis. AI labs treat autonomy as the capability that matters, and as a result, in their words, humans "increasingly get pushed out not because the work doesn't need them, but because the interface has no room for them."

Their answer is a two-part system: a time-aware interaction model that maintains real-time presence, paired with an asynchronous background model that handles sustained reasoning and tool use. When deep work is needed, the interaction model delegates — sending, as they put it, "a rich context package — not a standalone query" — and then "remains present throughout — answering follow-ups, taking new input, holding the thread — and integrates background results into the conversation as they arrive."

That is the conversation layer, stated as a research agenda. Presence and intelligence are different jobs on different clocks, so they get different loops with shared context. We've been running that split at the system level since the middle of last year: the ConversationManager is the interaction seat, the actors are the background model, `act(...)` hands over a filtered snapshot of the live conversation rather than a bare query, and results stream back for the conversation layer to weave in. On voice we even run the split twice — a fast brain handles turn-taking, fillers, and barge-in at sub-second latency while the slow brain composes what's actually worth saying.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-dual-brain-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/voice-dual-brain-light.png" alt="Voice call architecture: a caller connects through a LiveKit room to the fast brain (STT, VAD, TTS, fast LLM, fillers and barge-in), which exchanges utterance events and notifications with the slow brain — the ConversationManager's persistent reasoning loop — over IPC." width="820">
  </picture>
</p>

## The obvious objection

Thinking Machines would push back on one thing: their post argues interactivity should live *in the model*, not in a harness, and cites the bitter lesson. We're a harness. I take the point seriously, and for the lowest level — turn detection, barge-in, knowing whether a speaker is yielding or just thinking — I think they're right, and I'd happily delete our voice-activity plumbing the day their model is an API.

But look at their own system diagram: even with a natively interactive model, there are still two loops and a delegation seam between them. The seam is the architecture. Someone still has to wire the interaction seat to your channels, your memory, your team's permissions, and a fleet of long-running tasks it can steer. That's what our conversation layer is. When interaction models become available, they slot into that seat and make it much better — replacing the fast brain and a chunk of the turn machinery, inheriting all the plumbing. A framework where the conversation *is* the work loop has no seat to upgrade. The better models get at holding a thread, the more it costs to have fused the thread to the work.

## Where to look

All open at [github.com/unifyai/unify](https://github.com/unifyai/unify):

- The conversation layer's loop and event handling: [`unify/conversation_manager/conversation_manager.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/conversation_manager.py)
- `act`, `wait`, and the per-action steering tools: [`unify/conversation_manager/domains/brain_action_tools.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/brain_action_tools.py)
- The SPEAK/WAIT contract and response policies: [`unify/conversation_manager/prompt_builders.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/prompt_builders.py)
- The medium abstraction: [`unify/conversation_manager/cm_types/medium.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/cm_types/medium.py)
- The fast-brain voice script: [`unify/conversation_manager/medium_scripts/call.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/medium_scripts/call.py)
