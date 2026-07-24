---
title: "Agents that pick up where they left off"
description: "Persistent act sessions and nested steering in Unify: how a finished task stays alive, how you correct it mid-flight, and how it asks you questions back."
status: draft
---

# Agents that pick up where they left off

Most agent frameworks give you one of two interaction models.

The first is fire-and-forget delegation: you hand the agent a task, it runs, and eventually it comes back with a result or an error. If you then want a follow-up — "great, now do the same for the March data" — you start a new task from scratch. The new run has none of the old one's context: not the credentials it discovered, not the intermediate dataframes it built, not the quirks of the API it spent ten minutes figuring out. It re-derives everything, slowly and expensively, or it gets it subtly wrong.

The second is chat: full shared context, but you're supervising every step. The agent can't go away and work for twenty minutes while you do something else.

Working with a human colleague is neither of these. You delegate a chunk of work, they go do it, you can tap them on the shoulder mid-task, they can come back to you with a question, and when they hand you the result the conversation isn't *over* — "actually, can you also…" lands in the same shared context, not a blank slate.

Unify's runtime is built around that third model. The interesting part isn't the inner agent loop itself (async tool loops are table stakes now) — it's the plumbing between the outer conversational loop and the inner working loops. There are three channels, and together they change what delegation feels like.

## The shape

The outer loop is the [`ConversationManager`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/conversation_manager.py): the thing you talk to over chat, email, or a live voice call. When work needs doing, it doesn't do the work itself — it spawns an inner working loop by calling `act(query, persist=...)`, which starts a [`CodeActActor`](https://github.com/unifyai/unify/blob/main/unify/actor/code_act_actor.py) with its own LLM transcript, its own Python execution sandbox, and its own tool surface.

Every running action gets tracked in `in_flight_actions`, and — this is the part that makes the rest work — the ConversationManager's *own* tool list is regenerated dynamically to include per-action steering tools for each one:

- `interject_<name>__<id>` — push a correction or follow-up into the running task
- `ask_<name>__<id>` — inspect what the task is doing without disturbing it
- `pause_<name>__<id>` / `resume_<name>__<id>` — suspend and continue
- `stop_<name>__<id>` — cancel
- `answer_clarification_<name>__<id>__<call>` — appears only while the task is blocked on a question

So when you message the assistant mid-task, the model deciding what to do with your message literally has a tool named after the running task in front of it. Routing your correction into the right piece of in-flight work isn't a special case — it's an ordinary tool call.

## Persistent sessions: finishing isn't ending

The channel that matters most in practice is the simplest to state: with `persist=True`, **completing the work doesn't end the session**.

A normal (`persist=False`) action returns its result and is gone — the handle moves to `completed_actions`, where the only thing you can still do is ask questions about what happened. A persistent action does something different when it finishes a piece of work: it surfaces its response upward, and then blocks, waiting:

```python
# unify/common/_async_tool/loop.py — end of a turn, persist mode
if persist:
    await _outer._notification_q.put(
        {"type": "response", "content": _response_to_surface},
    )
    logger.info("Persist mode: waiting for next interjection...")
    ...
    # Block until an interjection arrives or cancellation is requested
    ...
    continue  # Back to top of loop to process the interjection

return final_content  # persist=False: DONE
```

The ConversationManager sees the response, marks the action `awaiting_input`, and keeps it in `in_flight_actions`. The task is *done* but the session is *alive*: the full inner transcript, the Python sandbox with whatever state was built up, the guidance and skills it loaded, the credentials it found — all still in memory, attached to a handle that hasn't returned.

There is deliberately no separate "resume session" API. Continuation is just another interjection: when you say "now do March", the outer model calls `interject_<name>__<id>` and the same loop wakes up, with the new instruction appended to the transcript it already has. From the inner model's point of view, the follow-up is indistinguishable from a mid-task correction — which is exactly right, because semantically that's what it is.

This sounds like a small mechanical difference from "start a new task with a summary of the old one", but it isn't. Summaries lose the things you didn't know would matter. The live session keeps everything, including state that never made it into text: variables in the sandbox, an authenticated client object, a half-explored directory tree. The monthly-report follow-up that would have been a cold start becomes a one-line instruction into a warm context.

The cost is that keeping sessions alive is a real decision. Our system prompt pushes the outer model to default to `persist=True` whenever a follow-up is plausible — walkthroughs, investigations, anything on a voice call — and to close sessions explicitly with `stop_*` rather than letting `persist=False` silently discard context it turns out we needed.

## Talking down: interjection

Interjections are how corrections get in. When the outer loop calls `handle.interject(message)`, the message lands on the inner loop's queue and is appended to its transcript as a user message — not a system message, not a synthetic tool result. The inner model sees it the same way it would see any instruction, tagged so it knows this arrived mid-task.

The detail that makes this feel immediate rather than eventual: the inner loop runs with `interrupt_llm_with_interjections` enabled, so it races the in-flight LLM generation against the interjection queue. If a correction arrives while the model is mid-generation, the generation is cancelled and restarted with the new message included. You're not waiting for the current step to finish before your "no, wrong account" takes effect.

## Talking up: clarification

The inner loop gets the mirror-image channel. When an actor is started with clarification enabled, its tool surface includes `request_clarification(question)`. Calling it blocks that exact call site: the question travels up through the handle's clarification queue, the ConversationManager wakes, relays the question to the user, and grows an `answer_clarification_*` tool for the pending question. When the answer comes back, it's routed down the same queues and the blocked call returns with the answer as its value.

So "which of these two Alices did you mean?" doesn't kill the task or force a restart — the task is suspended at precisely the point of ambiguity, and resumes from that point with the answer in hand.

## It nests

Actions spawn sub-actions — an actor working on a big task will delegate chunks to its own nested loops. Steering follows the work down. When you pause or interject an outer action, the operation is mirrored to its children through sentinel payloads on the same queues, and each child's transcript gets a synthesized helper call recording what happened. A nested model three levels deep that gets paused and redirected *sees* that it was paused and redirected, in its own history, rather than experiencing an unexplained gap.

That transcript visibility matters more than it sounds. Models behave badly when their context silently changes underneath them; they behave fine when the change is legible.

## Honest limits

Sessions are in-process. The handle, transcript, and sandbox live in the runtime's memory — a persistent session survives across hours of conversation, but not across a process restart. Durable state still needs to be written somewhere real, and the actor does that explicitly. Python variables persist across `execute_code` calls within a session only when stateful execution is requested; the default is a clean slate per call, which is usually what you want.

And persistence has a footprint. A session holds its sandbox open, so a runtime that never stops its sessions is a runtime that slowly accumulates them. Making the outer model responsible for `stop_*` — with the same first-class tooling as everything else — is the trade we chose.

## Where to look

All of this is MIT-licensed and in the open at [github.com/unifyai/unify](https://github.com/unifyai/unify). The pieces referenced here:

- Outer loop and action tracking: [`unify/conversation_manager/conversation_manager.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/conversation_manager.py)
- `act` and the dynamic steering tools: [`unify/conversation_manager/domains/brain_action_tools.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/brain_action_tools.py), [`unify/conversation_manager/task_actions.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/task_actions.py)
- The inner loop, persist wait, interjection and clarification plumbing: [`unify/common/_async_tool/loop.py`](https://github.com/unifyai/unify/blob/main/unify/common/_async_tool/loop.py)
- The actor: [`unify/actor/code_act_actor.py`](https://github.com/unifyai/unify/blob/main/unify/actor/code_act_actor.py)

The architecture doc has the deeper tour: [`ARCHITECTURE.md`](https://github.com/unifyai/unify/blob/main/ARCHITECTURE.md).
