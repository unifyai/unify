---
title: "Fork the conversation, or go in cold"
description: "Every delegation boundary in the Unify runtime carries the same choice: fork the outer conversation into the task, or dispatch it stateless on the request string alone. On why that argument exists at every layer, and what it saves."
status: draft
---

# Fork the conversation, or go in cold

Our runtime is a hierarchy of loops. The conversation layer dispatches tasks to actors, actors call into manager loops, managers can spawn sub-agents. Every one of those boundaries has the same problem: the parent knows things the request string doesn't say. How much of that should travel with the work?

We ended up with two clean answers and an explicit argument to pick between them. Pass the parent chat context, and the child effectively *forks the outer conversation* — it inherits the full rendered history up to the moment of dispatch, and then its own timeline diverges. Omit it, and the child goes in cold: a stateless loop, a pure function of the request text it was handed.

## What the fork actually is

When the conversation layer calls `act(...)` with context included, the child's system prompt gains a section titled `## Parent Chat Context` containing the outer conversation — messages, notifications, results of recently completed work. The roles are rewritten to `outer_user` and `outer_assistant` so the child can never confuse the parent's turns with its own. The framing text tells it exactly what this is: the parent conversation's history up to the point the request arrived, there to explain the broader goal while it focuses on its specific assignment.

The git analogy holds up better than most analogies do. A fork isn't a live link — the child gets the full history at the branch point and then works on its own timeline. If the outer conversation moves on in a way the running task should know about, that arrives as an explicit interjection carrying a *diff* of the parent state since dispatch, not a re-send of everything. And the snapshot is filtered on the way down: the conversation layer's own steering tools are stripped out, so the task doesn't read about tools it can't call and start trying to steer itself.

## Where the request string falls short

The request the child receives is written by the parent's model, and a good model writes decent requests. The interesting failures are the edge cases where a decent-sounding request is still ambiguous — and the parent doesn't notice, because from where it sits there's nothing ambiguous at all.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/parent-chat-context-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/parent-chat-context-light.png" alt="One outer conversation dispatching the same request two ways. With context=True, the task inherits the full history as Parent Chat Context and already knows which report and which Sarah. With context=False, the task knows only the request string and must ask a clarification or guess." width="820">
  </picture>
</p>

Say the thread has touched two documents this week — the Q2 board deck and the weekly metrics report — and two Sarahs: one in finance, mentioned three messages up, and one in the contact book who does design. The user says "send it to Sarah once it's done", and the conversation layer dispatches `act("email the report to Sarah when it's finalized")`. Which report? Which Sarah? The forked task doesn't even register the question — the answers are sitting in its inherited history. The cold task has exactly two moves: ask, or guess.

Or the quieter version: twenty messages ago, about something else entirely, the user mentioned they're flying on Monday. Later a task gets dispatched to schedule a vendor follow-up. Nothing in that request says "not next week" — no reasonable query-writer would think to include it. The forked task sees the flight and books Friday. The cold task books Tuesday and is wrong in a way nobody catches until the calendar invite goes out.

Asking isn't a disaster — clarifications bubble up through the layers cleanly, and I wrote about that plumbing in [an earlier post](nested-steering.md). But every clarification is a round-trip that interrupts the person, for something the system already knew. And the guess is worse than the question: a wrong assumption executed confidently is the failure mode that makes people stop delegating. Passing the context eagerly means the ambiguity never becomes anyone's problem.

## Turtles all the way down

The same argument exists below the conversation layer, at every boundary. When an actor calls a manager's inner loop — `contacts.ask`, `transcripts.ask`, the task scheduler — those methods accept `_parent_chat_context` too, and the actor's loop nests its own local messages on top of whatever parent context it received, so the manager sees the whole chain. The actor's Python sandbox even wraps the primitives in a forwarding proxy: generated code writes a plain `primitives.contacts.ask(...)` and the proxy injects the context argument behind the scenes, because the one thing you can't rely on is the model remembering to thread bookkeeping through every call it writes.

## Who decides

Whether to fork is itself a judgment call, so we gave it to the model doing the dispatching. The conversation layer's `act` tool takes `include_conversation_context`, defaulting to true, with docstring guidance to switch it off when the task is self-contained — a web search, a simple lookup — where the query really is all there is to say. The choice sticks: opt out at dispatch and later interjections into that task skip context forwarding too. Nested loops make the same per-call decision on their own boundaries.

Statelessness stays cheap, in other words, and context stays one keyword argument away. Most frameworks make delegation an API call: the request string is the interface, and anything the worker needs has to be serialized into it. Making the fork a first-class option is an admission that conversations carry more state than any single request can, and that the cheapest time to transfer that state is before anyone has to ask for it.

## Where to look

All open at [github.com/unifyai/unify](https://github.com/unifyai/unify):

- The `act` tool, snapshot filtering, and interjection diffs: [`unify/conversation_manager/domains/brain_action_tools.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/brain_action_tools.py)
- The `## Parent Chat Context` framing and `outer_*` roles: [`unify/common/_async_tool/loop.py`](https://github.com/unifyai/unify/blob/main/unify/common/_async_tool/loop.py)
- The forwarding proxy for generated code: [`unify/function_manager/primitives/context_proxy.py`](https://github.com/unifyai/unify/blob/main/unify/function_manager/primitives/context_proxy.py)
- The actor's side of the argument: [`unify/actor/code_act_actor.py`](https://github.com/unifyai/unify/blob/main/unify/actor/code_act_actor.py)
