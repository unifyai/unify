---
title: "There is no demonstration mode"
description: "Teaching Unify a new skill by sharing your screen isn't a feature we built. It falls out of images being ordinary message content, act() sessions staying alive, and functions being writable from anywhere — the abstractions were already enough."
status: draft
---

# There is no demonstration mode

The obvious way to ship "teach the assistant by showing it" is as a mode. You add a button, the user hits record, a dedicated pipeline captures the screen and the narration, a specialised model turns that into a skill file, and the skill lands in a library of things-learned-by-demonstration. That's a coherent product and several companies are shipping some version of it.

We can do the same thing and we never wrote any of that. There's no record button, no capture mode, no teaching flag — I checked before writing this, and the word "demonstration" appears in exactly one place in the runtime: a list of examples of when to keep a task session alive. That's it. Everything else is machinery that was already there for other reasons.

I want to walk through why, because it's the clearest example I have of a general principle: *every mode in a system is a place where the abstractions didn't generalise*. If you can count the modes, you can count the failures.

## Images are not a special kind of input

The load-bearing decision is upstream of anything about teaching. Visual input is plumbed through the system as ordinary message content, from the very bottom.

When you share your screen on a call, there's no recording. There's a buffer holding the latest frame, and it gets sampled when you say something — the moment you speak is the moment the picture is worth having, and the rest is discarded. That frame becomes an image part in a message. The same shape of thing as text. It joins the conversation state the assistant reasons over on its next turn, in the same list, next to what you said.

Which means the same is true of a photo you text in, an image pasted into chat, your webcam if it's on, an attachment on an email, or a screenshot of the assistant's own desktop. Every one of them becomes the same two things: an image part for whoever is reasoning right now, and a file on disk for anyone who needs it later.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/vision-everywhere-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/vision-everywhere-light.png" alt="Many sources — screen share, webcam, pasted chat images, MMS photos, the assistant's own desktop — converging on one representation (an inline image part, and a file on disk), which then fans out to every consumer: the fast brain mid-call, proactive speech, the conversation layer, a running task, and annotated guidance entries." width="820">
  </picture>
</p>

Because it's uniform, things nobody planned for work. The [fast brain on a live call](dual-brain-voice.md) gets fresh frames before its turn, so it can react to what's on screen inside a second. The silence-filling logic gets them too, which is why it can decide the useful thing to say into a pause is "that dropdown is the one you want". None of that was designed as a feature either. It's the consequence of pixels being a first-class message part rather than an attachment bolted onto one.

## Pixels by reference, not by value

Here's the part I got wrong when I first sketched this post, and it turns out to be the more interesting decision.

When the conversation layer hands work to a task, it can [fork its conversation](parent-chat-context.md) into it. You might assume the screenshots ride along. They don't — image payloads are deliberately stripped out of that snapshot. What survives is the annotation the transcript already carries: `[Screenshots: …/frame.jpg]`, sitting next to the message it belongs to. The task gets paths, not pixels, and the conversation layer is told plainly in its prompt that filepaths are the only way the task can reach an image.

The immediate reason is cost. A forked conversation with twenty frames of base64 in it is an enormous prompt to hand someone who might only need to grep a log file. But the better reason is that it's *lazy*, in the good sense. The task looks at what it decides is worth looking at, using the same tools it would use for any other image — attach the frame into its own context, or ask a focused question about it and get an answer back. Most tasks never look at all. The one that's learning your workflow looks at nearly all of them.

And the mechanism that makes this work is embarrassingly boring: an image on disk is a file, and files were already the currency that every part of the system passes around. A screenshot ends up in the same world as a PDF someone emailed you. Guidance entries can point at images, so "the export button is the small one in the top right" can carry the frame that proves it. Nothing needed a new concept.

## What a teaching session actually is

So you get on a call, share your screen, and say "right, this is how I do the weekly invoice run". Here's the entire mechanism.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/unifyai/.github/main/public_images/teaching-session-dark.png">
    <img src="https://raw.githubusercontent.com/unifyai/.github/main/public_images/teaching-session-light.png" alt="Four stages: you show it (walk through it once, frames captured as you speak), the conversation layer (sees the frames, dispatches act with persist=True, query carries filepaths), the task (loads the frames it needs, writes a function and guidance), and you correct it (an interjection, not a restart, updating the function in place) — with a loop back into the same running session." width="820">
  </picture>
</p>

The conversation layer sees your words and the frames, decides this is a job rather than a chat, and dispatches a task — with the session kept alive, because you're obviously not finished talking. That decision is made by the same rule as always: could the boss plausibly send another instruction about this? For a walkthrough, obviously yes. The prompt guidance listing when to keep a session open mentions step-by-step walkthroughs and demonstrations as examples, which is the sum total of how much this system knows about the concept of a demo.

The task pulls up the frames it cares about, watches what you did, and writes it down — as a function if the workflow is deterministic enough to be code, as prose guidance if it's a judgment call about tone or when to escalate, usually both, linked to each other and to whatever related entries already exist.

Then you say "no, not that button, the one underneath". This is where a recording mode gets awkward, because a recording is finished when you stop it, and now you're editing an artefact. For us it's an interjection into a session that never ended — the machinery I wrote about in [an earlier post](nested-steering.md), built for interrupting long-running work, doing a job nobody had teaching in mind for. The task still has the whole context. It updates the function in place rather than writing a second one, and if you shared a new frame while correcting it, that frame is in the update it just received, as another path.

Iterate as long as you like. Come back next week, reopen it, refine further. Teaching a person a workflow takes more than one pass and it's strange to build software that assumes otherwise.

## The thing you're left with

At the end there's a function in the library and some guidance next to it. They look exactly like a function and some guidance that got written during a completely ordinary task at 3am when nobody was watching. There is no flag saying this one was learned by demonstration, no separate shelf for demo-taught skills, no different execution path when it runs.

That sounds like an implementation detail and it isn't. It means a demonstrated workflow can be called by another function, refined by a [later distillation pass](functions-and-guidance.md) that never saw your screen, shared with a teammate, or found by search when something adjacent comes up. A skill that arrives through a special pipeline tends to stay in the world that pipeline built for it. One that arrives through the front door is just part of the library.

## Why I think this generalises

The temptation with a demo like screen-share teaching is to build straight at it. It demos well, the requirements are clear, and a dedicated pipeline gets you there faster than making three separate abstractions carry weight they weren't obviously designed for. I understand the pull. We've taken that road plenty of times on smaller things.

But modes compound badly. Each one is a second implementation of things you already have — its own state, its own storage format, its own edge cases — and the interactions between them are where the bugs live. What happens when someone shares their screen during a recording? What if the demo mode is on and a scheduled task fires? Every mode you add multiplies against every mode already there, and you find out in production.

The alternative is to keep asking whether a new capability is really new, or whether it's three existing things you haven't pointed at each other yet. Screen-share teaching was the second one: images being ordinary content, task sessions staying alive, and functions being writable from inside a task. Each was built for its own reason. All three pointing the same direction gives you a feature we never wrote.

## Where to look

All open at [github.com/unifyai/unify](https://github.com/unifyai/unify):

- Frame capture from screen share and webcam: [`unify/conversation_manager/medium_scripts/common.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/medium_scripts/common.py)
- Images as message parts in the reasoning turn: [`unify/conversation_manager/domains/brain.py`](https://github.com/unifyai/unify/blob/main/unify/conversation_manager/domains/brain.py)
- Redacting image payloads from forked context: [`unify/common/context_dump.py`](https://github.com/unifyai/unify/blob/main/unify/common/context_dump.py)
- Loading an image inside a running task: [`unify/common/_async_tool/images.py`](https://github.com/unifyai/unify/blob/main/unify/common/_async_tool/images.py)
- Writing what was learned into the libraries: [`unify/actor/code_act_actor.py`](https://github.com/unifyai/unify/blob/main/unify/actor/code_act_actor.py)
