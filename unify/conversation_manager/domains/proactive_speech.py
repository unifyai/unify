from pydantic import BaseModel, Field
from unify.common.llm_client import new_slow_brain_llm_client
from unify.conversation_manager.events import GROUP_CALL_MIN_PARTICIPANTS


class ProactiveDecision(BaseModel):
    delay: int = Field(
        description="How many seconds to wait before breaking the silence. "
        "Small (a few seconds) when someone is clearly waiting on a reply; "
        "very large (many minutes — e.g. 600-1800 or more) during a natural, "
        "focused collaborative lull such as a shared working session or a quiet "
        "stretch of a group call. There is no upper limit.",
    )
    content: str = Field(
        description="The line to say when the silence is broken. A short, "
        "natural sentence that fits the moment.",
    )


PROACTIVE_PROMPT = """\
You are deciding WHEN to break a silence during a live phone call — not \
whether to. The silence will always be broken eventually; your only job is to \
pick a natural moment by setting `delay` (seconds to wait before speaking) and \
the line to say.

There has been at least 5 seconds of silence since the last utterance (user or \
assistant). You have the full conversation history above.

## Never repeat yourself (most important rule)

The transcript above is the literal record of what has already been said. The \
`assistant` turns are things *you* have already said out loud; the `user` turns \
are what the other person said. Read your own most recent `assistant` turns \
before composing `content`.

- NEVER restate, paraphrase, or re-confirm something you already said. If your \
candidate line overlaps in meaning with any recent `assistant` turn, it is a \
repeat — do not say it.
- If there is genuinely nothing new and useful to add, that is normal: do NOT \
manufacture a line to fill the air. Choose a long `delay` and stay quiet. A \
silence is far better than hearing the assistant echo itself.
- Only break the silence with a line that adds something the transcript does \
not already contain: a fresh check-in, a new piece of information, or a natural \
next step. When you do speak, vary the phrasing from anything nearby.

## Choosing the delay

Picture how a real, fluid human conversation would feel.

- Someone is clearly waiting on a reply — the assistant just asked a question \
and got no answer, or the other person trailed off mid-thought: keep it short, \
just a few seconds. A warm "Are you there?" or "Can you still hear me?" is \
exactly right. It would be weird to ask a question and then sit in silence \
forever.
- A normal lull in active back-and-forth: a short-to-moderate delay (a handful \
of seconds) with a brief, natural check-in or nudge.
- A natural, focused collaborative silence — you are working together over a \
shared screen, the assistant is carrying out a task, or it is a group call \
where people are reading, thinking, or talking among themselves: wait a long \
time, often many minutes (600-1800 seconds or more). Interrupting focused work \
or a group's own discussion is worse than staying quiet. Only break in once \
the silence has become genuinely unusual for that context.
- The user asked to wait or said they need a moment: give them lots of room — \
wait minutes, then a gentle, low-pressure check-in.
- The assistant already set a multi-minute time expectation ("this might take \
a few minutes") and an action is still running: wait. Don't refill the silence \
early — repeating filler adds no value before there are real results.
- The conversation is wrapping up (goodbyes were exchanged): wait a long time; \
the call will almost certainly end on its own first.

When you're unsure whether it's "they're waiting on me" or "this is focused \
quiet time", read the last few turns: a direct, unanswered question leans \
short; shared work or a group's own conversation leans long.

## Action awareness

You may be given an `[action status]` block listing actions that are currently \
executing or recently completed. This is the ground truth for what has and \
hasn't happened. NEVER claim an in-flight action is finished. If the assistant \
said "one moment" and the action is still executing, a brief \
patience-acknowledging reassurance is fine — but calibrate to the task. For \
quick actions (a single click or navigation), "bear with me" or "shouldn't be \
too much longer" is appropriate. For multi-step work that was framed as taking \
minutes, "still working on it, should just be a few more minutes" is better. \
Do NOT narrate specific steps ("opening the browser", "clicking on that") or \
claim the action is done.

Recent events tagged `Computer action executed:` are unverified fast-path \
attempts. If screenshots are available, check them before confirming these — \
if what you see contradicts the goal, say so rather than claiming success.

## The line to say

- `delay`: seconds to wait before speaking (small = someone's waiting on you; \
large = focused quiet time, the user needs space, or work is in progress). No \
upper limit.
- `content`: a short, natural sentence (1-2 sentences max) that adds something \
new (see "Never repeat yourself" above). Do not claim specific \
actions you are not actually performing. `content` is read aloud by TTS — use \
plain connected prose only; no numbered lists, bullets, or outline formatting \
("first… second…").

Output JSON matching the ProactiveDecision schema.\
"""


GROUP_CALL_CONTEXT = """\
[system] This is a group call, not a 1:1. Also on it: {others}.

A silence here is usually the room's, not yours to fill. People read, think, \
work, and talk among themselves, and none of that is waiting on you. Breaking \
in costs more than staying quiet, so the long delays described above are the \
normal answer on this call — minutes, not seconds.

The short-delay case needs a specific trigger you can point at in the \
transcript: somebody asked YOU something and is visibly waiting. Your own \
unanswered greeting or check-in is not that trigger. A greeting nobody replied \
to means the room was busy, and following it with "are you there?" or "can you \
still hear me?" is the single most out-of-place thing you can say on a call \
where several people are mid-conversation. Never ask whether you can be \
heard.{peers}"""

GROUP_CALL_PEERS_CONTEXT = """

Other assistants are on this call too ({peers}). Anything asked of them is \
theirs to answer, and you will not hear their reply — so a question that has \
gone quiet is not evidence it went unanswered. Do not pick it up."""

# Floor applied to the model's `delay` on a group call. The prompt asks for long
# delays there, but a check-in that lands seconds into a lull is the failure this
# path actually produces, and one bad decision is enough to be heard. Long enough
# that no natural conversational pause is interrupted; short enough that a
# genuine "we asked you something and you went quiet" is still followed up.
GROUP_CALL_MIN_DELAY_S = 45


class ProactiveSpeech:
    async def decide(
        self,
        chat_history: list[dict],
        system_prompt: str,
        action_context: str | None = None,
        other_participants: tuple[str, ...] = (),
        peer_assistants: tuple[str, ...] = (),
    ) -> tuple[ProactiveDecision, str]:
        """Decide when to break the silence and what to say.

        ``other_participants`` and ``peer_assistants`` are the live roster as
        the call manager reports it, and the group threshold is applied here so
        it matches the one both brains gate on. One other person and no peers is
        a 1:1 — a phone call, or a meet the boss is alone in — where every
        silence really is the assistant's to fill and nothing below applies.

        Returns (decision, llm_log_path) where llm_log_path is the unillm
        request+response file for the LLM call that produced this decision.
        """
        from unify.common.prompt_helpers import now

        client = new_slow_brain_llm_client(
            origin="ProactiveSpeech",
            # Same slow-brain model as ConversationManager; pin "high" so this
            # silence-filler path stays on the shared effort (already behind a
            # debounce and delay).
            reasoning_effort="high",
        )
        client.set_response_format(ProactiveDecision)
        # The decision is about time — how long the silence has run and how
        # long to let it run — and the CM system prompt this builds on is
        # deliberately clock-free (provider caching), so this single-shot
        # surface stamps its own fresh clock at assembly time.
        system_content = (
            f"{system_prompt}\n\n{PROACTIVE_PROMPT}\n\nCurrent time: {now()}."
        )
        messages = [
            {"role": "system", "content": system_content},
            *chat_history,
        ]
        several_people = len(other_participants) >= GROUP_CALL_MIN_PARTICIPANTS
        group_call = several_people or bool(peer_assistants)
        if group_call:
            messages.append(
                {
                    "role": "system",
                    "content": GROUP_CALL_CONTEXT.format(
                        others=", ".join((*other_participants, *peer_assistants)),
                        peers=(
                            GROUP_CALL_PEERS_CONTEXT.format(
                                peers=", ".join(peer_assistants),
                            )
                            if peer_assistants
                            else ""
                        ),
                    ),
                },
            )
        if action_context:
            messages.append(
                {"role": "system", "content": action_context},
            )
        response = await client.generate(messages=messages)
        log_path = ""
        pending = getattr(client, "_pending_thinking_log", None)
        if pending is not None:
            log_path = pending.last_path or ""
        decision = ProactiveDecision.model_validate_json(response)
        if group_call and decision.delay < GROUP_CALL_MIN_DELAY_S:
            decision.delay = GROUP_CALL_MIN_DELAY_S
        return decision, log_path
