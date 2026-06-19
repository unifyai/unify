from pydantic import BaseModel, Field
from droid.common.llm_client import new_llm_client


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
- `content`: a short, natural sentence (1-2 sentences max). Vary phrasing -- \
never repeat what was already said in the transcript. Do not claim specific \
actions you are not actually performing. `content` is read aloud by TTS — use \
plain connected prose only; no numbered lists, bullets, or outline formatting \
("first… second…").

Output JSON matching the ProactiveDecision schema.\
"""


class ProactiveSpeech:
    async def decide(
        self,
        chat_history: list[dict],
        system_prompt: str,
        action_context: str | None = None,
    ) -> tuple[ProactiveDecision, str]:
        """Decide when to break the silence and what to say.

        Returns (decision, llm_log_path) where llm_log_path is the unillm
        request+response file for the LLM call that produced this decision.
        """
        client = new_llm_client(
            origin="ProactiveSpeech",
        )
        client.set_response_format(ProactiveDecision)
        messages = [
            {"role": "system", "content": f"{system_prompt}\n\n{PROACTIVE_PROMPT}"},
            *chat_history,
        ]
        if action_context:
            messages.append(
                {"role": "system", "content": action_context},
            )
        response = await client.generate(messages=messages)
        log_path = ""
        pending = getattr(client, "_pending_thinking_log", None)
        if pending is not None:
            log_path = pending.last_path or ""
        return ProactiveDecision.model_validate_json(response), log_path
