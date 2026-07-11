from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True)
class ActLLMProfile:
    """Curated model profile for one top-level CodeActActor.act run."""

    name: str
    model: str | None
    reasoning_effort: str | None
    description: str
    relative_price: str

    @property
    def client_kwargs(self) -> dict[str, str]:
        kwargs: dict[str, str] = {}
        if self.reasoning_effort is not None:
            kwargs["reasoning_effort"] = self.reasoning_effort
        return kwargs


DEFAULT_ACT_LLM_PROFILE = "default"
GPT_5_5_HIGH_ACT_LLM_PROFILE = "gpt_5_5_high"


_MINIMAX_PRICE_BASELINE = (
    "Baseline profile. Uses the actor's configured model: the assistant's "
    "per-assistant default model when one is set, otherwise the platform "
    "default (normally gpt-5.6-sol@openai at high reasoning effort). The "
    "platform Sol registry rate is about $5/M input tokens and $30/M output "
    "tokens; cheaper options such as MiniMax M3 cost substantially less."
)
_GPT_5_5_PRICE = (
    "Premium OpenAI profile. gpt-5.5@openai is about $5/M input tokens and "
    "$30/M output tokens, roughly 17x the MiniMax input-token rate and "
    "25x the MiniMax output-token rate before accounting for any extra "
    "reasoning/output tokens used by higher effort."
)


ACT_LLM_PROFILES: dict[str, ActLLMProfile] = {
    DEFAULT_ACT_LLM_PROFILE: ActLLMProfile(
        name=DEFAULT_ACT_LLM_PROFILE,
        model=None,
        reasoning_effort=None,
        description=(
            "Default actor profile for ordinary action execution. Use this "
            "unless the user explicitly asks for extra thinking effort or the "
            "task is unusually ambiguous, high-stakes, or complex."
        ),
        relative_price=_MINIMAX_PRICE_BASELINE,
    ),
    "gpt_5_5_low": ActLLMProfile(
        name="gpt_5_5_low",
        model="gpt-5.5@openai",
        reasoning_effort="low",
        description=(
            "Use GPT-5.5 with low reasoning effort for premium-model quality "
            "when latency and cost still matter."
        ),
        relative_price=_GPT_5_5_PRICE,
    ),
    "gpt_5_5_medium": ActLLMProfile(
        name="gpt_5_5_medium",
        model="gpt-5.5@openai",
        reasoning_effort="medium",
        description=(
            "Use GPT-5.5 with medium reasoning effort for difficult work that "
            "needs stronger planning than the default profile."
        ),
        relative_price=_GPT_5_5_PRICE,
    ),
    GPT_5_5_HIGH_ACT_LLM_PROFILE: ActLLMProfile(
        name=GPT_5_5_HIGH_ACT_LLM_PROFILE,
        model="gpt-5.5@openai",
        reasoning_effort="high",
        description=(
            "Use GPT-5.5 with high reasoning effort when the user explicitly "
            "asks for maximum thinking effort, or when the task is highly "
            "ambiguous, high-stakes, and worth premium latency/cost."
        ),
        relative_price=_GPT_5_5_PRICE,
    ),
}


CURRENT_ACT_LLM_PROFILE: contextvars.ContextVar[ActLLMProfile] = contextvars.ContextVar(
    "current_act_llm_profile",
    default=ACT_LLM_PROFILES[DEFAULT_ACT_LLM_PROFILE],
)


def resolve_act_llm_profile(profile: str | None) -> ActLLMProfile:
    """Resolve a public profile name to its model configuration."""

    name = (profile or DEFAULT_ACT_LLM_PROFILE).strip()
    if name in ACT_LLM_PROFILES:
        return ACT_LLM_PROFILES[name]
    valid = ", ".join(sorted(ACT_LLM_PROFILES))
    raise ValueError(f"Unknown act LLM profile {profile!r}. Valid profiles: {valid}")


def describe_act_llm_profiles() -> str:
    """Return compact actor-facing documentation for curated profiles."""

    lines = ["Available `llm_profile` values:"]
    for profile in ACT_LLM_PROFILES.values():
        model = profile.model or "actor default"
        effort = profile.reasoning_effort or "actor default"
        lines.append(
            f"- `{profile.name}`: model={model}, reasoning_effort={effort}. "
            f"{profile.description} Price: {profile.relative_price}",
        )
    return "\n".join(lines)


@contextmanager
def use_act_llm_profile(profile: ActLLMProfile) -> Iterator[None]:
    """Bind the active act profile for the current async context."""

    token = CURRENT_ACT_LLM_PROFILE.set(profile)
    try:
        yield
    finally:
        CURRENT_ACT_LLM_PROFILE.reset(token)
