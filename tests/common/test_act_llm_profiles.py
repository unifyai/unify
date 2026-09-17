from __future__ import annotations

import pytest

from unify.common.act_llm_profiles import (
    ACT_LLM_PROFILES,
    DEFAULT_ACT_LLM_PROFILE,
    GPT_5_5_HIGH_ACT_LLM_PROFILE,
    resolve_act_llm_profile,
)


def test_default_profile_uses_actor_model():
    profile = resolve_act_llm_profile(None)

    assert profile.name == DEFAULT_ACT_LLM_PROFILE
    assert profile.model is None
    assert profile.client_kwargs == {}


def test_gpt_5_5_high_profile_uses_openai_high_effort():
    profile = resolve_act_llm_profile(GPT_5_5_HIGH_ACT_LLM_PROFILE)

    assert profile.model == "openai/gpt-5.5@openrouter"
    assert profile.reasoning_effort == "high"
    assert profile.client_kwargs == {"reasoning_effort": "high"}


def test_unknown_profile_rejected():
    with pytest.raises(ValueError, match="Unknown act LLM profile"):
        resolve_act_llm_profile("not_a_profile")


def test_profiles_are_curated():
    assert set(ACT_LLM_PROFILES) == {
        "default",
        "gpt_5_5_low",
        "gpt_5_5_medium",
        "gpt_5_5_high",
    }
    priced = [p.relative_price for p in ACT_LLM_PROFILES.values()]
    assert any("openai/gpt-5.5@openrouter" in price for price in priced)
    assert any("openai/gpt-5.6-sol@openrouter" in price for price in priced)
