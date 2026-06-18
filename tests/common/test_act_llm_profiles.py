from __future__ import annotations

import pytest

from unity.common.act_llm_profiles import (
    ACT_LLM_PROFILES,
    CURRENT_ACT_LLM_PROFILE,
    DEFAULT_ACT_LLM_PROFILE,
    GPT_5_5_HIGH_ACT_LLM_PROFILE,
    describe_act_llm_profiles,
    resolve_act_llm_profile,
    use_act_llm_profile,
)


def test_default_profile_uses_actor_model():
    profile = resolve_act_llm_profile(None)

    assert profile.name == DEFAULT_ACT_LLM_PROFILE
    assert profile.model is None
    assert profile.client_kwargs == {}


def test_gpt_5_5_high_profile_uses_openai_high_effort():
    profile = resolve_act_llm_profile(GPT_5_5_HIGH_ACT_LLM_PROFILE)

    assert profile.model == "gpt-5.5@openai"
    assert profile.reasoning_effort == "high"
    assert profile.client_kwargs == {"reasoning_effort": "high"}


def test_unknown_profile_rejected():
    with pytest.raises(ValueError, match="Unknown act LLM profile"):
        resolve_act_llm_profile("not_a_profile")


def test_profile_docs_are_curated_and_include_relative_prices():
    docs = describe_act_llm_profiles()

    assert set(ACT_LLM_PROFILES) == {
        "default",
        "gpt_5_5_low",
        "gpt_5_5_medium",
        "gpt_5_5_high",
    }
    assert "roughly 11.5x the DeepSeek input-token rate" in docs
    assert "34.5x the DeepSeek output-token rate" in docs
    assert "gpt-5.5@openai" in docs
    assert "deepseek-v4-max@deepseek" in docs


def test_profile_context_is_scoped():
    original = CURRENT_ACT_LLM_PROFILE.get()
    profile = resolve_act_llm_profile(GPT_5_5_HIGH_ACT_LLM_PROFILE)

    with use_act_llm_profile(profile):
        assert CURRENT_ACT_LLM_PROFILE.get() is profile

    assert CURRENT_ACT_LLM_PROFILE.get() is original
