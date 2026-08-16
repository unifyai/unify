"""
Tests for ProductionSettings LLM provider validation.

Verifies that unify.init() hard-fails when LLM provider credentials are missing
and UNIFY_VALIDATE_LLM_PROVIDERS is enabled (the default).
"""

import pytest

from unify.settings import ProductionSettings


class TestLLMProviderValidation:
    """Tests for validate_llm_providers method."""

    def test_default_model_is_gpt_5_6_sol(self):
        """UNIFY_MODEL defaults to the primary production reasoning model."""
        field_info = ProductionSettings.model_fields["UNIFY_MODEL"]
        assert field_info.default == "openai/gpt-5.6-sol@openrouter"
        effort = ProductionSettings.model_fields["UNIFY_REASONING_EFFORT"]
        assert effort.default == "high"

    def test_validation_fails_when_all_credentials_missing(self, monkeypatch):
        """Validation raises RuntimeError when no credentials are set."""
        monkeypatch.delenv("UNILLM_LLM_GATEWAY_URL", raising=False)
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="",
        )
        with pytest.raises(RuntimeError) as exc_info:
            settings.validate_llm_providers()

        error_msg = str(exc_info.value)
        assert "At least one LLM provider credential is required" in error_msg

    def test_validation_passes_when_one_credential_provided(self):
        """Validation succeeds when at least one credential is set."""
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="sk-ant-test",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="",
        )
        settings.validate_llm_providers()

    def test_validation_rejects_openai_only_credentials(self, monkeypatch):
        """OpenAI chat models route via OpenRouter, so its key grants no access."""
        monkeypatch.delenv("UNILLM_LLM_GATEWAY_URL", raising=False)
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="sk-test",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="",
        )
        with pytest.raises(RuntimeError):
            settings.validate_llm_providers()

    def test_validation_passes_when_openrouter_credential_provided(self):
        """Validation accepts OpenRouter for *@openrouter platform defaults."""
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="sk-or-test",
        )
        settings.validate_llm_providers()

    def test_validation_passes_when_deepseek_credential_provided(self):
        """Validation accepts the default model provider credential."""
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="sk-test",
            OPENROUTER_API_KEY="",
        )
        settings.validate_llm_providers()

    def test_validation_passes_when_all_credentials_provided(self):
        """Validation succeeds when all credentials are set."""
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="sk-test-openai",
            ANTHROPIC_API_KEY="sk-ant-test",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="sk-or-test",
        )
        # Should not raise
        settings.validate_llm_providers()

    def test_validation_skipped_when_disabled(self):
        """Validation is skipped when UNIFY_VALIDATE_LLM_PROVIDERS=False."""
        settings = ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=False,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="",
        )
        # Should not raise even with empty credentials
        settings.validate_llm_providers()

    def test_validation_enabled_by_default(self):
        """UNIFY_VALIDATE_LLM_PROVIDERS defaults to True in code."""
        # Verify the class-level default is True (env vars may override at runtime)
        field_info = ProductionSettings.model_fields["UNIFY_VALIDATE_LLM_PROVIDERS"]
        assert field_info.default is True


class TestBrokeredProviderValidation:
    """Hosted pods hold no provider keys; the broker sidecar is the credential."""

    @staticmethod
    def _keyless_settings() -> ProductionSettings:
        return ProductionSettings(
            UNIFY_VALIDATE_LLM_PROVIDERS=True,
            OPENAI_API_KEY="",
            ANTHROPIC_API_KEY="",
            DEEPSEEK_API_KEY="",
            OPENROUTER_API_KEY="",
        )

    def test_broker_sidecar_satisfies_validation(self, monkeypatch):
        """A configured broker passes validation with zero provider keys."""
        monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "http://127.0.0.1:8787/llm")
        monkeypatch.setenv("UNIFY_KEY", "unify-test-key")
        self._keyless_settings().validate_llm_providers()

    def test_broker_without_unify_key_fails(self, monkeypatch):
        """The gateway URL alone is not enough: UNIFY_KEY is the sidecar nonce."""
        monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "http://127.0.0.1:8787/llm")
        monkeypatch.delenv("UNIFY_KEY", raising=False)
        with pytest.raises(RuntimeError):
            self._keyless_settings().validate_llm_providers()

    def test_malformed_gateway_url_fails(self, monkeypatch):
        """A gateway value that is not a URL does not count as a broker."""
        monkeypatch.setenv("UNILLM_LLM_GATEWAY_URL", "not-a-url")
        monkeypatch.setenv("UNIFY_KEY", "unify-test-key")
        with pytest.raises(RuntimeError):
            self._keyless_settings().validate_llm_providers()

    def test_error_message_mentions_broker(self, monkeypatch):
        """The failure message tells operators about the broker alternative."""
        monkeypatch.delenv("UNILLM_LLM_GATEWAY_URL", raising=False)
        with pytest.raises(RuntimeError) as exc_info:
            self._keyless_settings().validate_llm_providers()
        assert "UNILLM_LLM_GATEWAY_URL" in str(exc_info.value)
