"""Providers an account may only reach once it has real payment history.

A signup's free grant is worth farming only if it buys something expensive,
so the expensive providers are the ones held behind a payment. The gate runs
at the spend boundary rather than at model selection, so it covers the
Console and the public proxy with one rule and applies to assistants that
chose the model before the gate existed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unillm.limit_hooks import LimitCheckRequest

from unify.spending_limits import (
    PAYMENT_GATED_PROVIDERS,
    _payment_gated,
    _provider_of,
    check_spending_limits_callback,
)

#: A wallet with room — the "has paid" shape.
_PAID = {"cumulative_spend": 0.0, "limit": None, "credit_balance": 100.0}

#: Orchestra sets ``never_paid`` only for accounts with no payment history.
_NEVER_PAID = {**_PAID, "never_paid": True}


class TestProviderParsing:
    def test_reads_the_trailing_segment(self):
        assert _provider_of("claude-fable-5@anthropic") == "anthropic"

    def test_model_half_may_contain_slashes(self):
        assert _provider_of("openai/gpt-5.6-terra@openrouter") == "openrouter"

    def test_bare_model_names_no_provider(self):
        assert _provider_of("gpt-4") is None

    def test_case_is_normalised(self):
        assert _provider_of("claude-opus-5@Anthropic") == "anthropic"


class TestGatePredicate:
    def test_gated_provider_on_unpaid_account(self):
        assert _payment_gated("claude-fable-5@anthropic", never_paid=True) is True

    def test_same_call_allowed_once_paid(self):
        assert _payment_gated("claude-fable-5@anthropic", never_paid=False) is False

    def test_other_providers_unaffected(self):
        assert _payment_gated("openai/gpt-5.6-sol@openrouter", never_paid=True) is False

    def test_bare_model_is_not_gated(self):
        assert _payment_gated("gpt-4", never_paid=True) is False

    def test_anthropic_is_gated_by_default(self):
        assert "anthropic" in PAYMENT_GATED_PROVIDERS


def _run_callback(model: str, spend_data: dict):
    """Drive the callback for a personal account with *spend_data*."""

    async def _inner():
        with patch("unify.spending_limits._get_api_key", return_value="test-key"):
            with patch("unify.session_details.SESSION_DETAILS") as mock_session:
                mock_session.assistant.agent_id = None
                mock_session.user_id = "user_1"
                mock_session.org_id = None
                mock_session.assistant.timezone = "UTC"

                with patch(
                    "unify.spending_limits._get_spend_client",
                ) as mock_get_client:
                    client = MagicMock()
                    client.closed = False
                    client.get_user_spend = AsyncMock(return_value=spend_data)
                    client.get_assistant_spend = AsyncMock(return_value=spend_data)
                    client.notify_limit_reached = AsyncMock(
                        return_value={"notified": False},
                    )
                    mock_get_client.return_value = client

                    return await check_spending_limits_callback(
                        LimitCheckRequest(model=model, endpoint="chat/completions"),
                    )

    return _inner()


class TestGateAtTheSpendBoundary:
    @pytest.mark.asyncio
    async def test_denies_anthropic_for_a_never_paid_account(self):
        response = await _run_callback("claude-fable-5@anthropic", _NEVER_PAID)

        assert response.allowed is False
        # The refusal has to carry its own remedy: this string is what the
        # user is shown, and a denial they cannot act on is the failure mode
        # this gate keeps reproducing.
        assert "payment" in response.reason.lower()
        assert "billing" in response.reason.lower()

    @pytest.mark.asyncio
    async def test_reason_does_not_promise_that_a_card_is_enough(self):
        """Access turns on payment history, not on a card being attached.

        Earlier copy said "add a payment method", which sends someone to
        attach a card and hit the identical refusal afterwards.
        """
        response = await _run_callback("claude-fable-5@anthropic", _NEVER_PAID)

        assert "add a payment method" not in response.reason.lower()

    @pytest.mark.asyncio
    async def test_reason_offers_a_way_to_keep_working_now(self):
        """An included model is reachable immediately; say so."""
        response = await _run_callback("claude-fable-5@anthropic", _NEVER_PAID)

        assert "included models" in response.reason.lower()

    @pytest.mark.asyncio
    async def test_allows_anthropic_once_the_account_has_paid(self):
        response = await _run_callback("claude-fable-5@anthropic", _PAID)

        assert response.allowed is True

    @pytest.mark.asyncio
    async def test_included_models_stay_available_while_unpaid(self):
        """The gate must not touch the platform default, or trials break."""
        response = await _run_callback("openai/gpt-5.6-sol@openrouter", _NEVER_PAID)

        assert response.allowed is True

    @pytest.mark.asyncio
    async def test_applies_to_the_runtime_not_just_the_proxy(self):
        """No caller context is set here — this is the Console-driven path."""
        response = await _run_callback("claude-opus-5@anthropic", _NEVER_PAID)

        assert response.allowed is False

    @pytest.mark.asyncio
    async def test_reason_names_the_provider(self):
        response = await _run_callback("claude-opus-5@anthropic", _NEVER_PAID)

        # Spelled as a reader writes it, not as the endpoint suffix is
        # matched — this string is shown to the account holder.
        assert "Anthropic" in response.reason
