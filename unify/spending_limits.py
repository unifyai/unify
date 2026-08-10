"""
Spending limit checking for Unity.

This module implements the limit check callback that UniLLM invokes before
each LLM call. It queries Orchestra's spend endpoints to check if spending
limits have been exceeded for the current assistant, user, or organization.

The callback is registered with UniLLM during unify.init() and uses
SESSION_DETAILS to determine the current context.

Limit hierarchy:
- Personal context (user's personal API key): assistant + user limits
- Organization context (org API key): assistant + member + org limits

All checks run in parallel for minimal latency impact.

Uses ``unisdk.AsyncSpendClient`` (aiohttp-backed) for connection pooling,
automatic retries, and exponential backoff — matching the reliability
characteristics of the sync ``unisdk.utils.http`` session.
"""

from __future__ import annotations

import asyncio
import logging
import os
import zoneinfo
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, AsyncIterator, List, Optional

from unisdk.async_admin import AsyncSpendClient, SpendRequestError

if TYPE_CHECKING:
    from unillm.limit_hooks import LimitCheckRequest, LimitCheckResponse

logger = logging.getLogger(__name__)

LIMIT_CHECK_TIMEOUT = 5.0

#: Providers an account may only reach once it has real payment history.
#:
#: These are the models whose per-call cost is high enough that a signup's
#: free grant buys a meaningful amount of them, which makes them the ones
#: worth farming an account for. Requiring payment first removes the return
#: on creating the account at all, rather than trying to detect the misuse
#: afterwards.
#:
#: Provider-scoped rather than model-scoped on purpose: a per-model list has
#: to be revised every time a vendor ships, and the gap between the ship and
#: the revision is the exposure.
PAYMENT_GATED_PROVIDERS = frozenset(
    p.strip().lower()
    for p in os.environ.get("PAYMENT_GATED_PROVIDERS", "anthropic").split(",")
    if p.strip()
)

_spend_client: Optional[AsyncSpendClient] = None
_spend_client_key: Optional[str] = None


@dataclass(frozen=True)
class _CallerContext:
    """Per-request caller identity for limit checks with no assistant session.

    The assistant runtime identifies its caller process-wide (``UNIFY_KEY``
    plus ``SESSION_DETAILS``), but a multi-tenant host — the gateway's
    ``/unillm/chat/completions`` proxy — serves a different user on every
    request and must not read either. Setting this contextvar redirects
    :func:`_get_api_key` and :func:`_get_spend_client` at the caller for the
    duration of one request.
    """

    api_key: str
    user_id: Optional[str] = None
    org_id: Optional[int] = None
    client: Optional[AsyncSpendClient] = None


_CALLER: ContextVar[Optional[_CallerContext]] = ContextVar(
    "unify_limit_check_caller",
    default=None,
)


@asynccontextmanager
async def caller_context(
    api_key: str,
    *,
    user_id: Optional[str] = None,
    org_id: Optional[int] = None,
) -> AsyncIterator[None]:
    """Scope limit checks to one caller's API key for the enclosed block.

    Wrap any host-side LLM call made on behalf of an authenticated third
    party in this, so the balance / trial-cap / spending-cap gates resolve
    against *their* wallet rather than the host process's ``UNIFY_KEY``.

    The spend client is created and closed per request rather than cached
    per key: a public endpoint sees unbounded distinct keys, and a keyed
    cache would grow without limit and hold a connection pool open for
    every caller that ever hit the process.
    """
    client = AsyncSpendClient(api_key=api_key, timeout=LIMIT_CHECK_TIMEOUT)
    token = _CALLER.set(
        _CallerContext(
            api_key=api_key,
            user_id=user_id,
            org_id=org_id,
            client=client,
        ),
    )
    try:
        yield
    finally:
        _CALLER.reset(token)
        try:
            await client.close()
        except Exception as e:  # pragma: no cover - close is best-effort
            logger.warning(f"Failed to close spend client: {type(e).__name__}: {e}")


def _charges_billing() -> bool:
    """Whether Unify platform billing gates apply (mirrors Orchestra settings)."""
    from unify.settings import SETTINGS

    is_self_host = os.environ.get("SELF_HOST", "0") == "1"
    return SETTINGS.DEPLOY_ENV == "production" and not is_self_host


def _get_api_key() -> Optional[str]:
    """Get the user API key for Orchestra calls.

    An active :func:`caller_context` wins over the process key so a
    multi-tenant host checks the caller's wallet, not its own.
    """
    caller = _CALLER.get()
    if caller is not None:
        return caller.api_key
    return os.getenv("UNIFY_KEY")


def _get_spend_client() -> AsyncSpendClient:
    """Get or create the AsyncSpendClient for limit checks.

    Inside a :func:`caller_context` this is that request's own client.
    Otherwise it is the process-wide client, recreated when ``UNIFY_KEY``
    changes so a client cached under a stale key never authenticates
    later checks.
    """
    caller = _CALLER.get()
    if caller is not None and caller.client is not None:
        return caller.client

    global _spend_client, _spend_client_key
    api_key = _get_api_key()
    if _spend_client is None or _spend_client.closed or _spend_client_key != api_key:
        _spend_client = AsyncSpendClient(
            api_key=api_key,
            timeout=LIMIT_CHECK_TIMEOUT,
        )
        _spend_client_key = api_key
    return _spend_client


@dataclass
class _LimitCheckResult:
    """Internal result from a single limit check."""

    exceeded: bool
    limit_type: Optional[str] = None
    limit_value: Optional[float] = None
    current_spend: Optional[float] = None
    entity_id: Optional[str] = None
    entity_name: Optional[str] = None
    limit_set_at: Optional[str] = None  # ISO format timestamp
    organization_id: Optional[int] = None  # For member limits
    credit_balance: Optional[float] = None  # Billing account credit balance
    # Billing mode of the underlying account: "CREDITS" (pre-paid wallet,
    # subject to the credit_balance gate) or "METERED" (invoiced
    # monthly, gate must be skipped). Defaults to None when the spend
    # endpoint didn't surface the field — older Orchestra builds — in
    # which case we fall back to the legacy CREDITS-mode behaviour.
    billing_mode: Optional[str] = None
    # Account frozen server-side (admin freeze, card gate, abuse sweep).
    account_suspended: bool = False
    # Account has no real payment history. Set by Orchestra; false for
    # internal accounts and admin-granted free trials.
    never_paid: bool = False
    # The check could not be completed (Orchestra unreachable or errored).
    # Distinct from a clean 404, which legitimately means "no limit set".
    check_failed: bool = False
    # Whether this account may spend outside the Console. Only the
    # multi-tenant (proxy) path acts on it.
    api_access_allowed: bool = True


# Why a billing gate refused. Callers pick the wording the user sees from
# this, so a refusal never reaches them as a generic fault they are told to
# retry — the two causes need opposite advice, and neither clears on a retry.
GATE_BLOCK_ACCOUNT_SUSPENDED = "account_suspended"
GATE_BLOCK_CREDITS_DEPLETED = "credits_depleted"


@dataclass(frozen=True)
class BillingGateState:
    """Whether billing permits work, for surfaces that stay conversational."""

    allowed: bool = True
    reason: Optional[str] = None
    blocked_by: Optional[str] = None
    credit_balance: Optional[float] = None
    billing_mode: Optional[str] = None


def _get_current_month(timezone: str = "UTC") -> str:
    """Get current month string in YYYY-MM format for the given timezone."""
    try:
        tz = zoneinfo.ZoneInfo(timezone)
    except Exception:
        tz = zoneinfo.ZoneInfo("UTC")
    return datetime.now(tz).strftime("%Y-%m")


def _parse_spend_result(
    data: dict,
    limit_type: str,
    entity_id: str,
    *,
    entity_name: Optional[str] = None,
    organization_id: Optional[int] = None,
) -> _LimitCheckResult:
    """Parse a spend endpoint response into a ``_LimitCheckResult``."""
    limit = data.get("limit")
    spend = data.get("cumulative_spend", 0)
    limit_set_at = data.get("limit_set_at")
    credit_balance = data.get("credit_balance")
    billing_mode = data.get("billing_mode")
    gate_fields = {
        "account_suspended": bool(data.get("account_suspended", False)),
        "never_paid": bool(data.get("never_paid", False)),
        # Defaults True so an Orchestra build predating the field — or an
        # endpoint that doesn't carry it — never silently locks the API.
        "api_access_allowed": bool(data.get("api_access_allowed", True)),
    }

    if limit is None:
        return _LimitCheckResult(
            exceeded=False,
            credit_balance=credit_balance,
            billing_mode=billing_mode,
            **gate_fields,
        )

    return _LimitCheckResult(
        exceeded=spend >= limit,
        limit_type=limit_type,
        limit_value=limit,
        current_spend=spend,
        entity_id=entity_id,
        entity_name=entity_name or data.get("agent_name"),
        limit_set_at=limit_set_at,
        organization_id=organization_id,
        credit_balance=credit_balance,
        billing_mode=billing_mode,
        **gate_fields,
    )


def _billing_gate_from_spend_data(data: dict) -> BillingGateState:
    """Read every refusal the spend payload carries, not just the wallet.

    Mirrors the deny order of :func:`check_spending_limits_callback`, which is
    what actually stops the call: suspension outranks billing mode, so a
    suspended METERED account is still refused. A cause this gate does not
    know about surfaces to the user as an unexplained failure they are told to
    retry, and silently drops any work that needed a model turn.
    """
    credit_balance = data.get("credit_balance")
    billing_mode = data.get("billing_mode")

    if bool(data.get("account_suspended", False)):
        return BillingGateState(
            allowed=False,
            reason="Account is suspended.",
            blocked_by=GATE_BLOCK_ACCOUNT_SUSPENDED,
            credit_balance=credit_balance,
            billing_mode=billing_mode,
        )

    if billing_mode == "METERED":
        return BillingGateState(
            allowed=True,
            credit_balance=credit_balance,
            billing_mode=billing_mode,
        )

    if credit_balance is not None and credit_balance <= 0:
        return BillingGateState(
            allowed=False,
            reason=(
                f"Insufficient credits: balance is ${credit_balance:.2f}. "
                "Please add credits to continue."
            ),
            blocked_by=GATE_BLOCK_CREDITS_DEPLETED,
            credit_balance=credit_balance,
            billing_mode=billing_mode,
        )

    return BillingGateState(
        allowed=True,
        credit_balance=credit_balance,
        billing_mode=billing_mode,
    )


async def check_billing_gate_state() -> BillingGateState:
    """Return whether billing permits work for the active account.

    Narrower than ``check_spending_limits_callback``: it answers "will the
    spend boundary refuse this account outright", not "is this particular
    model call within every limit". Voice surfaces keep the call alive and use
    it to stop offering work that cannot run; text surfaces use it to say why
    before a turn is attempted.

    Fails open — an unreadable account is not a refused one.
    """
    from .session_details import SESSION_DETAILS

    api_key = _get_api_key()
    if not api_key:
        logger.debug("Billing gate check skipped: no API key")
        return BillingGateState()

    user_id = SESSION_DETAILS.user_id
    org_id = SESSION_DETAILS.org_id
    if not user_id:
        logger.debug("Billing gate check skipped: missing user context")
        return BillingGateState()

    timezone = "UTC"
    if SESSION_DETAILS.assistant:
        timezone = SESSION_DETAILS.assistant.timezone or "UTC"
    month = _get_current_month(timezone)

    try:
        client = _get_spend_client()
        if org_id is not None:
            data = await client.get_org_spend(org_id=org_id, month=month)
        else:
            data = await client.get_user_spend(month=month)
        return _billing_gate_from_spend_data(data)
    except SpendRequestError as e:
        if e.status != 404:
            logger.warning(f"Failed to check billing gate: {type(e).__name__}: {e}")
        return BillingGateState()
    except Exception as e:
        logger.warning(f"Failed to check billing gate: {type(e).__name__}: {e}")
        return BillingGateState()


async def _check_assistant_limit(
    agent_id: str,
    month: str,
) -> _LimitCheckResult:
    """Check if assistant spending limit is exceeded."""
    try:
        client = _get_spend_client()
        data = await client.get_assistant_spend(agent_id=int(agent_id), month=month)
        return _parse_spend_result(data, "assistant", agent_id)
    except SpendRequestError as e:
        if e.status == 404:
            return _LimitCheckResult(exceeded=False)
        logger.warning(f"Failed to check assistant limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)
    except Exception as e:
        logger.warning(f"Failed to check assistant limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)


async def _check_user_limit(
    user_id: str,
    month: str,
) -> _LimitCheckResult:
    """Check if user's personal spending limit is exceeded."""
    try:
        client = _get_spend_client()
        data = await client.get_user_spend(month=month)
        return _parse_spend_result(data, "user", user_id)
    except SpendRequestError as e:
        if e.status == 404:
            return _LimitCheckResult(exceeded=False)
        logger.warning(f"Failed to check user limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)
    except Exception as e:
        logger.warning(f"Failed to check user limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)


async def _check_member_limit(
    user_id: str,
    org_id: int,
    month: str,
) -> _LimitCheckResult:
    """Check if organization member's spending limit is exceeded."""
    try:
        client = _get_spend_client()
        data = await client.get_member_spend(
            user_id=user_id,
            org_id=org_id,
            month=month,
        )
        return _parse_spend_result(
            data,
            "member",
            user_id,
            organization_id=org_id,
        )
    except SpendRequestError as e:
        if e.status == 404:
            return _LimitCheckResult(exceeded=False)
        logger.warning(f"Failed to check member limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)
    except Exception as e:
        logger.warning(f"Failed to check member limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)


async def _check_org_limit(
    org_id: int,
    month: str,
) -> _LimitCheckResult:
    """Check if organization spending limit is exceeded."""
    try:
        client = _get_spend_client()
        data = await client.get_org_spend(org_id=org_id, month=month)
        return _parse_spend_result(
            data,
            "organization",
            str(org_id),
            entity_name=data.get("organization_name"),
        )
    except SpendRequestError as e:
        if e.status == 404:
            return _LimitCheckResult(exceeded=False)
        logger.warning(f"Failed to check org limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)
    except Exception as e:
        logger.warning(f"Failed to check org limit: {type(e).__name__}: {e}")
        return _LimitCheckResult(exceeded=False, check_failed=True)


async def _notify_limit_reached(
    result: _LimitCheckResult,
    month: str,
) -> None:
    """
    Fire-and-forget notification to Orchestra when a limit is reached.

    This calls Orchestra's spending-limit-reached endpoint which:
    - Deduplicates notifications (won't spam for same limit)
    - Sends email to affected users
    - Records the notification for auditing

    Errors are logged but don't affect the limit check response.
    """
    payload = {
        "limit_type": result.limit_type,
        "entity_id": result.entity_id,
        "limit_value": result.limit_value,
        "current_spend": result.current_spend,
        "month": month,
        "entity_name": result.entity_name,
    }

    if result.limit_set_at:
        payload["limit_set_at"] = result.limit_set_at

    if result.organization_id:
        payload["organization_id"] = result.organization_id

    try:
        client = _get_spend_client()
        data = await client.notify_limit_reached(payload)
        if data.get("notified"):
            logger.info(
                f"Spending limit notification sent for {result.limit_type} "
                f"limit (entity_id={result.entity_id}, limit=${result.limit_value})",
            )
        else:
            logger.debug(
                f"Spending limit notification skipped: {data.get('reason', 'unknown')}",
            )
    except Exception as e:
        logger.warning(f"Failed to send spending limit notification: {e}")


def _provider_of(model: str) -> Optional[str]:
    """Extract the provider from a UniLLM ``model@provider`` endpoint.

    The model half may itself contain ``/`` (``openai/gpt-5.6-terra``) and
    the provider is always the trailing segment, so split on the last ``@``.
    Returns ``None`` for a bare model name, which routes by UniLLM's own
    default rather than naming a provider here.
    """
    _, sep, provider = model.rpartition("@")
    if not sep:
        return None
    return provider.strip().lower() or None


#: Display spellings for providers that appear in user-facing refusals. The
#: endpoint suffix is lowercased for matching, which is not how anyone writes
#: these names; falling back to the raw slug keeps a newly-gated provider
#: readable rather than blocking on an entry here.
_PROVIDER_DISPLAY_NAMES = {
    "anthropic": "Anthropic",
    "openai": "OpenAI",
    "openrouter": "OpenRouter",
    "vertex-ai": "Vertex AI",
    "deepseek": "DeepSeek",
}


def _provider_label(provider: str) -> str:
    """How a provider is spelled when the user reads it."""
    return _PROVIDER_DISPLAY_NAMES.get(provider, provider)


def _payment_gated(model: str, *, never_paid: bool) -> bool:
    """Whether this call is for a paid-only provider on an unpaid account."""
    if not never_paid:
        return False
    provider = _provider_of(model)
    return provider is not None and provider in PAYMENT_GATED_PROVIDERS


async def check_spending_limits_callback(
    request: "LimitCheckRequest",
) -> "LimitCheckResponse":
    """Limit check callback for UniLLM.

    This is the callback registered with UniLLM via set_limit_check_hook().
    It uses SESSION_DETAILS to determine the current context and checks all
    applicable limits in parallel.

    Args:
        request: Information about the pending LLM call (from UniLLM).

    Returns:
        LimitCheckResponse indicating whether to proceed.
    """
    from unillm.limit_hooks import LimitCheckResponse, LimitType

    from .session_details import SESSION_DETAILS

    api_key = _get_api_key()
    if not api_key:
        logger.debug("Spending limit check skipped: no API key")
        return LimitCheckResponse(allowed=True)

    agent_id = SESSION_DETAILS.assistant.agent_id

    user_id = SESSION_DETAILS.user_id
    org_id = SESSION_DETAILS.org_id  # None for personal context

    # A multi-tenant host (the gateway proxy) has no session; its caller
    # identity arrives per request instead. SESSION_DETAILS is a process
    # singleton there and would either be empty or — worse, if some other
    # code path ever populated it — describe the wrong tenant.
    caller = _CALLER.get()
    if caller is not None:
        user_id = caller.user_id or ""
        org_id = caller.org_id
        agent_id = None

    timezone = "UTC"
    if SESSION_DETAILS.assistant:
        timezone = SESSION_DETAILS.assistant.timezone or "UTC"

    month = _get_current_month(timezone)

    checks: List[asyncio.Task] = []

    # The raw-API path (gateway/CLI usage with a bare UNIFY_KEY) carries no
    # assistant session, but the ``/user/spend`` endpoint resolves the key
    # owner's wallet server-side, so the balance/cap gates always apply.
    # Skipping the check when session context is missing would fail open —
    # exactly the channel free-credit farmers extract through.
    if agent_id:
        checks.append(
            asyncio.create_task(
                _check_assistant_limit(agent_id, month),
            ),
        )

    is_org_context = org_id is not None
    if is_org_context and user_id:
        checks.append(
            asyncio.create_task(
                _check_member_limit(user_id, org_id, month),
            ),
        )
        checks.append(
            asyncio.create_task(
                _check_org_limit(org_id, month),
            ),
        )
    else:
        checks.append(
            asyncio.create_task(
                _check_user_limit(user_id or "api-key-owner", month),
            ),
        )

    results = await asyncio.gather(*checks, return_exceptions=True)

    def _to_limit_type(type_str: Optional[str]) -> Optional[LimitType]:
        if type_str is None:
            return None
        try:
            return LimitType(type_str)
        except ValueError:
            return None

    credit_balance: Optional[float] = None
    billing_mode: Optional[str] = None
    account_suspended = False
    never_paid = False
    check_failed = False
    api_access_allowed = True

    for result in results:
        if isinstance(result, Exception):
            logger.warning(f"Limit check failed with exception: {result}")
            check_failed = True
            continue

        check_failed = check_failed or result.check_failed
        api_access_allowed = api_access_allowed and result.api_access_allowed

        if credit_balance is None and result.credit_balance is not None:
            credit_balance = result.credit_balance
        if billing_mode is None and result.billing_mode is not None:
            billing_mode = result.billing_mode
        account_suspended = account_suspended or result.account_suspended
        never_paid = never_paid or result.never_paid

        if result.exceeded:
            current = (
                f"${result.current_spend:.2f}" if result.current_spend else "unknown"
            )
            limit = f"${result.limit_value:.2f}" if result.limit_value else "unknown"
            reason = f"Monthly spending limit exceeded: {result.limit_type} limit of {limit} reached (current: {current})"

            asyncio.create_task(
                _notify_limit_reached(result, month),
            )

            return LimitCheckResponse(
                allowed=False,
                reason=reason,
                limit_type=_to_limit_type(result.limit_type),
                limit_value=result.limit_value,
                current_spend=result.current_spend,
                entity_id=result.entity_id,
                entity_name=result.entity_name,
            )

    # Fail closed for a multi-tenant caller whose limits could not be
    # verified. The runtime deliberately fails open here — an Orchestra
    # blip should not stall a paying customer's assistant mid-task — but
    # on a public endpoint spending money against a wallet we just failed
    # to read, "allow" is an abuse channel: induce the error, spend freely.
    # Availability of someone else's trial credits is not worth protecting.
    if check_failed and caller is not None:
        return LimitCheckResponse(
            allowed=False,
            reason=(
                "Unable to verify account spending limits. " "Please retry shortly."
            ),
        )

    # Console-only free credits. Applies solely to the multi-tenant path:
    # a caller context means someone reached the platform through the
    # public API, and an account still on free money is not entitled to
    # that route. The runtime has no caller context, so Console-driven
    # work is untouched — spending free credits there is the point.
    if caller is not None and not api_access_allowed:
        return LimitCheckResponse(
            allowed=False,
            reason=(
                "This account's free credits can only be spent through the "
                "Unify Console. Add a payment method to enable API access."
            ),
        )

    # Hard deny for server-side frozen accounts (admin freeze, card-gate
    # sweep, abuse-fingerprint sweep).
    if account_suspended:
        return LimitCheckResponse(
            allowed=False,
            reason=(
                "This account is suspended. Add a payment method or "
                "contact support to restore access."
            ),
        )

    # Paid-only providers. Orchestra sets ``never_paid`` only for accounts
    # with no real payment history, and already clears it for internal
    # accounts and for orgs holding an admin-granted free trial, so those
    # keep full model access without a second exemption list here.
    #
    # Unlike the Console-only gate above this applies on every surface,
    # including the runtime. An account that has never paid cannot reach
    # these providers from the Console either — that is the point, since
    # the Console is where the free grant is meant to be spent and these
    # models are what make spending it worthwhile.
    if _payment_gated(request.model, never_paid=never_paid):
        provider = _provider_of(request.model)
        return LimitCheckResponse(
            allowed=False,
            reason=(
                f"{_provider_label(str(provider))} models unlock once this "
                "account has made its first payment — adding a card alone "
                "does not enable them. Subscribe in billing to use them, or "
                "switch this assistant to one of the included models to "
                "carry on now."
            ),
        )

    # Credit-balance gate. METERED accounts pay by monthly invoice via
    # ``monthly_metered_invoicer`` and intentionally have a zero wallet
    # balance (``deduct_credits`` doesn't mutate it on METERED), so the
    # legacy gate would block every call. Skip it for METERED, keep it
    # for CREDITS (and for the no-billing-mode-yet legacy case so we
    # don't loosen the gate during a partial Orchestra rollout).
    if billing_mode != "METERED" and credit_balance is not None and credit_balance <= 0:
        return LimitCheckResponse(
            allowed=False,
            reason=(
                f"Insufficient credits: balance is ${credit_balance:.2f}. "
                "Please add credits to continue."
            ),
        )

    return LimitCheckResponse(allowed=True)


def install_limit_check_hook() -> None:
    """Install the spending limit check hook with UniLLM.

    This function is idempotent - calling it multiple times has no effect
    after the first successful installation.

    Should be called during unify.init() after SESSION_DETAILS is populated.
    """
    if not _charges_billing():
        logger.debug("Limit check hook not installed: platform billing disabled")
        return

    api_key = _get_api_key()
    if not api_key:
        logger.debug("Limit check hook not installed: no API key")
        return

    try:
        import unillm

        unillm.set_limit_check_hook(check_spending_limits_callback)
        logger.debug("Limit check hook installed")
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Failed to install limit check hook: {e}")


def install_multi_tenant_limit_check_hook() -> None:
    """Install the limit-check hook in a host that serves many callers.

    Identical to :func:`install_limit_check_hook` except that it does not
    require a process-wide ``UNIFY_KEY``. A multi-tenant host (the
    gateway's ``/unillm/chat/completions`` proxy) authenticates a
    different user on every request and supplies that user's key through
    :func:`caller_context`; there may be no process key at all.

    Requiring one at install time is not a harmless extra check — it is
    why the proxy shipped enforcing no spending limits whatsoever: the
    hook was never installed, and UniLLM treats a missing hook as
    "allowed".
    """
    if not _charges_billing():
        logger.debug("Limit check hook not installed: platform billing disabled")
        return

    try:
        import unillm

        unillm.set_limit_check_hook(check_spending_limits_callback)
        logger.info("Multi-tenant limit check hook installed")
    except ImportError:
        pass
    except Exception as e:
        logger.warning(f"Failed to install multi-tenant limit check hook: {e}")


def uninstall_limit_check_hook() -> None:
    """Uninstall the spending limit check hook from UniLLM."""
    try:
        import unillm

        unillm.clear_limit_check_hook()
        logger.debug("Limit check hook uninstalled")
    except ImportError:
        pass
    except Exception:
        pass
